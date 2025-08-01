use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

use crate::monitoring::get_global_monitor;

#[derive(Clone)]
pub struct S3Client {
    client: Client,
    bucket: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Object {
    pub key: String,
    pub size: i64,
    pub last_modified: Option<chrono::DateTime<chrono::Utc>>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub key: String,
    pub dataset_name: String,
    pub series_count: usize,
    pub file_format: FileFormat,
    pub schema_info: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
    Csv,
    Yaml,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    pub name: String,
    pub description: Option<String>,
    pub dataset_type: Option<String>,
    pub source: Option<String>,
    pub storage_location: Option<String>,
    pub item_id: Option<FieldInfo>,
    pub target: Option<FieldInfo>, // Keep for backward compatibility
    pub targets: Option<Vec<NamedFieldInfo>>, // NEW: Support multiple targets
    pub covariates: Option<Covariates>,
    pub tfc_data_store_features: Option<HashMap<String, FieldInfo>>,
    pub ts_id: Option<Vec<TsIdField>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsIdField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub subtype: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    pub field_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedFieldInfo {
    pub name: String,
    pub field_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Covariates {
    pub hist: Vec<NamedFieldInfo>, // Changed to store detailed field info with names
    pub future: Vec<NamedFieldInfo>, // Changed to store detailed field info with names
    #[serde(rename = "static")]
    pub static_vars: Vec<NamedFieldInfo>, // Changed to store detailed field info with names
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeriesStatistics {
    pub unique_series_count: i64,
    pub total_records: i64,
    pub avg_series_length: f64,
    pub min_series_length: i64,
    pub max_series_length: i64,
    pub series_id_columns: Vec<String>,
    // New: Individual series information for indexing
    pub individual_series: Vec<IndividualSeriesInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndividualSeriesInfo {
    pub series_id: String,
    pub record_count: i64,
    pub source_files: Vec<String>, // Files where this series appears
}

impl S3Client {
    /// Get the bucket name
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    
    /// Create a new S3 client
    pub async fn new(bucket: String, region: String) -> Result<Self> {
        info!("Initializing S3 client for bucket: {} in region: {}", bucket, region);
        
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.clone()))
            .load()
            .await;
            
        let client = Client::new(&config);
        
        Ok(S3Client { client, bucket })
    }
    
    /// List all objects in a given prefix
    pub async fn list_objects(&self, prefix: &str, max_keys: Option<usize>) -> Result<Vec<S3Object>> {
        info!("Listing objects with prefix: {}", prefix);
        
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        let mut total_processed = 0;
        
        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .max_keys(1000); // AWS maximum
                
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }
            
            let response = request
                .send()
                .await
                .context("Failed to list S3 objects")?;
            
            if let Some(contents) = response.contents {
                for object in contents {
                    if let (Some(key), Some(size)) = (object.key, object.size) {
                        objects.push(S3Object {
                            key: key.clone(),
                            size,
                            last_modified: object.last_modified
                                .map(|ts| chrono::DateTime::from_timestamp(ts.secs(), ts.subsec_nanos()).unwrap()),
                            etag: object.e_tag,
                        });
                        
                        total_processed += 1;
                        
                        if let Some(max) = max_keys {
                            if total_processed >= max {
                                info!("Reached maximum key limit: {}", max);
                                return Ok(objects);
                            }
                        }
                    }
                }
            }
            
            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }
        
        info!("Found {} objects", objects.len());
        Ok(objects)
    }
    
    /// Download and return the contents of an S3 object
    pub async fn download_object(&self, key: &str) -> Result<Vec<u8>> {
        debug!("Downloading object: {}", key);
        
        let response = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to download object: {}", key))?;
        
        let data = response.body.collect().await
            .context("Failed to read object body")?
            .into_bytes()
            .to_vec();
        
        debug!("Downloaded {} bytes from {}", data.len(), key);
        Ok(data)
    }
    
    /// Stream download of large objects with progress tracking
    pub async fn stream_download_object(&self, key: &str, progress_bar: Option<&ProgressBar>) -> Result<Vec<u8>> {
        debug!("Streaming download of object: {}", key);
        
        let response = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to start streaming download: {}", key))?;
        
        let content_length = response.content_length.unwrap_or(0) as usize;
        let mut buffer = Vec::with_capacity(content_length);
        let mut stream = response.body.into_async_read();
        
        if let Some(pb) = progress_bar {
            pb.set_length(content_length as u64);
        }
        
        let mut temp_buffer = [0u8; 8192]; // 8KB chunks
        loop {
            match stream.read(&mut temp_buffer).await {
                Ok(0) => break, // EOF
                Ok(bytes_read) => {
                    buffer.extend_from_slice(&temp_buffer[..bytes_read]);
                    if let Some(pb) = progress_bar {
                        pb.inc(bytes_read as u64);
                    }
                }
                Err(e) => return Err(anyhow::anyhow!("Stream read error: {}", e)),
            }
        }
        
        debug!("Streamed {} bytes from {}", buffer.len(), key);
        Ok(buffer)
    }
    
    /// Parallel download of multiple objects
    pub async fn parallel_download(&self, keys: &[String], max_concurrent: usize) -> Result<Vec<(String, Vec<u8>)>> {
        info!("Starting parallel download of {} objects", keys.len());
        
        let progress_bar = ProgressBar::new(keys.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-")
        );
        
        let results: Vec<(String, Vec<u8>)> = stream::iter(keys.iter().map(|k| k.clone()))
            .map(|key| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let pb = progress_bar.clone();
                
                async move {
                    let result = Self::download_single_object(&client, &bucket, &key).await;
                    pb.inc(1);
                    result.map(|data| (key, data))
                }
            })
            .buffer_unordered(max_concurrent)
            .try_collect()
            .await?;
        
        progress_bar.finish_with_message("Downloads completed");
        info!("Completed parallel download of {} objects", results.len());
        
        Ok(results)
    }
    
    /// Helper method for single object download in parallel context
    async fn download_single_object(client: &Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
        let response = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to download: {}", key))?;
        
        let data = response.body.collect().await
            .context("Failed to read object body")?
            .into_bytes()
            .to_vec();
            
        Ok(data)
    }
    
    /// Check if object exists
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.to_string().contains("NoSuchKey") {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!("Error checking object existence: {}", e))
                }
            }
        }
    }
    
    /// Filter objects by file extension and size
    pub fn filter_data_files(&self, objects: &[S3Object]) -> Vec<S3Object> {
        objects.iter()
            .filter(|obj| {
                let key_lower = obj.key.to_lowercase();
                let is_data_file = key_lower.ends_with(".parquet") 
                    || key_lower.ends_with(".csv")
                    || key_lower.ends_with(".csv.gz");
                
                let is_reasonable_size = obj.size > 1024 && obj.size < 10_000_000_000; // 1KB to 10GB
                
                if !is_data_file {
                    debug!("Skipping non-data file: {}", obj.key);
                }
                if !is_reasonable_size {
                    debug!("Skipping file with unreasonable size ({}): {}", obj.size, obj.key);
                }
                
                is_data_file && is_reasonable_size
            })
            .cloned()
            .collect()
    }
    
    /// Filter metadata files (YAML files)
    pub fn filter_metadata_files(&self, objects: &[S3Object]) -> Vec<S3Object> {
        objects.iter()
            .filter(|obj| {
                let key_lower = obj.key.to_lowercase();
                let is_metadata_file = key_lower.ends_with(".yaml") || key_lower.ends_with(".yml");
                
                if is_metadata_file {
                    debug!("Found metadata file: {}", obj.key);
                }
                
                is_metadata_file
            })
            .cloned()
            .collect()
    }
    
    /// Download and parse a YAML metadata file
    pub async fn download_and_parse_metadata(&self, key: &str) -> Result<DatasetMetadata> {
        debug!("Downloading and parsing metadata: {}", key);
        
        let data = self.download_object(key).await?;
        let yaml_str = String::from_utf8(data)
            .context("Failed to convert metadata to UTF-8")?;
        
        // Extract dataset name from key path
        let dataset_name = self.extract_dataset_name_from_metadata_key(key);
        
        // Parse YAML with fallback for missing fields
        let metadata: serde_yaml::Value = serde_yaml::from_str(&yaml_str)
            .context("Failed to parse YAML metadata")?;
        
        // Extract description from nested structure if available
        let description = metadata.get("metadata")
            .and_then(|m| m.get("description"))
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());
        
        let dataset_type = metadata.get("metadata")
            .and_then(|m| m.get("type"))
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());
        
        let source = metadata.get("metadata")
            .and_then(|m| m.get("source"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        
        let storage_location = metadata.get("metadata")
            .and_then(|m| m.get("storage_location"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        
        // Extract field info
        let item_id = metadata.get("item_id")
            .and_then(|f| f.get("type"))
            .and_then(|t| t.as_str())
            .map(|s| FieldInfo { field_type: s.to_string() });
        
        // Extract single target (backward compatibility)
        let target = metadata.get("target")
            .and_then(|f| f.get("type"))
            .and_then(|t| t.as_str())
            .map(|s| FieldInfo { field_type: s.to_string() });

        // Extract multiple targets array (NEW FORMAT)
        let targets = metadata.get("targets")
            .and_then(|t| t.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|item| {
                        let name = item.get("name")?.as_str()?.to_string();
                        let field_type = item.get("type")?.as_str()?.to_string();
                        Some(NamedFieldInfo { name, field_type })
                    })
                    .collect::<Vec<NamedFieldInfo>>()
            });

        // Extract covariates with detailed field information  
        let covariates = metadata.get("covariates")
            .and_then(|c| c.as_mapping())
            .map(|cov_map| {
                let hist = cov_map.get("hist")
                    .and_then(|h| h.as_sequence())
                    .map(|seq| {
                        seq.iter()
                            .filter_map(|item| {
                                let name = item.get("name")?.as_str()?.to_string();
                                let field_type = item.get("type")?.as_str()?.to_string();
                                Some(NamedFieldInfo { name, field_type })
                            })
                            .collect::<Vec<NamedFieldInfo>>()
                    })
                    .unwrap_or(Vec::new());

                let future = cov_map.get("future")
                    .and_then(|f| f.as_sequence())
                    .map(|seq| {
                        seq.iter()
                            .filter_map(|item| {
                                let name = item.get("name")?.as_str()?.to_string();
                                let field_type = item.get("type")?.as_str()?.to_string();
                                Some(NamedFieldInfo { name, field_type })
                            })
                            .collect::<Vec<NamedFieldInfo>>()
                    })
                    .unwrap_or(Vec::new());

                let static_vars = cov_map.get("static")
                    .and_then(|s| s.as_sequence())
                    .map(|seq| {
                        seq.iter()
                            .filter_map(|item| {
                                let name = item.get("name")?.as_str()?.to_string();
                                let field_type = item.get("type")?.as_str()?.to_string();
                                Some(NamedFieldInfo { name, field_type })
                            })
                            .collect::<Vec<NamedFieldInfo>>()
                    })
                    .unwrap_or(Vec::new());

                Covariates { hist, future, static_vars }
            });
        
        // Extract TFC features from both old and new schema formats
        let tfc_features = metadata.get("tfc_data_store_features")
            .or_else(|| metadata.get("tfc_data_store"))
            .and_then(|f| f.as_mapping())
            .map(|features| {
                let mut result = HashMap::new();
                
                // Handle both formats: direct mapping and nested future/hist structure
                if let Some(future) = features.get("future").and_then(|f| f.as_sequence()) {
                    for item in future {
                        if let (Some(name), Some(field_type)) = (
                            item.get("name").and_then(|n| n.as_str()),
                            item.get("type").and_then(|t| t.as_str())
                        ) {
                            result.insert(name.to_string(), FieldInfo { field_type: field_type.to_string() });
                        }
                    }
                }
                
                // Also handle direct field mappings
                for (k, v) in features.iter() {
                    if k.as_str() != Some("future") && k.as_str() != Some("hist") {
                        if let (Some(key), Some(field_type)) = (
                            k.as_str(),
                            v.get("type").and_then(|t| t.as_str())
                        ) {
                            result.insert(key.to_string(), FieldInfo { field_type: field_type.to_string() });
                        }
                    }
                }
                
                result
            });
        
        // Extract ts_id information
        let ts_id = metadata.get("ts_id")
            .and_then(|t| t.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|item| {
                        let name = item.get("name")?.as_str()?.to_string();
                        let field_type = item.get("type")?.as_str()?.to_string();
                        let subtype = item.get("subtype").and_then(|s| s.as_str()).map(|s| s.to_string());
                        Some(TsIdField { name, field_type, subtype })
                    })
                    .collect::<Vec<TsIdField>>()
            });
        
        Ok(DatasetMetadata {
            name: dataset_name,
            description,
            dataset_type,
            source,
            storage_location,
            item_id,
            target,
            targets,
            covariates,
            tfc_data_store_features: tfc_features,
            ts_id,
        })
    }
    
    /// Extract dataset name from S3 key
    pub fn extract_dataset_name(&self, key: &str) -> String {
        // Extract dataset name from path structure
        // e.g., "lotsa_long_format/dataset_name/file.parquet" -> "dataset_name"
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() >= 2 {
            parts[parts.len() - 2].to_string()
        } else {
            "unknown".to_string()
        }
    }
    
    /// Extract dataset name from metadata key
    pub fn extract_dataset_name_from_metadata_key(&self, key: &str) -> String {
        // Extract dataset name from metadata path structure
        // e.g., "metadata/lotsa_long_format/dataset_name_metadata.yaml" -> "dataset_name"
        let file_name = key.split('/').last().unwrap_or("unknown");
        file_name.replace("_metadata.yaml", "").replace("_metadata.yml", "")
    }
    
    /// Find the correct case-sensitive directory name for a dataset
    pub async fn find_case_sensitive_dataset_dir(&self, data_prefix: &str, dataset_name: &str) -> Result<Option<String>> {
        // List all directories in the data prefix
        let objects = self.list_objects(data_prefix, None).await?;
        
        // Convert dataset name to lowercase for comparison
        let dataset_name_lower = dataset_name.to_lowercase();
        debug!("Looking for dataset '{}' (lowercase: '{}') in {} objects", dataset_name, dataset_name_lower, objects.len());
        
        // Debug: Show first few object keys to understand the structure
        for (i, obj) in objects.iter().take(5).enumerate() {
            debug!("Object {}: '{}'", i, obj.key);
        }
        
        // Extract unique directory names from file paths
        let mut directories = std::collections::HashSet::new();
        for obj in objects {
            if let Some(remaining_path) = obj.key.strip_prefix(data_prefix) {
                // Extract the first directory component (dataset directory)
                if let Some(dir_name) = remaining_path.split('/').next() {
                    if !dir_name.is_empty() {
                        directories.insert(dir_name.to_string());
                    }
                }
            }
        }
        
        debug!("Found {} unique directories", directories.len());
        
        // Find directories that match case-insensitively
        for dir_name in directories {
            debug!("Checking directory: '{}' (lowercase: '{}')", dir_name, dir_name.to_lowercase());
            if dir_name.to_lowercase() == dataset_name_lower {
                debug!("✅ Found case-sensitive match: {} -> {}", dataset_name, dir_name);
                return Ok(Some(dir_name));
            }
        }
        
        debug!("❌ No directory found for dataset: {}", dataset_name);
        Ok(None)
    }

    /// Pre-compute directory mapping for all datasets to avoid race conditions in parallel processing
    pub async fn compute_dataset_directory_mapping(&self, data_prefix: &str) -> Result<HashMap<String, String>> {
        info!("Computing dataset directory mapping for prefix: {}", data_prefix);
        
        // List all directories in the data prefix once
        let objects = self.list_objects(data_prefix, None).await?;
        
        // Extract unique directory names from file paths
        let mut directories = std::collections::HashSet::new();
        for obj in objects {
            if let Some(remaining_path) = obj.key.strip_prefix(data_prefix) {
                // Extract the first directory component (dataset directory)
                if let Some(dir_name) = remaining_path.split('/').next() {
                    if !dir_name.is_empty() {
                        directories.insert(dir_name.to_string());
                    }
                }
            }
        }
        
        // Create mapping from lowercase dataset names to actual directory names
        let mut mapping = HashMap::new();
        for dir_name in directories {
            let lowercase_name = dir_name.to_lowercase();
            mapping.insert(lowercase_name, dir_name);
        }
        
        info!("Computed directory mapping for {} datasets", mapping.len());
        debug!("Directory mapping: {:?}", mapping);
        
        Ok(mapping)
    }

    /// Find dataset directory using pre-computed mapping (thread-safe)
    pub fn find_dataset_dir_from_mapping(
        &self, 
        dataset_name: &str, 
        directory_mapping: &HashMap<String, String>
    ) -> Option<String> {
        let dataset_name_lower = dataset_name.to_lowercase();
        directory_mapping.get(&dataset_name_lower).cloned()
    }

    /// Detect file format from extension
    pub fn detect_file_format(&self, key: &str) -> FileFormat {
        let key_lower = key.to_lowercase();
        if key_lower.ends_with(".parquet") {
            FileFormat::Parquet
        } else if key_lower.ends_with(".csv") || key_lower.ends_with(".csv.gz") {
            FileFormat::Csv
        } else if key_lower.ends_with(".yaml") || key_lower.ends_with(".yml") {
            FileFormat::Yaml
        } else {
            FileFormat::Unknown
        }
    }
    
    /// Analyze parquet files to compute precise series statistics
    pub async fn analyze_dataset_statistics(
        &self,
        data_files: &[S3Object],
        metadata: &DatasetMetadata,
    ) -> Result<SeriesStatistics> {
        info!("Computing precise statistics for {} data files", data_files.len());
        
        if data_files.is_empty() {
            return Ok(SeriesStatistics {
                unique_series_count: 0,
                total_records: 0,
                avg_series_length: 0.0,
                min_series_length: 0,
                max_series_length: 0,
                series_id_columns: vec![],
                individual_series: vec![],
            });
        }
        
        // Extract required columns for selective reading (ts-id + static string features)
        let required_columns = self.extract_required_columns_for_indexing(metadata);
        let series_id_columns = self.extract_series_id_columns(metadata);
        
        info!("Analyzing {} parquet files with selective column reading", data_files.len());
        info!("Will read {} columns instead of all columns for better performance", required_columns.len());
        
        use futures::stream::{self, StreamExt};
        use std::sync::Arc;
        
        let required_columns = Arc::new(required_columns);
        let series_id_columns = Arc::new(series_id_columns);
        
        // File size-aware concurrency - prevent memory overload from large files
        let total_size: i64 = data_files.iter().map(|f| f.size).sum();
        let avg_file_size = if data_files.len() > 0 { total_size / data_files.len() as i64 } else { 0 };
        
        let max_concurrent = if avg_file_size > 1_000_000_000 { // 1GB+ files (like CMIP6)
            info!("🐌 Large files detected (avg {}MB), using concurrency=1 to prevent memory overload", avg_file_size / 1024 / 1024);
            1 // Process one heavy file at a time
        } else if avg_file_size > 100_000_000 { // 100MB+ files
            info!("📊 Medium files detected (avg {}MB), using concurrency=2", avg_file_size / 1024 / 1024);
            2 // Light concurrency for medium files  
        } else {
            info!("🚀 Small files detected (avg {}MB), using concurrency=4", avg_file_size / 1024 / 1024);
            std::cmp::min(data_files.len(), 4) // Normal concurrency for small files
        };
        
        let results: Vec<_> = stream::iter(data_files.iter().enumerate())
            .map(|(i, file)| {
                let required_columns = required_columns.clone();
                let series_id_columns = series_id_columns.clone();
                let file_key = file.key.clone();
                async move {
                    debug!("Analyzing file {}/{}: {} (selective columns)", i + 1, data_files.len(), file_key);
                    
                    // Start file monitoring
                    let monitor = get_global_monitor();
                    let file_metrics = monitor.start_operation(format!("file_{}", 
                        file_key.split('/').last().unwrap_or(&file_key)));
                    
                    let result = self.analyze_parquet_file_selective(&file_key, &required_columns, &series_id_columns).await;
                    
                    // Finish file monitoring
                    monitor.finish_operation(file_metrics);
                    
                    (i, result)
                }
            })
            .buffer_unordered(max_concurrent)
            .collect()
            .await;
        
        let mut all_series_lengths = Vec::new();
        let mut unique_series = std::collections::HashSet::new();
        let mut total_records = 0i64;
        
        // Track individual series information across files
        let mut series_info_map: std::collections::HashMap<String, IndividualSeriesInfo> = std::collections::HashMap::new();
        
        for (i, result) in results {
            match result {
                Ok(file_stats) => {
                    let file_key = &data_files[i].key;
                    
                    // Process each series from this file
                    for (series_idx, series_id) in file_stats.unique_series.iter().enumerate() {
                        unique_series.insert(series_id.clone());
                        
                        // Get the record count for this specific series (if available)
                        let record_count = file_stats.series_lengths.get(series_idx).copied().unwrap_or(1);
                        
                        // Update or create series info
                        series_info_map.entry(series_id.clone())
                            .and_modify(|info| {
                                info.record_count += record_count;
                                info.source_files.push(file_key.clone());
                            })
                            .or_insert(IndividualSeriesInfo {
                                series_id: series_id.clone(),
                                record_count,
                                source_files: vec![file_key.clone()],
                            });
                    }
                    
                    // Add series lengths
                    all_series_lengths.extend(file_stats.series_lengths);
                    total_records += file_stats.total_records;
                }
                Err(e) => {
                    warn!("Failed to analyze file: {}", e);
                    continue;
                }
            }
        }
        
        // Use actual counts (no extrapolation)
        let actual_unique_series = unique_series.len() as i64;
        let actual_total_records = total_records;
        
        // Calculate series length statistics from all analyzed data
        let (avg_length, min_length, max_length) = if all_series_lengths.is_empty() {
            (0.0, 0, 0)
        } else {
            let sum: i64 = all_series_lengths.iter().sum();
            let avg = sum as f64 / all_series_lengths.len() as f64;
            let min = *all_series_lengths.iter().min().unwrap_or(&0);
            let max = *all_series_lengths.iter().max().unwrap_or(&0);
            (avg, min, max)
        };
        
        // Convert individual series map to vector
        let individual_series: Vec<IndividualSeriesInfo> = series_info_map.into_values().collect();
        
        info!("PRECISE Dataset statistics: {} unique series, {} total records, avg length: {:.1}", 
              actual_unique_series, actual_total_records, avg_length);
        debug!("Individual series count: {}", individual_series.len());
        
        Ok(SeriesStatistics {
            unique_series_count: actual_unique_series,
            total_records: actual_total_records,
            avg_series_length: avg_length,
            min_series_length: min_length,
            max_series_length: max_length,
            series_id_columns: (*series_id_columns).clone(),
            individual_series,
        })
    }
    
    /// Extract series ID column names from metadata  
    fn extract_series_id_columns(&self, metadata: &DatasetMetadata) -> Vec<String> {
        if let Some(ts_id_fields) = &metadata.ts_id {
            ts_id_fields.iter().map(|field| field.name.clone()).collect()
        } else {
            // Default to item_id if no ts_id information available
            vec!["item_id".to_string()]
        }
    }
    
    /// Extract all required columns for indexing from metadata
    /// Returns only ts-id columns for now (ignoring TFC features as requested)
    fn extract_required_columns_for_indexing(&self, metadata: &DatasetMetadata) -> Vec<String> {
        // For now, only include time-series ID columns (needed for groupby/counting)
        // This still provides significant performance benefits by avoiding loading
        // numerical time series data columns that aren't needed for indexing
        let required_columns = self.extract_series_id_columns(metadata);
        
        info!("Required columns for indexing (ts-id only): {:?}", required_columns);
        required_columns
    }
    
    /// Analyze a single parquet file for series statistics
    async fn analyze_parquet_file(
        &self,
        key: &str,
        series_id_columns: &[String],
    ) -> Result<FileAnalysisResult> {
        // Download the parquet file
        let data = self.download_object(key).await?;
        
        // Read with polars using cursor and ParquetReader
        let cursor = Cursor::new(data);
        let df = polars::io::parquet::ParquetReader::new(cursor)
            .finish()
            .context("Failed to read parquet file")?;
        
        // Check if required columns exist
        let available_columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        
        let valid_series_cols: Vec<String> = series_id_columns
            .iter()
            .filter(|col| available_columns.contains(col))
            .cloned()
            .collect();
        
        if valid_series_cols.is_empty() {
            return Err(anyhow::anyhow!(
                "No valid series ID columns found. Available: {:?}, Required: {:?}",
                available_columns, series_id_columns
            ));
        }
        
        // Get unique series identifiers
        let unique_series: Vec<String> = if valid_series_cols.len() == 1 {
            df.column(&valid_series_cols[0])?
                .unique()?
                .iter()
                .map(|v| v.to_string())
                .collect()
        } else {
            // For multiple columns, create composite keys
            let grouped = df.group_by(&valid_series_cols)?.count()?;
            let mut unique_series = Vec::new();
            
            for i in 0..grouped.height() {
                let mut key_parts = Vec::new();
                for col in &valid_series_cols {
                    let value = grouped.column(col)?.get(i)?;
                    key_parts.push(value.to_string());
                }
                unique_series.push(key_parts.join("|"));
            }
            unique_series
        };
        
        // Calculate series lengths (count per series)
        let series_counts_df = if valid_series_cols.len() == 1 {
            df.group_by([&valid_series_cols[0]])?.count()?
        } else {
            df.group_by(&valid_series_cols)?.count()?
        };
        
        // Get the count column (polars can name it "len", "count", or other variations)
        let available_count_cols = series_counts_df.get_column_names();
        debug!("Available columns in count DataFrame: {:?}", available_count_cols);
        
        let count_col_name = available_count_cols
            .iter()
            .find(|name| {
                let name_lower = name.to_lowercase();
                name_lower.contains("len") || name_lower.contains("count") || 
                name_lower == "n" || name_lower == "size" || **name == "len"
            })
            .copied()
            .unwrap_or_else(|| {
                // Fallback: use the last column (usually the count in groupby results)
                available_count_cols.last().copied().unwrap_or("len")
            });
            
        debug!("Using count column: '{}'", count_col_name);
            
        // Handle different data types for count column
        let count_column = series_counts_df.column(count_col_name)?;
        let series_lengths: Vec<i64> = match count_column.dtype() {
            DataType::UInt32 => count_column.u32()?.into_no_null_iter().map(|v| v as i64).collect(),
            DataType::UInt64 => count_column.u64()?.into_no_null_iter().map(|v| v as i64).collect(),
            DataType::Int32 => count_column.i32()?.into_no_null_iter().map(|v| v as i64).collect(),
            DataType::Int64 => count_column.i64()?.into_no_null_iter().collect(),
            _ => {
                // Try to cast to i64
                count_column.cast(&DataType::Int64)?
                    .i64()?
                    .into_no_null_iter()
                    .collect()
            }
        };
        
        let total_records = df.height() as i64;
        
        Ok(FileAnalysisResult {
            unique_series,
            series_lengths,
            total_records,
        })
    }
    
    /// Analyze a single parquet file with selective column reading for better performance
    async fn analyze_parquet_file_selective(
        &self,
        key: &str,
        required_columns: &[String],
        series_id_columns: &[String],
    ) -> Result<FileAnalysisResult> {
        let monitor = get_global_monitor();
        
        // Monitor S3 download operation
        let download_metrics = monitor.start_operation(format!("s3_download_{}", 
            key.split('/').last().unwrap_or("unknown")));
        let data = self.download_object(key).await?;
        monitor.finish_operation(download_metrics);
        
        // Monitor schema reading
        let schema_metrics = monitor.start_operation("parquet_schema_read".to_string());
        let cursor = Cursor::new(data.clone());
        let mut reader = polars::io::parquet::ParquetReader::new(cursor);
        let schema = reader.schema()?;
        let available_columns: Vec<String> = schema.get_names().into_iter().map(|s| s.to_string()).collect();
        monitor.finish_operation(schema_metrics);
        
        debug!("Available columns in {}: {:?}", key, available_columns);
        debug!("Requested columns: {:?}", required_columns);
        
        // Filter to only available columns from our required set
        let valid_required_cols: Vec<String> = required_columns
            .iter()
            .filter(|col| available_columns.contains(col))
            .cloned()
            .collect();
        
        if valid_required_cols.is_empty() {
            warn!("No required columns found in file {}, falling back to all columns", key);
            // Fall back to original method if no required columns are available
            return self.analyze_parquet_file(key, series_id_columns).await;
        }
        
        // Monitor selective parquet reading
        let read_metrics = monitor.start_operation(format!("parquet_selective_read_{}cols", 
            valid_required_cols.len()));
        let cursor = Cursor::new(data); // Use the already downloaded data
        let df = polars::io::parquet::ParquetReader::new(cursor)
            .with_columns(Some(valid_required_cols.clone()))
            .finish()
            .context("Failed to read parquet file with selective columns")?;
        monitor.finish_operation(read_metrics);
        
        let actual_columns: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        info!("Successfully read {} with {} columns (reduced from {} total)", 
              key, actual_columns.len(), available_columns.len());

        // Filter series ID columns to only those that are available (needed for debug output too)
        let valid_series_cols: Vec<String> = series_id_columns
            .iter()
            .filter(|col| actual_columns.contains(col))
            .cloned()
            .collect();
            
        // Debug: Show the loaded table structure and sample data
        debug!("DataFrame shape: {} rows × {} columns", df.height(), df.width());
        debug!("Column names: {:?}", df.get_column_names());
        
        // Show first 5 rows of the selected data
        if df.height() > 0 {
            let sample_rows = std::cmp::min(5, df.height());
            debug!("First {} rows of selected data:", sample_rows);
            for i in 0..sample_rows {
                let mut row_data = Vec::new();
                for col_name in df.get_column_names() {
                    let value = df.column(col_name).unwrap().get(i).unwrap();
                    row_data.push(format!("{}: {}", col_name, value));
                }
                debug!("  Row {}: [{}]", i + 1, row_data.join(", "));
            }
            
            // Show unique time-series count based on selected columns
            if !valid_series_cols.is_empty() {
                let unique_series_count = if valid_series_cols.len() == 1 {
                    df.column(&valid_series_cols[0]).unwrap().n_unique().unwrap()
                } else {
                    // For multiple columns, group by all series columns to get unique combinations
                    df.group_by(&valid_series_cols).unwrap().count().unwrap().height()
                };
                debug!("📊 Unique time-series in this file: {} (based on columns: {:?})", 
                      unique_series_count, valid_series_cols);
            }
        }
        
        if valid_series_cols.is_empty() {
            return Err(anyhow::anyhow!(
                "No valid series ID columns found after selective reading. Available: {:?}, Required: {:?}",
                actual_columns, series_id_columns
            ));
        }
        
        // Monitor combined series analysis (unique identification + length calculation together)
        let analysis_metrics = monitor.start_operation("series_analysis_combined".to_string());
        
        // Perform groupby once to get both unique series and their counts
        let series_counts_df = if valid_series_cols.len() == 1 {
            df.group_by([&valid_series_cols[0]])?.count()?
        } else {
            df.group_by(&valid_series_cols)?.count()?
        };
        
        // Extract unique series IDs and their corresponding counts in the same order
        let (unique_series, series_lengths): (Vec<String>, Vec<i64>) = if valid_series_cols.len() == 1 {
            let series_col = series_counts_df.column(&valid_series_cols[0])?;
            let count_col_name = series_counts_df.get_column_names()
                .iter()
                .find(|name| {
                    let name_lower = name.to_lowercase();
                    name_lower.contains("len") || name_lower.contains("count") || 
                    name_lower == "n" || name_lower == "size" || **name == "len"
                })
                .copied()
                .unwrap_or_else(|| {
                    series_counts_df.get_column_names().last().copied().unwrap_or("len")
                });
            
            let count_col = series_counts_df.column(count_col_name)?;
            
            let series_ids: Vec<String> = series_col.iter().map(|v| v.to_string()).collect();
            let counts: Vec<i64> = match count_col.dtype() {
                DataType::UInt32 => count_col.u32()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::UInt64 => count_col.u64()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::Int32 => count_col.i32()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::Int64 => count_col.i64()?.into_no_null_iter().collect(),
                _ => count_col.cast(&DataType::Int64)?.i64()?.into_no_null_iter().collect(),
            };
            
            (series_ids, counts)
        } else {
            // For multiple columns, create composite keys and extract counts
            let mut series_ids = Vec::new();
            let mut counts = Vec::new();
            
            let count_col_name = series_counts_df.get_column_names()
                .iter()
                .find(|name| {
                    let name_lower = name.to_lowercase();
                    name_lower.contains("len") || name_lower.contains("count") || 
                    name_lower == "n" || name_lower == "size" || **name == "len"
                })
                .copied()
                .unwrap_or_else(|| {
                    series_counts_df.get_column_names().last().copied().unwrap_or("len")
                });
            
            let count_col = series_counts_df.column(count_col_name)?;
            let count_values: Vec<i64> = match count_col.dtype() {
                DataType::UInt32 => count_col.u32()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::UInt64 => count_col.u64()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::Int32 => count_col.i32()?.into_no_null_iter().map(|v| v as i64).collect(),
                DataType::Int64 => count_col.i64()?.into_no_null_iter().collect(),
                _ => count_col.cast(&DataType::Int64)?.i64()?.into_no_null_iter().collect(),
            };
            
            for i in 0..series_counts_df.height() {
                let mut key_parts = Vec::new();
                for col in &valid_series_cols {
                    let value = series_counts_df.column(col)?.get(i)?;
                    key_parts.push(value.to_string());
                }
                series_ids.push(key_parts.join("|"));
                counts.push(count_values[i]);
            }
            
            (series_ids, counts)
        };
        
        monitor.finish_operation(analysis_metrics);
        
        // Debug: Verify alignment and show results
        debug!("Series analysis results:");
        debug!("  Unique series count: {}", unique_series.len());
        debug!("  Series lengths count: {}", series_lengths.len());
        
        // Ensure alignment 
        if unique_series.len() != series_lengths.len() {
            return Err(anyhow::anyhow!(
                "CRITICAL BUG: Series IDs and lengths are misaligned! {} series vs {} lengths",
                unique_series.len(), series_lengths.len()
            ));
        }
        
        // Show sample mapping for verification
        if !unique_series.is_empty() {
            let sample_count = std::cmp::min(5, unique_series.len());
            debug!("Sample series -> record count mappings:");
            for i in 0..sample_count {
                debug!("  '{}' -> {} records", unique_series[i], series_lengths[i]);
            }
        }
        
        let total_records = df.height() as i64;
        
        monitor.log_milestone("file_analysis", "complete", 
            &format!("series={}, records={}, cols_read={}", unique_series.len(), total_records, valid_required_cols.len()));
        
        Ok(FileAnalysisResult {
            unique_series,
            series_lengths,
            total_records,
        })
    }
}

#[derive(Debug)]
struct FileAnalysisResult {
    unique_series: Vec<String>,
    series_lengths: Vec<i64>,
    total_records: i64,
}

/// Create a progress bar for file operations
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!("[{{elapsed_precise}}] {{bar:40.cyan/blue}} {{pos}}/{{len}} {}", message))
            .unwrap()
            .progress_chars("##-")
    );
    pb
}