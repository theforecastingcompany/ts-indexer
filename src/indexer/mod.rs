use anyhow::Result;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::{info, warn, debug, error};

use crate::db::{Database, Dataset};
use crate::s3::S3Client;

pub struct Indexer {
    db: Database,
    s3_client: S3Client,
}

pub struct IndexingStats {
    pub files_processed: usize,
    pub series_indexed: usize,
    pub records_indexed: usize,
    pub errors: usize,
    pub processing_time_ms: u128,
}

#[derive(Debug)]
pub struct DatasetTask {
    pub metadata_key: String,
    pub data_prefix: String,
    pub index: usize,
    pub total: usize,
}

#[derive(Debug)]
pub struct DatasetResult {
    pub dataset: Option<Dataset>,
    pub index: usize,
    pub error: Option<anyhow::Error>,
}

impl Indexer {
    pub fn new(db: Database, s3_client: S3Client) -> Self {
        Self { db, s3_client }
    }
    
    /// Create a shareable indexer instance
    pub fn shared(db: Database, s3_client: S3Client) -> Arc<Self> {
        Arc::new(Self::new(db, s3_client))
    }

    pub async fn index_data(
        &self,
        data_prefix: &str,
        metadata_prefix: &str,
        max_files: Option<usize>,
        _force_rebuild: bool,
    ) -> Result<IndexingStats> {
        // Use parallel indexing for better performance
        self.index_data_parallel(data_prefix, metadata_prefix, max_files, 8).await
    }
    
    /// Parallel indexing with progress tracking
    pub async fn index_data_parallel(
        &self,
        data_prefix: &str,
        metadata_prefix: &str,
        max_files: Option<usize>,
        max_concurrent: usize,
    ) -> Result<IndexingStats> {
        let start_time = Instant::now();
        info!("Starting parallel indexing process with {} workers...", max_concurrent);
        
        // List metadata files first to get dataset information
        let metadata_objects = self.s3_client.list_objects(metadata_prefix, max_files).await?;
        let metadata_files = self.s3_client.filter_metadata_files(&metadata_objects);
        
        let total_datasets = metadata_files.len();
        info!("Found {} metadata files to process", total_datasets);
        
        if total_datasets == 0 {
            return Ok(IndexingStats {
                files_processed: 0,
                series_indexed: 0,
                records_indexed: 0,
                errors: 0,
                processing_time_ms: start_time.elapsed().as_millis(),
            });
        }
        
        // Create progress tracking
        let multi_progress = MultiProgress::new();
        let main_progress = multi_progress.add(ProgressBar::new(total_datasets as u64));
        main_progress.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} datasets {msg}")
                .unwrap()
                .progress_chars("##-")
        );
        main_progress.set_message("Starting...");
        
        // Create tasks for parallel processing
        let tasks: Vec<DatasetTask> = metadata_files
            .into_iter()
            .enumerate()
            .map(|(index, metadata_file)| DatasetTask {
                metadata_key: metadata_file.key,
                data_prefix: data_prefix.to_string(),
                index,
                total: total_datasets,
            })
            .collect();
        
        // Create shared indexer instance
        let indexer = Arc::new(Indexer::new(self.db.clone(), self.s3_client.clone()));
        
        // Use semaphore to limit concurrent processing
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        
        // Process datasets in parallel with progress tracking
        let mut results = stream::iter(tasks)
            .map(|task| {
                let indexer = indexer.clone();
                let semaphore = semaphore.clone();
                let progress = main_progress.clone();
                
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    let result = indexer
                        .process_metadata_file(&task.metadata_key, &task.data_prefix)
                        .await;
                    
                    let dataset_result = match result {
                        Ok(dataset) => {
                            progress.set_message(format!("Completed: {}", dataset.name));
                            DatasetResult {
                                dataset: Some(dataset),
                                index: task.index,
                                error: None,
                            }
                        }
                        Err(e) => {
                            progress.set_message(format!("Failed: {}", task.metadata_key));
                            DatasetResult {
                                dataset: None,
                                index: task.index,
                                error: Some(e),
                            }
                        }
                    };
                    
                    progress.inc(1);
                    dataset_result
                }
            })
            .buffer_unordered(max_concurrent)
            .collect::<Vec<_>>()
            .await;
        
        // Sort results by index to maintain order
        results.sort_by_key(|r| r.index);
        
        // Aggregate statistics
        let mut datasets_processed = 0;
        let mut series_count = 0i64;
        let mut errors = 0;
        
        for result in results {
            if let Some(dataset) = result.dataset {
                datasets_processed += 1;
                series_count += dataset.total_series;
                info!("Successfully processed dataset: {}", dataset.name);
            } else if let Some(error) = result.error {
                errors += 1;
                error!("Failed to process dataset: {}", error);
            }
        }
        
        main_progress.finish_with_message(format!(
            "Completed! {} datasets, {} errors", 
            datasets_processed, errors
        ));
        
        let stats = IndexingStats {
            files_processed: datasets_processed,
            series_indexed: series_count as usize,
            records_indexed: series_count as usize,
            errors,
            processing_time_ms: start_time.elapsed().as_millis(),
        };
        
        info!("Parallel indexing completed successfully!");
        Ok(stats)
    }
    
    async fn process_metadata_file(&self, metadata_key: &str, data_prefix: &str) -> Result<Dataset> {
        debug!("Processing metadata file: {}", metadata_key);
        
        // Download and parse metadata
        let metadata = self.s3_client.download_and_parse_metadata(metadata_key).await?;
        
        // List data files for this dataset
        let dataset_name = &metadata.name;
        let dataset_data_prefix = format!("{}{}/", data_prefix, dataset_name);
        let data_objects = self.s3_client.list_objects(&dataset_data_prefix, None).await.unwrap_or_default();
        let data_files = self.s3_client.filter_data_files(&data_objects);
        
        info!("Found {} data files for dataset: {}", data_files.len(), dataset_name);
        
        // Compute precise statistics by analyzing parquet files
        let stats = self.s3_client.analyze_dataset_statistics(&data_files, &metadata).await
            .unwrap_or_else(|e| {
                warn!("Failed to compute precise statistics for {}: {}. Using estimates.", dataset_name, e);
                crate::s3::SeriesStatistics {
                    unique_series_count: data_files.len() as i64,
                    total_records: data_files.len() as i64 * 1000,
                    avg_series_length: 1000.0,
                    min_series_length: 1000,
                    max_series_length: 1000,
                    series_id_columns: vec!["item_id".to_string()],
                }
            });
        
        // Create dataset record from metadata and computed statistics
        let dataset = Dataset {
            dataset_id: metadata.name.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            source_bucket: self.s3_client.bucket().to_string(),
            source_prefix: data_prefix.to_string(),
            schema_version: "1.0".to_string(),
            dataset_type: metadata.dataset_type.clone(),
            source: metadata.source.clone(),
            storage_location: metadata.storage_location.clone(),
            item_id_type: metadata.item_id.as_ref().map(|f| f.field_type.clone()),
            target_type: metadata.target.as_ref().map(|f| f.field_type.clone()),
            tfc_features: metadata.tfc_data_store_features.as_ref()
                .and_then(|features| serde_json::to_string(features).ok()),
            total_series: stats.unique_series_count,
            total_records: stats.total_records,
            series_id_columns: serde_json::to_string(&stats.series_id_columns).ok(),
            avg_series_length: Some(stats.avg_series_length),
            min_series_length: Some(stats.min_series_length),
            max_series_length: Some(stats.max_series_length),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        
        // Insert dataset into database
        self.db.insert_dataset(&dataset)?;
        
        Ok(dataset)
    }
}