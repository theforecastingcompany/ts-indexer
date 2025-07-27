use anyhow::Result;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::select;
use tracing::{info, warn, debug, error};

use crate::db::{Database, Dataset, IndexingStatus};
use crate::progress::IndexerProgress;
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
    
    /// Parallel indexing with progress tracking and resume functionality
    pub async fn index_data_parallel(
        &self,
        data_prefix: &str,
        metadata_prefix: &str,
        max_files: Option<usize>,
        max_concurrent: usize,
    ) -> Result<IndexingStats> {
        self.index_data_parallel_with_resume(data_prefix, metadata_prefix, max_files, max_concurrent, false).await
    }
    
    /// Parallel indexing with graceful shutdown and explicit resume control
    pub async fn index_data_parallel_with_resume(
        &self,
        data_prefix: &str,
        metadata_prefix: &str,
        max_files: Option<usize>,
        max_concurrent: usize,
        force_reindex: bool,
    ) -> Result<IndexingStats> {
        let start_time = Instant::now();
        info!("Starting resumable parallel indexing process with {} workers...", max_concurrent);
        
        // Set up graceful shutdown handling
        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let shutdown_signal_clone = shutdown_signal.clone();
        
        // Spawn signal handler task
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create SIGINT handler");
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to create SIGTERM handler");
                
                select! {
                    _ = sigint.recv() => {
                        info!("🛑 Received SIGINT (Ctrl+C), initiating graceful shutdown...");
                    }
                    _ = sigterm.recv() => {
                        info!("🛑 Received SIGTERM, initiating graceful shutdown...");
                    }
                }
            }
            
            #[cfg(windows)]
            {
                use tokio::signal;
                signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
                info!("🛑 Received Ctrl+C, initiating graceful shutdown...");
            }
            
            shutdown_signal_clone.store(true, Ordering::SeqCst);
        });
        
        // List metadata files first to get dataset information (discover ALL datasets)
        let metadata_objects = self.s3_client.list_objects(metadata_prefix, None).await?;
        let mut metadata_files = self.s3_client.filter_metadata_files(&metadata_objects);
        
        let total_available_datasets = metadata_files.len();
        info!("Found {} metadata files available for indexing", total_available_datasets);
        
        // Apply max_files limit AFTER discovery but BEFORE processing
        if let Some(max) = max_files {
            if metadata_files.len() > max {
                info!("Limiting processing to first {} datasets (out of {} available)", max, metadata_files.len());
                metadata_files.truncate(max);
            }
        }
        
        if total_available_datasets == 0 {
            return Ok(IndexingStats {
                files_processed: 0,
                series_indexed: 0,
                records_indexed: 0,
                errors: 0,
                processing_time_ms: start_time.elapsed().as_millis(),
            });
        }
        
        // Filter out already completed datasets (unless force reindex)
        let datasets_to_process = if force_reindex {
            info!("Force reindex enabled - will process {} datasets", metadata_files.len());
            metadata_files
        } else {
            let datasets_to_filter: Vec<_> = metadata_files.iter()
                .map(|file| self.extract_dataset_name_from_metadata_key(&file.key))
                .collect();
                
            let mut filtered_files = Vec::new();
            for (file, dataset_name) in metadata_files.iter().zip(datasets_to_filter.iter()) {
                match self.db.get_dataset_status(dataset_name) {
                    Ok(Some(IndexingStatus::Completed)) => {
                        debug!("Skipping already completed dataset: {}", dataset_name);
                        continue;
                    }
                    Ok(Some(IndexingStatus::InProgress)) => {
                        warn!("Found dataset in 'in_progress' state, will retry: {}", dataset_name);
                        // Reset to pending for retry
                        let _ = self.db.update_dataset_status(dataset_name, IndexingStatus::Pending, None);
                        filtered_files.push(file.clone());
                    }
                    Ok(Some(IndexingStatus::Failed)) => {
                        info!("Retrying previously failed dataset: {}", dataset_name);
                        filtered_files.push(file.clone());
                    }
                    Ok(Some(IndexingStatus::Pending)) | Ok(None) => {
                        filtered_files.push(file.clone());
                    }
                    Err(e) => {
                        warn!("Error checking status for dataset {}: {}", dataset_name, e);
                        filtered_files.push(file.clone());
                    }
                }
            }
            
            let skipped_count = total_available_datasets - filtered_files.len();
            if skipped_count > 0 {
                info!("Resuming indexing: {} datasets already completed, {} remaining to process", 
                     skipped_count, filtered_files.len());
            }
            
            filtered_files
        };
        
        let total_datasets = datasets_to_process.len();
        
        // List all datasets that will be processed
        if total_datasets > 0 {
            info!("📋 Datasets to be indexed ({} total):", total_datasets);
            let dataset_names: Vec<String> = datasets_to_process.iter()
                .map(|file| self.extract_dataset_name_from_metadata_key(&file.key))
                .collect();
            
            // Sort for consistent output
            let mut sorted_names = dataset_names.clone();
            sorted_names.sort();
            
            for (i, name) in sorted_names.iter().enumerate() {
                info!("  {}. {}", i + 1, name);
            }
            info!("📋 End of dataset list");
        }
        
        if total_datasets == 0 {
            info!("All datasets are already indexed! Use --force to reindex.");
            return Ok(IndexingStats {
                files_processed: 0,
                series_indexed: 0,
                records_indexed: 0,
                errors: 0,
                processing_time_ms: start_time.elapsed().as_millis(),
            });
        }
        
        // Initialize live progress tracking (lock-free atomic counters)
        let progress_tracker = IndexerProgress::new(total_datasets);
        info!("📊 Live progress tracking initialized for {} datasets (PID: {})", 
             total_datasets, std::process::id());
        
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
        let tasks: Vec<DatasetTask> = datasets_to_process
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
        
        // Process datasets in parallel with progress tracking and graceful shutdown
        let mut results = Vec::new();
        let mut stream = stream::iter(tasks)
            .map(|task| {
                let indexer = indexer.clone();
                let semaphore = semaphore.clone();
                let progress = main_progress.clone();
                let shutdown_signal = shutdown_signal.clone();
                let progress_tracker = progress_tracker.clone();
                
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    
                    // Check for shutdown signal before starting
                    if shutdown_signal.load(Ordering::SeqCst) {
                        let dataset_name = indexer.extract_dataset_name_from_metadata_key(&task.metadata_key);
                        progress.set_message(format!("🛑 Shutdown requested, skipping: {}", dataset_name));
                        return DatasetResult {
                            dataset: None,
                            index: task.index,
                            error: Some(anyhow::anyhow!("Indexing interrupted by shutdown signal")),
                        };
                    }
                    
                    // Extract dataset name for status tracking
                    let dataset_name = indexer.extract_dataset_name_from_metadata_key(&task.metadata_key);
                    
                    // Mark as started
                    if let Err(e) = indexer.db.mark_dataset_started(&dataset_name) {
                        warn!("Failed to mark dataset {} as started: {}", dataset_name, e);
                    }
                    
                    let result = indexer
                        .process_metadata_file(&task.metadata_key, &task.data_prefix)
                        .await;
                    
                    let dataset_result = match result {
                        Ok(dataset) => {
                            // Check for shutdown signal before marking completed
                            if shutdown_signal.load(Ordering::SeqCst) {
                                warn!("🛑 Shutdown signal received during processing of {}, marking as pending for next run", dataset.name);
                                let _ = indexer.db.update_dataset_status(&dataset.dataset_id, IndexingStatus::Pending, None);
                                progress.set_message(format!("🛑 Interrupted: {}", dataset.name));
                                DatasetResult {
                                    dataset: None,
                                    index: task.index,
                                    error: Some(anyhow::anyhow!("Indexing interrupted by shutdown signal")),
                                }
                            } else {
                                // Mark as completed
                                if let Err(e) = indexer.db.mark_dataset_completed(&dataset.dataset_id) {
                                    warn!("Failed to mark dataset {} as completed: {}", dataset.dataset_id, e);
                                }
                                // Update live progress tracker (atomic increment)
                                let completed_count = progress_tracker.increment_completed();
                                progress.set_message(format!("✅ Completed: {} ({}/{})", 
                                                    dataset.name, completed_count, progress_tracker.total_count()));
                                DatasetResult {
                                    dataset: Some(dataset),
                                    index: task.index,
                                    error: None,
                                }
                            }
                        }
                        Err(e) => {
                            // Mark as failed unless shutdown was requested
                            if shutdown_signal.load(Ordering::SeqCst) {
                                let _ = indexer.db.update_dataset_status(&dataset_name, IndexingStatus::Pending, None);
                                progress.set_message(format!("🛑 Interrupted: {}", dataset_name));
                            } else {
                                let error_msg = e.to_string();
                                if let Err(db_err) = indexer.db.mark_dataset_failed(&dataset_name, &error_msg) {
                                    warn!("Failed to mark dataset {} as failed: {}", dataset_name, db_err);
                                }
                                // Update live progress tracker (atomic increment for failures)
                                let failed_count = progress_tracker.increment_failed();
                                progress.set_message(format!("❌ Failed: {} ({} failed)", dataset_name, failed_count));
                            }
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
            .buffer_unordered(max_concurrent);
        
        // Collect results while checking for shutdown
        while let Some(result) = stream.next().await {
            results.push(result);
            
            // If shutdown signal received, break early
            if shutdown_signal.load(Ordering::SeqCst) {
                warn!("🛑 Shutdown signal received, stopping further processing...");
                main_progress.set_message("🛑 Shutting down gracefully...");
                break;
            }
        }
        
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
        
        // Check if shutdown was requested
        let was_interrupted = shutdown_signal.load(Ordering::SeqCst);
        
        if was_interrupted {
            main_progress.finish_with_message(format!(
                "🛑 Gracefully shut down! {} datasets completed, {} errors. Progress saved - resume with same command.", 
                datasets_processed, errors
            ));
            warn!("Indexing was interrupted by shutdown signal. Progress has been saved to database.");
            warn!("You can resume indexing by running the same command again - completed datasets will be skipped.");
        } else {
            main_progress.finish_with_message(format!(
                "✅ Completed! {} datasets, {} errors", 
                datasets_processed, errors
            ));
            info!("Parallel indexing completed successfully!");
        }
        
        // Cleanup progress tracking on completion/shutdown
        if !was_interrupted {
            // Force write final state before cleanup
            let _ = progress_tracker.write_state_to_file();
        }
        progress_tracker.cleanup_on_shutdown();
        
        let stats = IndexingStats {
            files_processed: datasets_processed,
            series_indexed: series_count as usize,
            records_indexed: series_count as usize,
            errors,
            processing_time_ms: start_time.elapsed().as_millis(),
        };
        Ok(stats)
    }
    
    async fn process_metadata_file(&self, metadata_key: &str, data_prefix: &str) -> Result<Dataset> {
        debug!("Processing metadata file: {}", metadata_key);
        
        // Download and parse metadata
        let metadata = self.s3_client.download_and_parse_metadata(metadata_key).await?;
        
        // Find the correct case-sensitive directory name for this dataset
        let dataset_name = &metadata.name;
        let actual_dir_name = match self.s3_client.find_case_sensitive_dataset_dir(data_prefix, dataset_name).await? {
            Some(dir_name) => dir_name,
            None => {
                warn!("No data directory found for dataset: {} (skipping)", dataset_name);
                return Err(anyhow::anyhow!("No data directory found for dataset: {}", dataset_name));
            }
        };
        
        // List data files for this dataset using the correct case-sensitive directory name
        let dataset_data_prefix = format!("{}{}/", data_prefix, actual_dir_name);
        let data_objects = self.s3_client.list_objects(&dataset_data_prefix, None).await.unwrap_or_default();
        let data_files = self.s3_client.filter_data_files(&data_objects);
        
        info!("Found {} data files for dataset: {} (directory: {})", data_files.len(), dataset_name, actual_dir_name);
        
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
            // Resumable indexing fields
            indexing_status: IndexingStatus::InProgress, // Will be updated to Completed on success
            indexing_started_at: Some(Utc::now()),
            indexing_completed_at: None,
            indexing_error: None,
            metadata_file_path: Some(metadata_key.to_string()),
            data_file_count: data_files.len() as i64,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        
        // Insert dataset into database
        self.db.insert_dataset(&dataset)?;
        
        Ok(dataset)
    }
    
    /// Extract dataset name from metadata file key
    /// e.g. "metadata/lotsa_long_format/alibaba_cluster_trace_2018_metadata.yaml" -> "alibaba_cluster_trace_2018"
    fn extract_dataset_name_from_metadata_key(&self, metadata_key: &str) -> String {
        metadata_key
            .split('/')
            .last()
            .unwrap_or(metadata_key)
            .trim_end_matches("_metadata.yaml")
            .trim_end_matches("_metadata.yml")
            .trim_end_matches(".yaml")
            .trim_end_matches(".yml")
            .to_string()
    }
}