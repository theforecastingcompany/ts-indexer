use anyhow::Result;
use clap::Args;
use tracing::info;

use crate::db::Database;
use crate::indexer::Indexer;
use crate::s3::S3Client;
use crate::search::{SearchEngine, create_search_query};

#[derive(Args)]
pub struct IndexCommand {
    /// S3 bucket name
    #[arg(short, long)]
    pub bucket: String,
    
    /// S3 prefix for data files
    #[arg(short, long, default_value = "lotsa_long_format/")]
    pub prefix: String,
    
    /// S3 prefix for metadata files  
    #[arg(short, long, default_value = "metadata/lotsa_long_format/")]
    pub metadata_prefix: String,
    
    /// AWS region
    #[arg(short, long, default_value = "eu-west-3")]
    pub region: String,
    
    /// Maximum files to process (for testing)
    #[arg(long)]
    pub max_files: Option<usize>,
    
    /// Force rebuild of existing index (ignore completed datasets)
    #[arg(long, help = "Force reindex all datasets, ignoring completion status")]
    pub force: bool,
    
    /// Number of concurrent workers for resumable parallel processing
    #[arg(short, long, default_value = "8", help = "Number of parallel workers (1-16, default: 8)")]
    pub concurrency: usize,
}

impl IndexCommand {
    pub async fn execute(self) -> Result<()> {
        if self.force {
            info!("🔄 Starting resumable indexing process with FORCE REBUILD...");
        } else {
            info!("🚀 Starting resumable indexing process...");
        }
        info!("📍 Bucket: {}, Prefix: {}", self.bucket, self.prefix);
        info!("📁 Metadata prefix: {}", self.metadata_prefix);
        info!("🌍 Region: {}", self.region);
        info!("👥 Workers: {} parallel threads", self.concurrency);
        
        if let Some(max) = self.max_files {
            info!("🔢 Max files to process: {} (testing mode)", max);
        }
        
        if self.force {
            info!("⚠️  Force rebuild enabled - will reindex all datasets regardless of completion status");
        } else {
            info!("📊 Resume mode enabled - will skip already completed datasets");
        }
        
        // Initialize database
        let db = Database::new("ts_indexer.db")?;
        
        // Initialize S3 client
        let s3_client = S3Client::new(self.bucket, self.region).await?;
        
        // Create indexer and run with resumable functionality
        let indexer = Indexer::new(db, s3_client);
        let stats = indexer.index_data_parallel_with_resume(
            &self.prefix, 
            &self.metadata_prefix, 
            self.max_files, 
            self.concurrency,
            self.force // Use force flag for reindexing
        ).await?;
        
        println!("\n🎉 Resumable indexing completed!");
        println!("📂 Datasets processed: {}", stats.files_processed);
        println!("📊 Series indexed: {}", stats.series_indexed);
        println!("📈 Records indexed: {}", stats.records_indexed);
        println!("⏱️  Processing time: {}ms", stats.processing_time_ms);
        
        if !self.force {
            println!("\n💡 Tip: Use --force to reindex completed datasets, or Ctrl+C for graceful shutdown");
        }
        
        Ok(())
    }
}

#[derive(Args)]
pub struct SearchCommand {
    /// Search query (fuzzy search across themes, IDs, names, descriptions)
    #[arg(help = "Search term to find datasets (supports partial matches)")]
    pub query: String,
    
    /// Limit number of results
    #[arg(short, long, default_value = "10", help = "Maximum number of results to return")]
    pub limit: usize,
    
    /// Output format
    #[arg(short, long, value_enum, default_value = "table", help = "Output format: table, json, or csv")]
    pub format: OutputFormat,
    
    /// Show time-series data preview
    #[arg(long, help = "Show additional dataset preview information")]
    pub preview: bool,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl SearchCommand {
    pub async fn execute(self) -> Result<()> {
        info!("Searching for: '{}'", self.query);
        info!("Limit: {}, Format: {:?}", self.limit, self.format);
        
        if self.preview {
            info!("Preview mode enabled");
        }
        
        // Initialize database
        let db = Database::new("ts_indexer.db")?;
        
        // Create search engine
        let search_engine = SearchEngine::new(db);
        
        // Perform search
        let search_query = create_search_query(self.query, Some(self.limit));
        let results = search_engine.search(search_query).await?;
        
        // Display results
        match self.format {
            OutputFormat::Table => {
                println!("\nSearch Results ({} found in {}ms):", results.total_results, results.search_time_ms);
                println!("{:-<80}", "");
                for result in &results.results {
                    println!("Series ID: {}", result.search_result.series_id);
                    println!("Dataset: {}", result.search_result.dataset_id);
                    if let Some(theme) = &result.search_result.theme {
                        println!("Theme: {}", theme);
                    }
                    println!("Records: {}", result.search_result.record_count);
                    println!("Relevance: {:.2}", result.relevance_score);
                    println!("{:-<80}", "");
                }
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&results)?);
            }
            OutputFormat::Csv => {
                println!("series_id,dataset_id,theme,record_count,relevance_score");
                for result in &results.results {
                    println!("{},{},{},{},{:.2}", 
                        result.search_result.series_id,
                        result.search_result.dataset_id,
                        result.search_result.theme.as_deref().unwrap_or(""),
                        result.search_result.record_count,
                        result.relevance_score
                    );
                }
            }
        }
        
        Ok(())
    }
}

#[derive(Args)]  
pub struct StatusCommand {
    /// Show detailed statistics and progress with dataset breakdowns
    #[arg(short, long, help = "Show detailed statistics including progress breakdowns and dataset details")]
    pub detailed: bool,
}

impl StatusCommand {
    pub async fn execute(self) -> Result<()> {
        info!("Showing database status and indexing progress...");
        
        if self.detailed {
            info!("Detailed mode enabled");
        }
        
        // Try to initialize database, handle lock conflicts gracefully
        let db = match Database::new("ts_indexer.db") {
            Ok(db) => db,
            Err(e) if e.to_string().contains("Conflicting lock") => {
                println!("⏳ Indexer is currently running, trying read-only access...");
                // Try opening in read-only mode by connecting to a copy
                match std::fs::copy("ts_indexer.db", "ts_indexer_readonly.db") {
                    Ok(_) => {
                        let readonly_db = Database::new("ts_indexer_readonly.db")?;
                        // Clean up the temporary file after we're done
                        let _ = std::fs::remove_file("ts_indexer_readonly.db");
                        readonly_db
                    },
                    Err(_) => {
                        println!("❌ Cannot access database while indexer is running.");
                        println!("💡 Try: kill the indexer gracefully (Ctrl+C) or wait for it to complete.");
                        return Ok(());
                    }
                }
            },
            Err(e) => return Err(e),
        };
        
        // Get both statistics and progress
        let stats = db.get_stats()?;
        let progress = db.get_indexing_progress()?;
        
        // Display unified status information
        println!("\n📊 Database Status & Statistics:");
        println!("{:-<60}", "");
        
        // Dataset progress overview
        println!("📈 Dataset Progress:");
        println!("  ✅ Completed: {} datasets", progress.completed);
        println!("  🔄 In Progress: {} datasets", progress.in_progress);
        println!("  ❌ Failed: {} datasets", progress.failed);
        println!("  ⏳ Pending: {} datasets", progress.pending);
        println!("  📊 Total: {} datasets", progress.total());
        println!("  🎯 Completion: {:.1}%", progress.completion_percentage());
        
        println!();
        
        // Data statistics (from completed datasets only)
        println!("📋 Data Statistics (Completed Only):");
        println!("  🔢 Series Indexed: {}", stats.series_count);
        println!("  📈 Records Indexed: {}", stats.record_count);
        
        if let (Some(earliest), Some(latest)) = (stats.earliest_timestamp, stats.latest_timestamp) {
            println!("  📅 Date Range: {} to {}", 
                earliest.format("%Y-%m-%d"), 
                latest.format("%Y-%m-%d"));
        }
        
        // Show detailed breakdown if requested
        if self.detailed {
            println!("\n📋 Detailed Breakdown:");
            
            // Show failed datasets
            if progress.failed > 0 {
                println!("\n❌ Failed Datasets:");
                let failed_datasets = db.get_datasets_by_status(crate::db::IndexingStatus::Failed)?;
                for dataset in failed_datasets.iter().take(10) {
                    let error_msg = dataset.indexing_error.as_deref().unwrap_or("Unknown error");
                    println!("  • {}: {}", dataset.name, error_msg);
                }
                if failed_datasets.len() > 10 {
                    println!("  ... and {} more", failed_datasets.len() - 10);
                }
            }
            
            // Show in-progress datasets
            if progress.in_progress > 0 {
                println!("\n🔄 In Progress Datasets:");
                let in_progress_datasets = db.get_datasets_by_status(crate::db::IndexingStatus::InProgress)?;
                for dataset in in_progress_datasets.iter().take(5) {
                    if let Some(started) = dataset.indexing_started_at {
                        let duration = chrono::Utc::now().signed_duration_since(started);
                        println!("  • {} (running for {})", dataset.name, humanize_duration(duration));
                    } else {
                        println!("  • {}", dataset.name);
                    }
                }
            }
            
            // Show recently completed datasets
            if progress.completed > 0 {
                println!("\n✅ Recently Completed Datasets:");
                let completed_datasets = db.get_datasets_by_status(crate::db::IndexingStatus::Completed)?;
                // Sort by completion time (most recent first) and show top 5
                let mut recent_completed: Vec<_> = completed_datasets.iter().collect();
                recent_completed.sort_by(|a, b| {
                    b.indexing_completed_at.cmp(&a.indexing_completed_at)
                });
                
                for dataset in recent_completed.iter().take(5) {
                    if let Some(completed) = dataset.indexing_completed_at {
                        let duration = completed.signed_duration_since(
                            dataset.indexing_started_at.unwrap_or(completed)
                        );
                        println!("  • {} (took {})", dataset.name, humanize_duration(duration));
                    } else {
                        println!("  • {}", dataset.name);
                    }
                }
                
                if completed_datasets.len() > 5 {
                    println!("  ... and {} more completed", completed_datasets.len() - 5);
                }
            }
        }
        
        Ok(())
    }
}

/// Convert duration to human-readable format
fn humanize_duration(duration: chrono::Duration) -> String {
    let total_seconds = duration.num_seconds().abs(); // Handle negative durations
    
    if total_seconds < 60 {
        format!("{}s", total_seconds)
    } else if total_seconds < 3600 {
        format!("{}m {}s", total_seconds / 60, total_seconds % 60)
    } else {
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        format!("{}h {}m", hours, minutes)
    }
}