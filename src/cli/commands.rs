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
    
    /// Force rebuild of existing index
    #[arg(long)]
    pub force: bool,
    
    /// Number of concurrent workers for parallel processing
    #[arg(short, long, default_value = "8")]
    pub concurrency: usize,
}

impl IndexCommand {
    pub async fn execute(self) -> Result<()> {
        info!("Starting indexing process...");
        info!("Bucket: {}, Prefix: {}", self.bucket, self.prefix);
        info!("Metadata prefix: {}", self.metadata_prefix);
        info!("Region: {}", self.region);
        
        if let Some(max) = self.max_files {
            info!("Max files to process: {}", max);
        }
        
        if self.force {
            info!("Force rebuild enabled");
        }
        
        // Initialize database
        let db = Database::new("ts_indexer.db")?;
        
        // Initialize S3 client
        let s3_client = S3Client::new(self.bucket, self.region).await?;
        
        // Create indexer and run
        let indexer = Indexer::new(db, s3_client);
        let stats = indexer.index_data_parallel(
            &self.prefix, 
            &self.metadata_prefix, 
            self.max_files, 
            self.concurrency
        ).await?;
        
        println!("Indexing completed!");
        println!("Files processed: {}", stats.files_processed);
        println!("Series indexed: {}", stats.series_indexed);
        println!("Records indexed: {}", stats.records_indexed);
        println!("Processing time: {}ms", stats.processing_time_ms);
        
        Ok(())
    }
}

#[derive(Args)]
pub struct SearchCommand {
    /// Search query (fuzzy search across themes, IDs, names)
    pub query: String,
    
    /// Limit number of results
    #[arg(short, long, default_value = "10")]
    pub limit: usize,
    
    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub format: OutputFormat,
    
    /// Show time-series data preview
    #[arg(long)]
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
pub struct StatsCommand {
    /// Show detailed statistics
    #[arg(short, long)]
    pub detailed: bool,
}

impl StatsCommand {
    pub async fn execute(self) -> Result<()> {
        info!("Showing index statistics...");
        
        if self.detailed {
            info!("Detailed mode enabled");
        }
        
        // Initialize database
        let db = Database::new("ts_indexer.db")?;
        
        // Get statistics
        let stats = db.get_stats()?;
        
        println!("\nDatabase Statistics:");
        println!("{:-<50}", "");
        println!("Datasets: {}", stats.dataset_count);
        println!("Series: {}", stats.series_count);
        println!("Records: {}", stats.record_count);
        
        if let (Some(earliest), Some(latest)) = (stats.earliest_timestamp, stats.latest_timestamp) {
            println!("Date range: {} to {}", 
                earliest.format("%Y-%m-%d"), 
                latest.format("%Y-%m-%d"));
        }
        
        Ok(())
    }
}