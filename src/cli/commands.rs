use anyhow::Result;
use clap::Args;
use std::io::{self, Write};
use tracing::info;
use serde_json;

use crate::cli::interactive::InteractiveFinder;
use crate::db::{Database, ColumnType, Dataset, IndexingStatus};
use crate::indexer::Indexer;
use crate::plotting::TimeSeriesPlotter;
use crate::progress::ProgressState;
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
    
    /// Filter to specific datasets (comma-separated list or single dataset name)
    #[arg(long, help = "Filter to specific datasets by name (e.g., 'beijing_subway_30min' or 'beijing_subway_30min,alibaba_cluster_trace_2018')")]
    pub filter: Option<String>,
    
    /// Enable detailed resource monitoring and performance logging
    #[arg(long, help = "Enable detailed resource monitoring (memory, threads, timing)")]
    pub monitor: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
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
        
        if let Some(ref filter) = self.filter {
            info!("🔍 Dataset filter: {}", filter);
        }
        
        if self.monitor {
            info!("📊 Resource monitoring enabled");
            crate::monitoring::init_monitoring("index_command");
        }
        
        if self.force {
            info!("⚠️  Force rebuild enabled - will reindex all datasets regardless of completion status");
        } else {
            info!("📊 Resume mode enabled - will skip already completed datasets");
        }
        
        // Initialize database
        info!("📊 Using database: {}", self.database);
        let db = Database::new(&self.database)?;
        
        // Initialize S3 client
        let s3_client = S3Client::new(self.bucket, self.region).await?;
        
        // Create indexer and run with resumable functionality
        let indexer = Indexer::new(db, s3_client);
        let stats = indexer.index_data_parallel_with_resume(
            &self.prefix, 
            &self.metadata_prefix, 
            self.max_files, 
            self.concurrency,
            self.force, // Use force flag for reindexing
            self.filter.as_deref(), // Pass the filter option
            self.monitor // Pass the monitoring flag
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
    #[arg(help = "Search term to find datasets (supports partial matches). Optional - if not provided, interactive mode will be used")]
    pub query: Option<String>,
    
    /// Limit number of results
    #[arg(short, long, default_value = "10", help = "Maximum number of results to return")]
    pub limit: usize,
    
    /// Output format
    #[arg(short, long, value_enum, default_value = "table", help = "Output format: table, json, or csv")]
    pub format: OutputFormat,
    
    /// Show time-series data preview
    #[arg(long, help = "Show additional dataset preview information")]
    pub preview: bool,
    
    /// Interactive hierarchical search mode
    #[arg(short, long, help = "Enable interactive hierarchical search with navigation")]
    pub interactive: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
}

impl SearchCommand {
    pub async fn execute(self) -> Result<()> {
        let query_str = self.query.as_deref().unwrap_or("");
        info!("Searching for: '{}'", query_str);
        info!("Limit: {}, Format: {:?}", self.limit, self.format);
        
        if self.preview {
            info!("Preview mode enabled");
        }
        
        // Initialize database
        info!("📊 Using database: {}", self.database);
        let db = Database::new(&self.database)?;
        
        // Create search engine
        let search_engine = SearchEngine::new(db);
        
        // If no query provided, automatically use interactive mode
        if self.interactive || self.query.is_none() {
            // Interactive hierarchical search mode
            self.run_interactive_search(search_engine).await
        } else {
            // Traditional search mode - simplified for now
            let search_query = create_search_query(self.query.as_deref().unwrap_or("").to_string(), Some(self.limit));
            let results = search_engine.search(search_query).await?;
            
            match self.format {
                OutputFormat::Table => {
                    self.display_table_results(&results.results)?;
                }
                OutputFormat::Json => {
                    let json = serde_json::to_string_pretty(&results.results)?;
                    println!("{}", json);
                }
                OutputFormat::Csv => {
                    self.display_csv_results(&results.results)?;
                }
            }
            
            Ok(())
        }
    }
    
    async fn run_interactive_search(&self, search_engine: SearchEngine) -> Result<()> {
        // Initialize database for the interactive finder
        let db = Database::new(&self.database)?;
        
        // Create and run the fzf-style interactive finder with the existing search engine
        let finder = InteractiveFinder::with_search_engine(db, search_engine);
        
        match finder.run().await? {
            Some(selected_results) => {
                println!("\n✅ Selected {} series:", selected_results.len());
                
                // Display selected results in requested format
                match self.format {
                    OutputFormat::Table => {
                        self.display_table_results(&selected_results)?;
                    }
                    OutputFormat::Json => {
                        let json = serde_json::to_string_pretty(&selected_results)?;
                        println!("{}", json);
                    }
                    OutputFormat::Csv => {
                        self.display_csv_results(&selected_results)?;
                    }
                }
                
                // Show preview if requested (disabled for now to fix compilation)
                if self.preview && !selected_results.is_empty() {
                    println!("\n🔍 Time Series Preview: (Preview functionality temporarily disabled)");
                    // TODO: Re-implement proper plotting integration
                }
            }
            None => {
                println!("Search cancelled.");
            }
        }
        
        Ok(())
    }

    fn display_table_results(&self, results: &[crate::search::EnhancedSearchResult]) -> Result<()> {
        println!("📊 Search Results ({})", results.len());
        for (i, result) in results.iter().enumerate() {
            println!("{}. {} - {} records", 
                i + 1, 
                result.search_result.series_id,
                result.search_result.record_count
            );
        }
        Ok(())
    }

    fn display_csv_results(&self, results: &[crate::search::EnhancedSearchResult]) -> Result<()> {
        println!("series_id,dataset_id,record_count,relevance_score");
        for result in results {
            println!("{},{},{},{:.2}",
                result.search_result.series_id,
                result.search_result.dataset_id,
                result.search_result.record_count,
                result.relevance_score
            );
        }
        Ok(())
    }
}

#[derive(Args)]  
pub struct StatusCommand {
    /// Show detailed statistics and progress with dataset breakdowns
    #[arg(short, long, help = "Show detailed statistics including progress breakdowns and dataset details")]
    pub detailed: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
}

impl StatusCommand {
    pub async fn execute(self) -> Result<()> {
        info!("Showing database status and indexing progress...");
        
        // Check for live indexer progress first
        match ProgressState::read_from_file()? {
            Some(live_state) if live_state.is_indexer_alive() && !live_state.is_stale(5) => {
                // We have a live indexer! Show real-time status
                return self.display_live_status(&live_state).await;
            }
            Some(stale_state) => {
                println!("⚠️  Found stale indexer state (PID: {}, last update: {})", 
                        stale_state.indexer_pid,
                        stale_state.last_updated.format("%H:%M:%S"));
                println!("📊 Falling back to database-only status...\n");
            }
            None => {
                // No live progress file, continue with database-only status
            }
        }
        
        if self.detailed {
            info!("Detailed mode enabled");
        }
        
        // Try to initialize database, handle lock conflicts gracefully
        info!("📊 Using database: {}", self.database);
        let db = match Database::new(&self.database) {
            Ok(db) => db,
            Err(e) if e.to_string().contains("Conflicting lock") => {
                println!("⏳ Indexer is currently running, trying read-only access...");
                // Try opening in read-only mode by connecting to a copy
                let readonly_path = format!("{}_readonly.db", self.database.trim_end_matches(".db"));
                match std::fs::copy(&self.database, &readonly_path) {
                    Ok(_) => {
                        let readonly_db = Database::new(&readonly_path)?;
                        // Clean up the temporary file after we're done
                        let _ = std::fs::remove_file(&readonly_path);
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
        
        if self.detailed && progress.total() > 0 {
            self.display_detailed_progress(&db, &progress).await?;
        }
        
        Ok(())
    }
    
    /// Display detailed dataset progress breakdown
    async fn display_detailed_progress(&self, db: &Database, progress: &crate::db::IndexingProgress) -> Result<()> {
        println!("\n📋 Detailed Dataset Progress:");
        println!("{:-<60}", "");
        
        // Show dataset breakdown by status  
        // Note: Temporarily disabled - get_dataset_progress_details is a stub
        // let dataset_details = db.get_dataset_progress_details()?;
        // 
        // if !dataset_details.is_empty() {
        //     for detail in dataset_details.iter().take(20) { // Limit to top 20 for readability
        //         let status_emoji = match detail.status.as_str() {
        //             "completed" => "✅",
        //             "in_progress" => "🔄",
        //             "failed" => "❌",
        //             _ => "⏳",
        //         };
        //         
        //         println!("  {} {} ({} series, {} records)", 
        //                 status_emoji, 
        //                 detail.dataset_name,
        //                 detail.series_count,
        //                 detail.record_count);
        //     }
        //     
        //     if dataset_details.len() > 20 {
        //         println!("  ... and {} more datasets", dataset_details.len() - 20);
        //     }
        // }
        
        Ok(())
    }
    
    /// Display real-time status from live indexer progress
    async fn display_live_status(&self, state: &ProgressState) -> Result<()> {
        println!("\n🚀 Live Indexer Status (PID: {}):", state.indexer_pid);
        println!("{:-<60}", "");
        
        let progress_pct = if state.total_datasets > 0 {
            (state.completed_count as f64 / state.total_datasets as f64) * 100.0
        } else {
            0.0
        };
        
        let progress_bar = self.create_progress_bar(progress_pct);
        let pending_count = state.total_datasets.saturating_sub(state.completed_count + state.failed_count);
        
        println!("📊 Progress: {} {}/{} datasets ({:.1}%)", 
                progress_bar,
                state.completed_count,
                state.total_datasets,
                progress_pct);
        
        println!("  ✅ Completed: {}", state.completed_count);
        println!("  ❌ Failed: {}", state.failed_count);
        println!("  ⏳ Pending: {}", pending_count);
        
        // Note: current_dataset field not available in ProgressState
        // if let Some(current) = &state.current_dataset {
        //     println!("  🔄 Currently processing: {}", current);
        // }
        
        println!("  🕒 Last update: {}", state.last_updated.format("%H:%M:%S"));
        
        // Show database stats if available  
        // Note: database field not available in ProgressState, using command's database path
        // if !state.database.is_empty() {
        if let Ok(db) = Database::new(&self.database) {
            let stats = db.get_stats()?;
            println!("\n📋 Database Statistics:");
            println!("  🔢 Series Indexed: {}", stats.series_count);
            println!("  📈 Records Indexed: {}", stats.record_count);
        }
        
        Ok(())
    }
    
    /// Create a visual progress bar
    fn create_progress_bar(&self, percentage: f64) -> String {
        let bar_width: usize = 30;
        let filled = ((percentage / 100.0) * bar_width as f64) as usize;
        let empty = bar_width.saturating_sub(filled);
        
        format!("[{}{}]", 
                "█".repeat(filled), 
                "░".repeat(empty))
    }
}

#[derive(Args)]
pub struct CleanCommand {
    /// Force clean without confirmation prompt
    #[arg(short, long, help = "Skip confirmation prompt and force clean all states")]
    pub force: bool,
    
    /// Clean only progress files, keep database
    #[arg(long, help = "Clean only progress tracking files, preserve database")]
    pub progress_only: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
}

impl CleanCommand {
    pub async fn execute(self) -> Result<()> {
        println!("🧹 Clean Command - Reinitialize All States");
        println!("{:-<60}", "");
        
        // Check for running indexers first
        if let Ok(Some(state)) = ProgressState::read_from_file() {
            if state.is_indexer_alive() && !state.is_stale(5) {
                println!("⚠️  Active indexer detected (PID: {})", state.indexer_pid);
                println!("   Cannot clean while indexer is running.");
                println!("   Please stop the indexer first (Ctrl+C) then try again.");
                return Ok(());
            }
        }
        
        if !self.force {
            println!("This will remove:");
            if !self.progress_only {
                println!("  📊 Database file: {}", self.database);
            }
            println!("  📈 Progress tracking files");
            println!("  🔄 All indexing state");
            println!();
            
            print!("Are you sure? (y/N): ");
            use std::io::{self, Write};
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            
            if !input.trim().to_lowercase().starts_with('y') {
                println!("❌ Clean operation cancelled");
                return Ok(());
            }
        }
        
        // Clean progress files
        println!("🗑️  Removing progress files...");
        let _ = std::fs::remove_file("ts_indexer_progress.json");
        
        // Clean database if not progress-only
        if !self.progress_only {
            println!("🗑️  Removing database: {}", self.database);
            let _ = std::fs::remove_file(&self.database);
            let _ = std::fs::remove_file(format!("{}-shm", &self.database));
            let _ = std::fs::remove_file(format!("{}-wal", &self.database));
        }
        
        println!("✅ Clean completed successfully!");
        println!("💡 You can now start fresh indexing with: cargo run -- index");
        
        Ok(())
    }
}

#[derive(Args)]
pub struct DebugCommand {
    /// Dataset ID to inspect
    #[arg(help = "Dataset ID to inspect (e.g., 'hzmetro')")]
    pub dataset_id: String,
    
    /// Show sample data from time_series table
    #[arg(short, long, help = "Show sample data from time_series table")]
    pub show_data: bool,
    
    /// Show extra debugging information  
    #[arg(short, long, help = "Show extra debugging information")]
    pub verbose: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
    
    /// Search query (stub field for compatibility)
    #[arg(long, help = "Search query")]
    pub query: Option<String>,
    
    /// Limit for results (stub field for compatibility)
    #[arg(long, default_value = "10", help = "Limit for results")]
    pub limit: usize,
    
    /// Output format (stub field for compatibility)
    #[arg(long, default_value = "table", help = "Output format")]
    pub format: OutputFormat,
}

impl DebugCommand {
    pub async fn execute(self) -> Result<()> {
        info!("🔍 Debugging dataset: {}", self.dataset_id);
        
        // Initialize database
        info!("📊 Using database: {}", self.database);
        let db = Database::new(&self.database)?;
        
        // Get dataset info
        let dataset_info = db.get_dataset_info(&self.dataset_id);
        
        println!("\n🔍 Dataset Debug Information");
        println!("{:-<60}", "");
        println!("📊 Dataset: {}", self.dataset_id);
        
        if let Ok((series_count, record_count)) = dataset_info {
            println!("  📈 Series Count: {}", series_count);
            println!("  📋 Record Count: {}", record_count);
            // Note: Date range info not available from get_dataset_info
            // println!("  📅 Date Range: {} to {}", info.min_date, info.max_date);
            
            if self.show_data {
                self.show_sample_data(&db).await?;
            }
            
            if self.verbose {
                self.show_detailed_info(&db).await?;
            }
        } else {
            println!("❌ Dataset '{}' not found in database", self.dataset_id);
        }
        
        Ok(())
    }
    
    async fn show_sample_data(&self, db: &Database) -> Result<()> {
        println!("\n📋 Sample Data:");
        println!("{:-<40}", "");
        
        // Get sample records
        let sample_data = db.get_sample_data(&self.dataset_id, 10)?;
        
        for (idx, record) in sample_data.iter().enumerate() {
            // Handle JSON Value access safely
            let series_id = record.get("series_id").and_then(|v| v.as_str()).unwrap_or("unknown");
            let timestamp = record.get("timestamp").and_then(|v| v.as_str()).unwrap_or("unknown");
            let value = record.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
            println!("{}. {} | {} | {}", idx + 1, series_id, timestamp, value);
        }
        
        Ok(())
    }
    
    async fn show_detailed_info(&self, db: &Database) -> Result<()> {
        println!("\n🔧 Detailed Information:");
        println!("{:-<40}", "");
        
        // Show additional debugging info
        println!("🗂️  Database tables and indexes");
        println!("🔍 Query performance statistics");
        println!("💾 Storage utilization");
        
        Ok(())
    }

    fn filter_series_direct(&self, series: &[crate::search::EnhancedSearchResult], filter_term: &str) -> Vec<crate::search::EnhancedSearchResult> {
        let filter_lower = filter_term.to_lowercase();
        series.iter()
            .filter(|s| {
                s.search_result.series_id.to_lowercase().contains(&filter_lower) ||
                s.search_result.dataset_id.to_lowercase().contains(&filter_lower) ||
                s.search_result.dataset_name.as_ref().map_or(false, |name| name.to_lowercase().contains(&filter_lower))
            })
            .cloned()
            .collect()
    }

    fn show_series_direct_help(&self) {
        println!("\n📖 SERIES SEARCH COMMANDS:");
        println!("  • Enter number: View series columns and plot data");
        println!("  • /filter <term>: Filter by series ID or dataset name");
        println!("  • /filter clear: Clear current filter");
        println!("  • /sort: Show sorting options");
        println!("  • /stats: Show statistics for current results");
        println!("  • h/help: Show this help");
        println!("  • q/quit: Exit program");
    }

    fn show_interactive_help(&self) {
        println!("📖 INTERACTIVE COMMANDS:");
        println!("  • Enter number: Navigate to dataset/series/column");
        println!("  • /filter <term>: Filter current list by term");
        println!("  • /stats: Show statistics for current selection");
        println!("  • /export: Export current data");
        println!("  • b/back: Go back one level");
        println!("  • h/help: Show this help");
        println!("  • q/quit: Exit");
        println!();
    }
    
    async fn display_dataset_level(&self, hierarchical_results: &std::collections::HashMap<String, DatasetGroup>, db: &Database, plotter: &TimeSeriesPlotter) -> Result<()> {
        use std::io::{self, Write};
        
        let mut filtered_datasets = hierarchical_results.clone();
        let mut current_filter = String::new();
        
        loop {
            // Clear screen and show current state
            self.clear_screen();
            let query_str = self.query.as_deref().unwrap_or("");
            println!("🔍 Interactive Search: '{}' (Enhanced Mode)\n", query_str);
            
            if !current_filter.is_empty() {
                println!("🔍 Filter: '{}'\n", current_filter);
            }
            
            // Display filtered datasets
            println!("📁 DATASETS ({} found)", filtered_datasets.len());
            if filtered_datasets.is_empty() {
                println!("  No datasets match the current filter.");
                println!("  Use '/filter <term>' to search or '/filter clear' to reset.\n");
            } else {
                let dataset_names: Vec<_> = filtered_datasets.keys().cloned().collect();
                
                for (idx, dataset_name) in dataset_names.iter().enumerate() {
                    let group = &filtered_datasets[dataset_name];
                    let series_count = group.total_series;  // Use actual series count from database
                    let total_records = group.total_records;
                    
                    println!("{}. 📂 {} ({} series, {} records)", 
                             idx + 1, dataset_name, series_count, total_records);
                }
            }
            
            print!("\n> ");
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            
            // Handle commands
            if input.starts_with("/filter ") {
                let filter_term = input.strip_prefix("/filter ").unwrap_or("").trim();
                if filter_term == "clear" {
                    filtered_datasets = hierarchical_results.clone();
                    current_filter.clear();
                    println!("✅ Filter cleared");
                } else {
                    current_filter = filter_term.to_string();
                    filtered_datasets = self.filter_datasets(&hierarchical_results, filter_term);
                    println!("✅ Applied filter: '{}'", filter_term);
                }
                std::thread::sleep(std::time::Duration::from_millis(1000));
                continue;
            } else if input == "/stats" {
                if let Err(e) = self.show_dataset_stats(hierarchical_results) {
                    println!("Error showing stats: {}", e);
                }
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "h" || input == "help" || input == "/help" {
                self.show_interactive_help();
                println!("Press Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "q" || input == "quit" {
                break;
            }
            
            // Handle dataset selection
            if let Ok(choice) = input.parse::<usize>() {
                let dataset_names: Vec<_> = filtered_datasets.keys().cloned().collect();
                if choice > 0 && choice <= dataset_names.len() {
                    let dataset_name = &dataset_names[choice - 1];
                    let group = &filtered_datasets[dataset_name];
                    
                    // Navigate to series level
                    if let Err(e) = self.show_enhanced_series_level(&db, &plotter, dataset_name, group).await {
                        println!("Error in series level: {}", e);
                        std::thread::sleep(std::time::Duration::from_millis(2000));
                    }
                } else {
                    println!("Invalid choice. Please enter a number between 1 and {}", dataset_names.len());
                    std::thread::sleep(std::time::Duration::from_millis(2000));
                }
            } else {
                println!("Invalid input. Enter a dataset number, command (/filter, /stats, /help), or 'q' to quit.");
                std::thread::sleep(std::time::Duration::from_millis(2000));
            }
        }
        
        Ok(())
    }
    
    fn clear_screen(&self) {
        // Simple clear with newlines for terminal compatibility
        print!("\n{}", "\n".repeat(50));
    }
    
    fn filter_datasets(&self, datasets: &std::collections::HashMap<String, DatasetGroup>, filter_term: &str) -> std::collections::HashMap<String, DatasetGroup> {
        let filter_lower = filter_term.to_lowercase();
        datasets.iter()
            .filter(|(name, group)| {
                // Filter by dataset name or series names
                name.to_lowercase().contains(&filter_lower) ||
                group.series.iter().any(|series| {
                    series.search_result.series_id.to_lowercase().contains(&filter_lower)
                })
            })
            .map(|(k, v)| (k.clone(), (*v).clone()))
            .collect()
    }
    
    fn show_dataset_stats(&self, datasets: &std::collections::HashMap<String, DatasetGroup>) -> Result<()> {
        let total_datasets = datasets.len();
        let total_series: usize = datasets.values().map(|g| g.series.len()).sum();
        let total_records: i64 = datasets.values().map(|g| g.total_records).sum();
        
        println!("\n📊 DATASET STATISTICS:");
        println!("  • Total Datasets: {}", total_datasets);
        println!("  • Total Series: {}", total_series);
        println!("  • Total Records: {}", total_records);
        
        if total_datasets > 0 {
            let avg_series_per_dataset = total_series as f64 / total_datasets as f64;
            let avg_records_per_dataset = total_records as f64 / total_datasets as f64;
            println!("  • Average Series per Dataset: {:.1}", avg_series_per_dataset);
            println!("  • Average Records per Dataset: {:.0}", avg_records_per_dataset);
        }
        
        // Show top 5 largest datasets
        let mut sorted_datasets: Vec<_> = datasets.iter().collect();
        sorted_datasets.sort_by_key(|(_, group)| std::cmp::Reverse(group.total_records));
        
        println!("\n🏆 TOP 5 LARGEST DATASETS:");
        for (i, (name, group)) in sorted_datasets.iter().take(5).enumerate() {
            println!("  {}. {} ({} records, {} series)", 
                     i + 1, name, group.total_records, group.series.len());
        }
        
        Ok(())
    }
    
    async fn show_series_level(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, group: &DatasetGroup) -> Result<()> {
        use std::io::{self, Write};
        
        println!("\n📂 {} > 📊 SERIES ({} series)", dataset_name, group.series.len());
        println!("Enter series number to view columns, 'b' to go back, or 'q' to quit\n");
        
        for (idx, series) in group.series.iter().enumerate() {
            println!("{}. 📊 {} ({} records, relevance: {:.2})", 
                     idx + 1, series.search_result.series_id,
                     series.search_result.record_count,
                     series.relevance_score);
        }
        
        loop {
            print!("\n> ");
            io::stdout().flush()?;
            
            let mut series_input = String::new();
            io::stdin().read_line(&mut series_input)?;
            let series_input = series_input.trim();
            
            if series_input == "q" || series_input == "quit" {
                std::process::exit(0);
            } else if series_input == "b" || series_input == "back" {
                return Ok(());
            }
            
            if let Ok(series_choice) = series_input.parse::<usize>() {
                if series_choice > 0 && series_choice <= group.series.len() {
                    let selected_series = &group.series[series_choice - 1];
                    let series_id = &selected_series.search_result.series_id;
                    
                    // Show column level
                    if let Err(e) = self.show_column_level(db, plotter, dataset_name, series_id).await {
                        println!("Error in column level: {}", e);
                    }
                    
                    // Show series again after returning from column level
                    println!("\n📂 {} > 📊 SERIES ({} series)", dataset_name, group.series.len());
                    for (idx, series) in group.series.iter().enumerate() {
                        println!("{}. 📊 {} ({} records, relevance: {:.2})", 
                                 idx + 1, series.search_result.series_id,
                                 series.search_result.record_count,
                                 series.relevance_score);
                    }
                } else {
                    println!("Invalid choice. Please enter a number between 1 and {}", group.series.len());
                }
            } else {
                println!("Invalid input. Enter a series number, 'b' to go back, or 'q' to quit.");
            }
        }
    }
    
    async fn show_enhanced_series_level(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, group: &DatasetGroup) -> Result<()> {
        use std::io::{self, Write};
        
        // Get ALL series for this dataset from the database, not just the limited search results
        // TODO: Fix method resolution issue
        // let all_series_for_dataset = self.get_all_series_for_dataset(db, dataset_name)?;
        let all_series_for_dataset: Vec<crate::search::EnhancedSearchResult> = vec![]; // Temporary empty vec
        let mut filtered_series = all_series_for_dataset;
        let mut current_filter = String::new();
        
        loop {
            // Clear screen and show current state
            self.clear_screen();
            println!("📂 {} > 📊 SERIES (Enhanced Mode)\n", dataset_name);
            
            if !current_filter.is_empty() {
                println!("🔍 Filter: '{}'\n", current_filter);
            }
            
            // Display filtered series
            println!("📊 SERIES ({} found)", filtered_series.len());
            if filtered_series.is_empty() {
                println!("  No series match the current filter.");
                println!("  Use '/filter <term>' to search or '/filter clear' to reset.\n");
            } else {
                for (idx, series) in filtered_series.iter().enumerate() {
                    println!("{}. 📊 {} ({} records, relevance: {:.2})", 
                             idx + 1, series.search_result.series_id,
                             series.search_result.record_count,
                             series.relevance_score);
                }
            }
            
            println!("\nCommands: /filter <term>, /sort, /preview <num>, /stats, h/help, b/back, q/quit");
            print!("\n> ");
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            
            // Handle commands
            if input.starts_with("/filter ") {
                let filter_term = input.strip_prefix("/filter ").unwrap_or("").trim();
                if filter_term == "clear" {
                    // filtered_series = self.get_all_series_for_dataset(db, dataset_name)?;
                    filtered_series = vec![]; // Temporary empty vec
                    current_filter.clear();
                    println!("✅ Filter cleared");
                } else {
                    current_filter = filter_term.to_string();
                    // let all_series = self.get_all_series_for_dataset(db, dataset_name)?;
                    let all_series: Vec<crate::search::EnhancedSearchResult> = vec![]; // Temporary empty vec
                    filtered_series = self.filter_series(&all_series, filter_term);
                    println!("✅ Applied filter: '{}'", filter_term);
                }
                std::thread::sleep(std::time::Duration::from_millis(1000));
                continue;
            } else if input == "/sort" {
                self.show_sort_options(&mut filtered_series);
                continue;
            } else if input.starts_with("/preview ") {
                if let Ok(num) = input.strip_prefix("/preview ").unwrap_or("").trim().parse::<usize>() {
                    if num > 0 && num <= filtered_series.len() {
                        let series_id = &filtered_series[num - 1].search_result.series_id;
                        self.show_series_preview(db, dataset_name, series_id).await?;
                        continue;
                    }
                }
                println!("Invalid preview number. Use /preview <1-{}>", filtered_series.len());
                std::thread::sleep(std::time::Duration::from_millis(2000));
                continue;
            } else if input == "/stats" {
                self.show_series_stats(&filtered_series);
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "h" || input == "help" || input == "/help" {
                self.show_series_help();
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "b" || input == "back" {
                return Ok(());
            } else if input == "q" || input == "quit" {
                std::process::exit(0);
            }
            
            // Handle series selection
            if let Ok(choice) = input.parse::<usize>() {
                if choice > 0 && choice <= filtered_series.len() {
                    let selected_series = &filtered_series[choice - 1];
                    let series_id = &selected_series.search_result.series_id;
                    
                    // Navigate to column level
                    if let Err(e) = self.show_enhanced_column_level(db, plotter, dataset_name, series_id).await {
                        println!("Error in column level: {}", e);
                        std::thread::sleep(std::time::Duration::from_millis(2000));
                    }
                } else {
                    println!("Invalid choice. Please enter a number between 1 and {}", filtered_series.len());
                    std::thread::sleep(std::time::Duration::from_millis(2000));
                }
            } else {
                println!("Invalid input. Enter a series number, command, or 'b'/'q'.");
                std::thread::sleep(std::time::Duration::from_millis(2000));
            }
        }
    }
    
    fn filter_series(&self, series: &[crate::search::EnhancedSearchResult], filter_term: &str) -> Vec<crate::search::EnhancedSearchResult> {
        let filter_lower = filter_term.to_lowercase();
        series.iter()
            .filter(|s| s.search_result.series_id.to_lowercase().contains(&filter_lower))
            .cloned()
            .collect()
    }
    
    fn show_sort_options(&self, series: &mut Vec<crate::search::EnhancedSearchResult>) {
        use std::io::{self, Write};
        
        println!("\n📊 SORT OPTIONS:");
        println!("  1. By relevance (highest first)");
        println!("  2. By record count (highest first)");
        println!("  3. By series ID (alphabetical)");
        println!("  4. By record count (lowest first)");
        
        print!("\nSelect sort option (1-4): ");
        io::stdout().flush().unwrap();
        
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        
        match input.trim().parse::<usize>() {
            Ok(1) => {
                series.sort_by(|a, b| b.relevance_score.partial_cmp(&a.relevance_score).unwrap());
                println!("✅ Sorted by relevance (highest first)");
            }
            Ok(2) => {
                series.sort_by_key(|s| std::cmp::Reverse(s.search_result.record_count));
                println!("✅ Sorted by record count (highest first)");
            }
            Ok(3) => {
                series.sort_by(|a, b| a.search_result.series_id.cmp(&b.search_result.series_id));
                println!("✅ Sorted by series ID (alphabetical)");
            }
            Ok(4) => {
                series.sort_by_key(|s| s.search_result.record_count);
                println!("✅ Sorted by record count (lowest first)");
            }
            _ => println!("❌ Invalid sort option"),
        }
        
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }
    
    async fn show_series_preview(&self, db: &Database, dataset_name: &str, series_id: &str) -> Result<()> {
        println!("\n🔍 SERIES PREVIEW: {}", series_id);
        println!("──────────────────────────────────────");
        
        // Show basic series info
        if let Ok(Some(series_info)) = db.get_series_with_column_metadata(series_id) {
            println!("📊 Dataset: {}", dataset_name);
            println!("🆔 Series ID: {}", series_id);
            println!("📈 Records: {}", series_info.series_metadata.record_count);
            
            if let Some(column_info) = &series_info.column_info {
                println!("📋 Columns: {}", column_info.columns.len());
                
                // Show column types summary
                let mut targets = 0;
                let mut historical = 0;
                let mut static_vars = 0;
                let mut future = 0;
                
                for col in &column_info.columns {
                    match col.column_type {
                        ColumnType::Target => targets += 1,
                        ColumnType::HistoricalCovariate => historical += 1,
                        ColumnType::StaticCovariate => static_vars += 1,
                        ColumnType::FutureCovariate => future += 1,
                        ColumnType::Timestamp => {}, // Count separately if needed
                    }
                }
                
                println!("  • 🎯 Targets: {}", targets);
                println!("  • 📈 Historical: {}", historical);
                println!("  • 💎 Static: {}", static_vars);
                println!("  • 🔮 Future: {}", future);
            }
        }
        
        println!("\nPress Enter to continue...");
        let mut _dummy = String::new();
        std::io::stdin().read_line(&mut _dummy)?;
        
        Ok(())
    }
    
    fn show_series_stats(&self, series: &[crate::search::EnhancedSearchResult]) {
        if series.is_empty() {
            println!("\n📊 No series to show statistics for.");
            return;
        }
        
        let total_series = series.len();
        let total_records: i64 = series.iter().map(|s| s.search_result.record_count).sum();
        let avg_records = total_records as f64 / total_series as f64;
        
        let min_records = series.iter().map(|s| s.search_result.record_count).min().unwrap_or(0);
        let max_records = series.iter().map(|s| s.search_result.record_count).max().unwrap_or(0);
        
        let avg_relevance = series.iter().map(|s| s.relevance_score).sum::<f64>() / total_series as f64;
        
        println!("\n📊 SERIES STATISTICS:");
        println!("  • Total Series: {}", total_series);
        println!("  • Total Records: {}", total_records);
        println!("  • Average Records per Series: {:.1}", avg_records);
        println!("  • Min Records: {}", min_records);
        println!("  • Max Records: {}", max_records);
        println!("  • Average Relevance Score: {:.2}", avg_relevance);
        
        // Show distribution by record count ranges
        let mut small = 0;  // < 100 records
        let mut medium = 0; // 100-1000 records
        let mut large = 0;  // > 1000 records
        
        for s in series {
            let records = s.search_result.record_count;
            if records < 100 {
                small += 1;
            } else if records <= 1000 {
                medium += 1;
            } else {
                large += 1;
            }
        }
        
        println!("\n📈 SIZE DISTRIBUTION:");
        println!("  • Small (<100 records): {}", small);
        println!("  • Medium (100-1000 records): {}", medium);
        println!("  • Large (>1000 records): {}", large);
    }
    
    fn show_series_help(&self) {
        println!("\n📖 SERIES LEVEL COMMANDS:");
        println!("  • Enter number: View series columns");
        println!("  • /filter <term>: Filter series by name");
        println!("  • /filter clear: Clear current filter");
        println!("  • /sort: Show sorting options");
        println!("  • /preview <num>: Quick preview of series");
        println!("  • /stats: Show statistics for current series");
        println!("  • /export: Export current series list");
        println!("  • h/help: Show this help");
        println!("  • b/back: Go back to datasets");
        println!("  • q/quit: Exit program");
    }
    
    async fn show_enhanced_column_level(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, series_id: &str) -> Result<()> {
        use std::io::{self, Write};
        use std::collections::HashMap;
        
        // Try to get enhanced column metadata first, fallback to legacy
        let enhanced_info = db.get_enhanced_dataset_column_metadata(&self.extract_dataset_id_from_series(series_id))?;
        
        if let Some(enhanced) = enhanced_info {
            self.show_enhanced_feature_mode(db, plotter, dataset_name, series_id, &enhanced).await
        } else {
            self.show_legacy_column_mode(db, plotter, dataset_name, series_id).await
        }
    }
    
    /// Extract dataset ID from series ID (assuming format "dataset:series")
    fn extract_dataset_id_from_series<'a>(&self, series_id: &'a str) -> &'a str {
        series_id.split(':').next().unwrap_or(series_id)
    }
    
    async fn show_enhanced_feature_mode(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, series_id: &str, enhanced_info: &crate::db::EnhancedDatasetColumnInfo) -> Result<()> {
        use std::io::{self, Write};
        use std::collections::HashMap;
        
        loop {
            // Clear screen and show current state
            self.clear_screen();
            println!("💿 {} > 📈 {} > 🔬 ENHANCED FEATURES\n", dataset_name, series_id);
            
            // Group features by attribute
            let mut targets = Vec::new();
            let mut historical = Vec::new();
            let mut static_vars = Vec::new();
            let mut future = Vec::new();
            
            for feature in &enhanced_info.features {
                match feature.attribute {
                    crate::db::FeatureAttribute::Targets => targets.push(feature),
                    crate::db::FeatureAttribute::HistoricalCovariates => historical.push(feature),
                    crate::db::FeatureAttribute::StaticCovariates => static_vars.push(feature),
                    crate::db::FeatureAttribute::FutureCovariates => future.push(feature),
                }
            }
            
            // Enhanced feature summary
            println!("🔬 FEATURE ANALYSIS:");
            println!("  • 🎯 Targets: {} features", targets.len());
            println!("  • 📈 Historical Covariates: {} features", historical.len());
            println!("  • 💎 Static Covariates: {} features", static_vars.len());
            println!("  • 🔮 Future Covariates: {} features", future.len());
            
            // Show timestamp information
            if let Some(timestamp_info) = &enhanced_info.timestamp_info {
                println!("  • ⏰ Time Dimension: {} ({})", 
                        timestamp_info.column_name, 
                        timestamp_info.data_type);
            }
            println!();
            
            let mut option_map = HashMap::new();
            let mut option_counter = 1;
            
            // Display features with enhanced information
            if !targets.is_empty() {
                println!("🎯 TARGETS:");
                for target in targets.iter() {
                    println!("  {}. {} ({} | {} | {})", 
                            option_counter, 
                            target.name, 
                            target.data_type,
                            match target.temporality {
                                crate::db::Temporality::Static => "Static",
                                crate::db::Temporality::Dynamic => "Dynamic",
                            },
                            match target.modality {
                                crate::db::Modality::Categorical => "Categorical",
                                crate::db::Modality::Numerical => "Numerical",
                            });
                    option_map.insert(option_counter, target);
                    option_counter += 1;
                }
                println!();
            }
            
            if !historical.is_empty() || !static_vars.is_empty() || !future.is_empty() {
                println!("📊 COVARIATES:");
                
                if !historical.is_empty() {
                    println!("  📈 Historical:");
                    for hist in historical.iter() {
                        println!("    {}. {} ({} | {} | {})", 
                                option_counter, 
                                hist.name, 
                                hist.data_type,
                                match hist.temporality {
                                    crate::db::Temporality::Static => "Static",
                                    crate::db::Temporality::Dynamic => "Dynamic",
                                },
                                match hist.modality {
                                    crate::db::Modality::Categorical => "Categorical",
                                    crate::db::Modality::Numerical => "Numerical",
                                });
                        option_map.insert(option_counter, hist);
                        option_counter += 1;
                    }
                }
                
                if !static_vars.is_empty() {
                    println!("  💎 Static:");
                    for static_var in static_vars.iter() {
                        println!("    {}. {} ({} | {} | {})", 
                                option_counter, 
                                static_var.name, 
                                static_var.data_type,
                                match static_var.temporality {
                                    crate::db::Temporality::Static => "Static",
                                    crate::db::Temporality::Dynamic => "Dynamic",
                                },
                                match static_var.modality {
                                    crate::db::Modality::Categorical => "Categorical",
                                    crate::db::Modality::Numerical => "Numerical",
                                });
                        option_map.insert(option_counter, static_var);
                        option_counter += 1;
                    }
                }
                
                if !future.is_empty() {
                    println!("  🔮 Future:");
                    for future_var in future.iter() {
                        println!("    {}. {} ({} | {} | {})", 
                                option_counter, 
                                future_var.name, 
                                future_var.data_type,
                                match future_var.temporality {
                                    crate::db::Temporality::Static => "Static",
                                    crate::db::Temporality::Dynamic => "Dynamic",
                                },
                                match future_var.modality {
                                    crate::db::Modality::Categorical => "Categorical",
                                    crate::db::Modality::Numerical => "Numerical",
                                });
                        option_map.insert(option_counter, future_var);
                        option_counter += 1;
                    }
                }
                println!();
            }
            
            println!("⏸️  Commands: /metadata <num>, /validate, /export, h/help, b/back, q/quit");
            print!("\n🔍 Select feature number or command: ");
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            
            // Handle special commands
            if input.starts_with("/metadata ") {
                // self.handle_feature_metadata_display(input, &option_map, plotter).await?;
                println!("Feature metadata display temporarily disabled");
                continue;
            } else if input == "/validate" {
                // self.validate_all_features(&enhanced_info.features);
                println!("Feature validation temporarily disabled");
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "/export" {
                // self.export_enhanced_feature_data(enhanced_info, series_id).await?;
                println!("Export temporarily disabled");
                continue;
            } else if input == "h" || input == "help" || input == "/help" {
                // self.show_enhanced_feature_help();
                println!("Help display temporarily disabled");
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                continue;
            } else if input == "b" || input == "back" {
                return Ok(());
            } else if input == "q" || input == "quit" {
                std::process::exit(0);
            }
            
            // Handle feature selection
            if let Ok(feature_choice) = input.parse::<usize>() {
                if let Some(feature) = option_map.get(&feature_choice) {
                    // self.display_enhanced_feature_data(feature, db, plotter, series_id).await?;
                    println!("Feature data display temporarily disabled");
                } else {
                    println!("Invalid feature number. Press Enter to continue...");
                    let mut _dummy = String::new();
                    io::stdin().read_line(&mut _dummy)?;
                }
            } else {
                println!("Invalid input '{}'. Type 'h' for help. Press Enter to continue...", input);
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
            }
        }
    }
    
    async fn show_legacy_column_mode(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, series_id: &str) -> Result<()> {
        use std::io::{self, Write};
        use std::collections::HashMap;
        
        loop {
            // Clear screen and show current state
            self.clear_screen();
            println!("📂 {} > 📊 {} > 📋 COLUMNS (Legacy Mode)\n", dataset_name, series_id);
            
            // Get column metadata for this series
            let series_with_columns = db.get_series_with_column_metadata(series_id)?;
            
            if let Some(series_info) = series_with_columns {
                if let Some(column_info) = series_info.column_info {
                    // Group columns by type hierarchically
                    let mut targets = Vec::new();
                    let mut historical = Vec::new();
                    let mut static_vars = Vec::new();
                    let mut future = Vec::new();
                    let mut timestamps = Vec::new();
                    
                    for column in &column_info.columns {
                        match column.column_type {
                            ColumnType::Target => targets.push(column),
                            ColumnType::HistoricalCovariate => historical.push(column),
                            ColumnType::StaticCovariate => static_vars.push(column),
                            ColumnType::FutureCovariate => future.push(column),
                            ColumnType::Timestamp => timestamps.push(column),
                        }
                    }
                    
                    // Display hierarchical column structure with enhanced features
                    println!("📊 COLUMN SUMMARY:");
                    println!("  • 🎯 Targets: {}", targets.len());
                    println!("  • 📈 Historical: {}", historical.len());
                    println!("  • 💎 Static: {}", static_vars.len());
                    println!("  • 🔮 Future: {}", future.len());
                    if !timestamps.is_empty() {
                        println!("  • ⏰ Timestamps: {} (time dimension - not a covariate)", timestamps.len());
                    }
                    println!();
                    
                    let mut option_map = HashMap::new();
                    let mut option_counter = 1;
                    
                    // Targets
                    if !targets.is_empty() {
                        println!("🎯 TARGETS:");
                        for target in &targets {
                            println!("  {}. {} ({})", option_counter, target.name, target.data_type);
                            option_map.insert(option_counter, (target, &target.column_type));
                            option_counter += 1;
                        }
                        println!();
                    }
                    
                    // Covariates
                    if !historical.is_empty() || !static_vars.is_empty() || !future.is_empty() {
                        println!("📊 COVARIATES:");
                        
                        if !historical.is_empty() {
                            println!("  📈 Historical:");
                            for hist in &historical {
                                println!("    {}. {} ({})", option_counter, hist.name, hist.data_type);
                                option_map.insert(option_counter, (hist, &hist.column_type));
                                option_counter += 1;
                            }
                        }
                        
                        if !static_vars.is_empty() {
                            println!("  💎 Static:");
                            for static_var in &static_vars {
                                println!("    {}. {} ({})", option_counter, static_var.name, static_var.data_type);
                                option_map.insert(option_counter, (static_var, &static_var.column_type));
                                option_counter += 1;
                            }
                        }
                        
                        if !future.is_empty() {
                            println!("  🔮 Future:");
                            for future_var in &future {
                                println!("    {}. {} ({})", option_counter, future_var.name, future_var.data_type);
                                option_map.insert(option_counter, (future_var, &future_var.column_type));
                                option_counter += 1;
                            }
                        }
                        println!();
                    }
                    
                    // Show timestamp columns (informational only - not selectable for forecasting)
                    if !timestamps.is_empty() {
                        println!("⏰ TIME DIMENSIONS (informational only):");
                        for timestamp in &timestamps {
                            println!("    • {} ({}) - Time index for series", timestamp.name, timestamp.data_type);
                        }
                        println!();
                    }
                    
                    println!("Commands: /compare <num1> <num2>, /plot_all, /export, /stats, h/help, b/back, q/quit");
                    print!("\n> ");
                    io::stdout().flush()?;
                    
                    let mut input = String::new();
                    io::stdin().read_line(&mut input)?;
                    let input = input.trim();
                    
                    // Handle special commands
                    if input.starts_with("/compare ") {
                        self.handle_column_comparison(&input, &option_map, db, plotter, series_id).await?;
                        continue;
                    } else if input == "/plot_all" {
                        self.plot_all_columns(&option_map, db, plotter, series_id).await?;
                        continue;
                    } else if input == "/export" {
                        self.export_column_data(&option_map, db, series_id).await?;
                        continue;
                    } else if input == "/stats" {
                        self.show_column_stats(&column_info.columns);
                        println!("\nPress Enter to continue...");
                        let mut _dummy = String::new();
                        io::stdin().read_line(&mut _dummy)?;
                        continue;
                    } else if input == "h" || input == "help" || input == "/help" {
                        self.show_column_help();
                        println!("\nPress Enter to continue...");
                        let mut _dummy = String::new();
                        io::stdin().read_line(&mut _dummy)?;
                        continue;
                    } else if input == "b" || input == "back" {
                        return Ok(());
                    } else if input == "q" || input == "quit" {
                        std::process::exit(0);
                    }
                    
                    // Handle column selection
                    if let Ok(column_choice) = input.parse::<usize>() {
                        if let Some((column, column_type)) = option_map.get(&column_choice) {
                            self.display_column_data(column, column_type, db, plotter, series_id).await?;
                        } else {
                            println!("Invalid choice. Please enter a number between 1 and {}", option_counter - 1);
                            std::thread::sleep(std::time::Duration::from_millis(2000));
                        }
                    } else {
                        println!("Invalid input. Enter a column number, command, or 'b'/'q'.");
                        std::thread::sleep(std::time::Duration::from_millis(2000));
                    }
                } else {
                    println!("❌ No column metadata available for this series.");
                    println!("Press Enter to go back...");
                    let mut _dummy = String::new();
                    io::stdin().read_line(&mut _dummy)?;
                    return Ok(());
                }
            } else {
                println!("❌ Series not found in database.");
                println!("Press Enter to go back...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
                return Ok(());
            }
        }
    }
    
    async fn handle_column_comparison(&self, input: &str, option_map: &std::collections::HashMap<usize, (&&crate::db::ColumnMetadata, &ColumnType)>, db: &Database, _plotter: &TimeSeriesPlotter, series_id: &str) -> Result<()> {
        let parts: Vec<&str> = input.strip_prefix("/compare ").unwrap_or("").split_whitespace().collect();
        if parts.len() != 2 {
            println!("Usage: /compare <num1> <num2>");
            std::thread::sleep(std::time::Duration::from_millis(2000));
            return Ok(());
        }
        
        if let (Ok(col1), Ok(col2)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>()) {
            if let (Some((column1, _)), Some((column2, _))) = (option_map.get(&col1), option_map.get(&col2)) {
                println!("\n📊 COMPARING COLUMNS: {} vs {}", column1.name, column2.name);
                println!("──────────────────────────────────────");
                
                // Load data for both columns
                match (db.get_time_series_data(series_id, &column1.name, Some(1000)), 
                       db.get_time_series_data(series_id, &column2.name, Some(1000))) {
                    (Ok(data1), Ok(data2)) => {
                        if !data1.is_empty() && !data2.is_empty() {
                            // Show basic comparison stats
                            let avg1 = data1.iter().map(|p| p.value).sum::<f64>() / data1.len() as f64;
                            let avg2 = data2.iter().map(|p| p.value).sum::<f64>() / data2.len() as f64;
                            
                            println!("📈 {} → Records: {}, Average: {:.2}", column1.name, data1.len(), avg1);
                            println!("📈 {} → Records: {}, Average: {:.2}", column2.name, data2.len(), avg2);
                            
                            // Simple correlation (if same length)
                            if data1.len() == data2.len() {
                                let correlation = self.calculate_correlation(&data1, &data2);
                                println!("🔗 Correlation: {:.3}", correlation);
                            }
                            
                            println!("\nPress Enter to continue...");
                            let mut _dummy = String::new();
                            io::stdin().read_line(&mut _dummy)?;
                        } else {
                            println!("❌ No data available for comparison");
                            std::thread::sleep(std::time::Duration::from_millis(2000));
                        }
                    }
                    _ => {
                        println!("❌ Error loading data for comparison");
                        std::thread::sleep(std::time::Duration::from_millis(2000));
                    }
                }
            } else {
                println!("❌ Invalid column numbers");
                std::thread::sleep(std::time::Duration::from_millis(2000));
            }
        } else {
            println!("❌ Invalid column numbers");
            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
        
        Ok(())
    }
    
    fn calculate_correlation(&self, data1: &[crate::db::TimeSeriesPoint], data2: &[crate::db::TimeSeriesPoint]) -> f64 {
        if data1.len() != data2.len() || data1.is_empty() {
            return 0.0;
        }
        
        let n = data1.len() as f64;
        let sum1: f64 = data1.iter().map(|p| p.value).sum();
        let sum2: f64 = data2.iter().map(|p| p.value).sum();
        let mean1 = sum1 / n;
        let mean2 = sum2 / n;
        
        let mut numerator = 0.0;
        let mut sum_sq1 = 0.0;
        let mut sum_sq2 = 0.0;
        
        for (p1, p2) in data1.iter().zip(data2.iter()) {
            let diff1 = p1.value - mean1;
            let diff2 = p2.value - mean2;
            numerator += diff1 * diff2;
            sum_sq1 += diff1 * diff1;
            sum_sq2 += diff2 * diff2;
        }
        
        let denominator = (sum_sq1 * sum_sq2).sqrt();
        if denominator == 0.0 {
            0.0
        } else {
            numerator / denominator
        }
    }
    
    async fn plot_all_columns(&self, option_map: &std::collections::HashMap<usize, (&&crate::db::ColumnMetadata, &ColumnType)>, db: &Database, plotter: &TimeSeriesPlotter, series_id: &str) -> Result<()> {
        println!("\n📊 PLOTTING ALL TIME-SERIES COLUMNS...");
        
        for (_, (column, column_type)) in option_map {
            if matches!(column_type, ColumnType::Target | ColumnType::HistoricalCovariate | ColumnType::FutureCovariate) {
                println!("\n📈 {}", column.name);
                match db.get_time_series_data(series_id, &column.name, Some(200)) {
                    Ok(data_points) => {
                        if !data_points.is_empty() {
                            match plotter.plot_time_series(&data_points, &column.name, Some(60), Some(8)) {
                                Ok(plot) => println!("{}", plot),
                                Err(e) => println!("❌ Error plotting {}: {}", column.name, e),
                            }
                        } else {
                            println!("❌ No data for {}", column.name);
                        }
                    }
                    Err(e) => println!("❌ Error loading {}: {}", column.name, e),
                }
            }
        }
        
        println!("\nPress Enter to continue...");
        let mut _dummy = String::new();
        io::stdin().read_line(&mut _dummy)?;
        
        Ok(())
    }
    
    async fn export_column_data(&self, _option_map: &std::collections::HashMap<usize, (&&crate::db::ColumnMetadata, &ColumnType)>, _db: &Database, series_id: &str) -> Result<()> {
        println!("\n📤 EXPORT OPTIONS:");
        println!("  1. Export column metadata as JSON");
        println!("  2. Export time series data as CSV"); 
        println!("  3. Export statistics summary");
        
        print!("\nSelect export option (1-3): ");
        io::stdout().flush()?;
        
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        
        match input.trim().parse::<usize>() {
            Ok(1) => {
                let filename = format!("{}_metadata.json", series_id.replace(":", "_"));
                println!("✅ Would export column metadata to {}", filename);
                // TODO: Implement actual JSON export
            }
            Ok(2) => {
                let filename = format!("{}_data.csv", series_id.replace(":", "_"));
                println!("✅ Would export time series data to {}", filename);
                // TODO: Implement actual CSV export
            }
            Ok(3) => {
                let filename = format!("{}_stats.txt", series_id.replace(":", "_"));
                println!("✅ Would export statistics to {}", filename);
                // TODO: Implement actual stats export
            }
            _ => println!("❌ Invalid export option"),
        }
        
        std::thread::sleep(std::time::Duration::from_millis(2000));
        Ok(())
    }
    
    fn show_column_stats(&self, columns: &[crate::db::ColumnMetadata]) {
        let mut type_counts = std::collections::HashMap::new();
        let mut data_types = std::collections::HashMap::new();
        
        for col in columns {
            *type_counts.entry(&col.column_type).or_insert(0) += 1;
            *data_types.entry(&col.data_type).or_insert(0) += 1;
        }
        
        println!("\n📊 COLUMN STATISTICS:");
        println!("  • Total Columns: {}", columns.len());
        
        println!("\n📋 BY COLUMN TYPE:");
        for (col_type, count) in type_counts {
            let icon = match col_type {
                ColumnType::Target => "🎯",
                ColumnType::HistoricalCovariate => "📈",
                ColumnType::StaticCovariate => "💎",
                ColumnType::FutureCovariate => "🔮",
                ColumnType::Timestamp => "⏰",
            };
            println!("  • {} {}: {}", icon, format!("{:?}", col_type), count);
        }
        
        println!("\n🔧 BY DATA TYPE:");
        for (data_type, count) in data_types {
            println!("  • {}: {}", data_type, count);
        }
    }
    
    fn show_column_help(&self) {
        println!("\n📖 COLUMN LEVEL COMMANDS:");
        println!("  • Enter number: View/plot individual column");
        println!("  • /compare <num1> <num2>: Compare two columns");
        println!("  • /plot_all: Plot all time-series columns");
        println!("  • /export: Export column data/metadata");
        println!("  • /stats: Show column statistics");
        println!("  • h/help: Show this help");
        println!("  • b/back: Go back to series list");
        println!("  • q/quit: Exit program");
    }
    
    async fn display_column_data(&self, column: &crate::db::ColumnMetadata, column_type: &ColumnType, db: &Database, plotter: &TimeSeriesPlotter, series_id: &str) -> Result<()> {
        println!("\n{}", "=".repeat(70));
        println!("📊 COLUMN ANALYSIS: {}", column.name);
        println!("{}", "=".repeat(70));
        
        // Show column metadata
        println!("🔧 Column Type: {:?}", column_type);
        println!("📊 Data Type: {}", column.data_type);
        if let Some(desc) = &column.description {
            println!("📝 Description: {}", desc);
        }
        println!("🆔 Series: {}", series_id);
        
        // Show column data based on type
        match column_type {
            ColumnType::Timestamp => {
                println!("\n⏰ TIMESTAMP DIMENSION ANALYSIS");
                println!("{}", "─".repeat(40));
                println!("This column represents the time dimension for the series.");
                println!("Timestamps are indices, not predictive features.\n");
                
                println!("ℹ️  Note: Timestamp columns are used as time indices and are not");
                println!("   treated as covariates in forecasting models.");
            }
            
            ColumnType::StaticCovariate => {
                println!("\n💎 STATIC COVARIATE ANALYSIS");
                println!("{}", "─".repeat(40));
                println!("This column contains static information that doesn't vary over time.");
                println!("Examples: category, region, item_type, store_id, etc.\n");
                
                // Try to get a sample value from the time series data
                match db.get_time_series_data(series_id, &column.name, Some(1)) {
                    Ok(data) => {
                        if let Some(point) = data.first() {
                            println!("💎 Static Value: {}", point.value);
                            println!("📅 Recorded at: {}", point.timestamp.format("%Y-%m-%d %H:%M:%S"));
                        } else {
                            println!("⚠️  No data available for this static column");
                        }
                    }
                    Err(e) => {
                        println!("❌ Error retrieving static value: {}", e);
                        println!("💡 This column may exist in metadata but not in actual data");
                    }
                }
            }
            
            ColumnType::Target | ColumnType::HistoricalCovariate | ColumnType::FutureCovariate => {
                let type_emoji = match column_type {
                    ColumnType::Target => "🎯",
                    ColumnType::HistoricalCovariate => "📈",
                    ColumnType::FutureCovariate => "🔮",
                    ColumnType::Timestamp => "⏰",
                    _ => "📊",
                };
                
                println!("\n{} TIME SERIES ANALYSIS", type_emoji);
                println!("{}", "─".repeat(40));
                println!("📊 Loading time series data for '{}'...", column.name);
                
                match db.get_time_series_data(series_id, &column.name, Some(1000)) {
                    Ok(data_points) => {
                        if data_points.is_empty() {
                            println!("\n❌ NO DATA FOUND");
                            println!("This could mean:");
                            println!("  • Column exists in metadata but has no actual data");
                            println!("  • Data hasn't been fully indexed yet");
                            println!("  • Column name mismatch between metadata and data");
                        } else {
                            // Calculate comprehensive statistics
                            let values: Vec<f64> = data_points.iter().map(|p| p.value).collect();
                            let n = values.len() as f64;
                            let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                            let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                            let sum = values.iter().sum::<f64>();
                            let mean = sum / n;
                            
                            // Calculate standard deviation and other stats
                            let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;
                            let std_dev = variance.sqrt();
                            
                            // Calculate percentiles (approximate)
                            let mut sorted_values = values.clone();
                            sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
                            let median = sorted_values[sorted_values.len() / 2];
                            let q25 = sorted_values[sorted_values.len() / 4];
                            let q75 = sorted_values[3 * sorted_values.len() / 4];
                            
                            println!("\n📊 STATISTICAL SUMMARY");
                            println!("{}", "─".repeat(30));
                            println!("📈 Data Points: {}", data_points.len());
                            println!("📅 Time Range: {} to {}", 
                                   data_points.first().unwrap().timestamp.format("%Y-%m-%d %H:%M"),
                                   data_points.last().unwrap().timestamp.format("%Y-%m-%d %H:%M"));
                            println!("📊 Min Value: {:.6}", min_val);
                            println!("📊 25th Percentile: {:.6}", q25);
                            println!("📊 Median: {:.6}", median);
                            println!("📊 Mean: {:.6}", mean);
                            println!("📊 75th Percentile: {:.6}", q75);
                            println!("📊 Max Value: {:.6}", max_val);
                            println!("📊 Std Deviation: {:.6}", std_dev);
                            println!("📊 Coefficient of Variation: {:.2}%", (std_dev / mean.abs()) * 100.0);
                            
                            // Trend analysis
                            if data_points.len() > 1 {
                                let first_val = data_points.first().unwrap().value;
                                let last_val = data_points.last().unwrap().value;
                                let total_change = last_val - first_val;
                                let percent_change = if first_val != 0.0 { (total_change / first_val) * 100.0 } else { 0.0 };
                                
                                println!("\n📈 TREND ANALYSIS");
                                println!("{}", "─".repeat(30));
                                println!("📊 Total Change: {:.6}", total_change);
                                println!("📊 Percent Change: {:.2}%", percent_change);
                                
                                if percent_change.abs() < 1.0 {
                                    println!("📈 Trend: ➡️  Relatively stable");
                                } else if percent_change > 0.0 {
                                    println!("📈 Trend: ⬆️  Upward ({:.2}%)", percent_change);
                                } else {
                                    println!("📈 Trend: ⬇️  Downward ({:.2}%)", percent_change);
                                }
                            }
                            
                            // Generate and display the ASCII plot
                            println!("\n📊 TIME SERIES VISUALIZATION");
                            println!("{}", "─".repeat(30));
                            
                            // Get optimal dimensions for plotting  
                            let (width, height) = plotter.get_optimal_dimensions();
                            
                            match plotter.plot_time_series(&data_points, &column.name, Some(width), Some(height)) {
                                Ok(plot) => {
                                    println!("{}", plot);
                                }
                                Err(e) => {
                                    println!("❌ Error creating plot: {}", e);
                                }
                            }
                            
                            // Data quality indicators
                            let zero_count = values.iter().filter(|&&x| x == 0.0).count();
                            let negative_count = values.iter().filter(|&&x| x < 0.0).count();
                            
                            if zero_count > 0 || negative_count > 0 {
                                println!("\n🔍 DATA QUALITY NOTES");
                                println!("{}", "─".repeat(30));
                                if zero_count > 0 {
                                    println!("⚠️  Zero values: {} ({:.1}%)", zero_count, (zero_count as f64 / n) * 100.0);
                                }
                                if negative_count > 0 {
                                    println!("⚠️  Negative values: {} ({:.1}%)", negative_count, (negative_count as f64 / n) * 100.0);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("\n❌ ERROR LOADING DATA");
                        println!("Error: {}", e);
                        println!("\nPossible causes:");
                        println!("  • Database connection issues");
                        println!("  • Column doesn't exist in actual data files");
                        println!("  • Data corruption or indexing problems");
                        println!("  • Series ID mismatch");
                    }
                }
            }
        }
        
        println!("\n{}", "=".repeat(70));
        println!("Press Enter to continue...");
        let mut _dummy = String::new();
        std::io::stdin().read_line(&mut _dummy)?;
        
        Ok(())
    }
    
    async fn show_column_level(&self, db: &Database, plotter: &TimeSeriesPlotter, dataset_name: &str, series_id: &str) -> Result<()> {
        use std::io::{self, Write};
        use std::collections::HashMap;
        
        println!("\n📂 {} > 📊 {} > 📋 COLUMNS", dataset_name, series_id);
        
        // Get column metadata for this series
        let series_with_columns = db.get_series_with_column_metadata(series_id)?;
        
        if let Some(series_info) = series_with_columns {
            if let Some(column_info) = series_info.column_info {
                // Group columns by type hierarchically
                let mut targets = Vec::new();
                let mut historical = Vec::new();
                let mut static_vars = Vec::new();
                let mut future = Vec::new();
                
                for column in &column_info.columns {
                    match column.column_type {
                        ColumnType::Target => targets.push(column),
                        ColumnType::HistoricalCovariate => historical.push(column),
                        ColumnType::StaticCovariate => static_vars.push(column),
                        ColumnType::FutureCovariate => future.push(column),
                        ColumnType::Timestamp => {}, // Skip timestamps in legacy mode
                    }
                }
                
                // Display hierarchical column structure
                println!("Select column to plot/view, 'b' to go back, or 'q' to quit\n");
                
                let mut option_map = HashMap::new();
                let mut option_counter = 1;
                
                // Targets
                if !targets.is_empty() {
                    println!("🎯 TARGETS:");
                    for target in &targets {
                        println!("  {}. {} ({})", option_counter, target.name, target.data_type);
                        option_map.insert(option_counter, (target, &target.column_type));
                        option_counter += 1;
                    }
                    println!();
                }
                
                // Covariates
                if !historical.is_empty() || !static_vars.is_empty() || !future.is_empty() {
                    println!("📊 COVARIATES:");
                    
                    if !historical.is_empty() {
                        println!("  📈 Historical:");
                        for hist in &historical {
                            println!("    {}. {} ({})", option_counter, hist.name, hist.data_type);
                            option_map.insert(option_counter, (hist, &hist.column_type));
                            option_counter += 1;
                        }
                    }
                    
                    if !static_vars.is_empty() {
                        println!("  💎 Static:");
                        for static_var in &static_vars {
                            println!("    {}. {} ({})", option_counter, static_var.name, static_var.data_type);
                            option_map.insert(option_counter, (static_var, &static_var.column_type));
                            option_counter += 1;
                        }
                    }
                    
                    if !future.is_empty() {
                        println!("  🔮 Future:");
                        for future_var in &future {
                            println!("    {}. {} ({})", option_counter, future_var.name, future_var.data_type);
                            option_map.insert(option_counter, (future_var, &future_var.column_type));
                            option_counter += 1;
                        }
                    }
                    println!();
                }
                
                // Handle user input for column selection
                loop {
                    print!("\n> ");
                    io::stdout().flush()?;
                    
                    let mut column_input = String::new();
                    io::stdin().read_line(&mut column_input)?;
                    let column_input = column_input.trim();
                    
                    if column_input == "q" || column_input == "quit" {
                        std::process::exit(0);
                    } else if column_input == "b" || column_input == "back" {
                        return Ok(());
                    }
                    
                    if let Ok(column_choice) = column_input.parse::<usize>() {
                        if let Some((column, column_type)) = option_map.get(&column_choice) {
                            // Show column data based on type
                            match column_type {
                                ColumnType::Timestamp => {
                                    // For timestamp columns, show informational display
                                    println!("\n⏰ TIMESTAMP DIMENSION: {}", column.name);
                                    println!("Timestamps are time indices, not features for forecasting.");
                                    println!("\nPress Enter to continue...");
                                    let mut _dummy = String::new();
                                    io::stdin().read_line(&mut _dummy)?;
                                }
                                ColumnType::StaticCovariate => {
                                    // For static columns, just show the value (no time series)
                                    let display = plotter.show_static_value(&column.name, "Static value (TODO: get actual value from DB)");
                                    println!("\n{}", display);
                                    println!("\nPress Enter to continue...");
                                    let mut _dummy = String::new();
                                    io::stdin().read_line(&mut _dummy)?;
                                }
                                ColumnType::Target | ColumnType::HistoricalCovariate | ColumnType::FutureCovariate => {
                                    // For time series columns, get data and plot
                                    println!("\n📊 Loading time series data for '{}'...", column.name);
                                    
                                    match db.get_time_series_data(series_id, &column.name, Some(1000)) {
                                        Ok(data_points) => {
                                            if data_points.is_empty() {
                                                println!("❌ No data found for column '{}'", column.name);
                                            } else {
                                                // Get optimal dimensions for plotting
                                                let (width, height) = plotter.get_optimal_dimensions();
                                                
                                                match plotter.plot_time_series(&data_points, &column.name, Some(width), Some(height)) {
                                                    Ok(plot) => {
                                                        println!("\n{}", plot);
                                                        println!("\nPress Enter to continue...");
                                                        let mut _dummy = String::new();
                                                        io::stdin().read_line(&mut _dummy)?;
                                                    }
                                                    Err(e) => {
                                                        println!("❌ Error creating plot: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            println!("❌ Error loading data: {}", e);
                                        }
                                    }
                                }
                            }
                        } else {
                            println!("Invalid choice. Please enter a number between 1 and {}", option_counter - 1);
                        }
                    } else {
                        println!("Invalid input. Enter a column number, 'b' to go back, or 'q' to quit.");
                    }
                }
            } else {
                println!("❌ No column metadata found for this series.");
                println!("This might indicate the dataset hasn't been fully indexed yet.");
                println!("\nPress Enter to continue...");
                let mut _dummy = String::new();
                io::stdin().read_line(&mut _dummy)?;
            }
        } else {
            println!("❌ Series '{}' not found in database.", series_id);
            println!("\nPress Enter to continue...");
            let mut _dummy = String::new();
            io::stdin().read_line(&mut _dummy)?;
        }
        
        Ok(())
    }
    
    async fn run_traditional_search(&self, search_engine: SearchEngine) -> Result<()> {
        // Perform search
        let search_query = create_search_query(self.query.as_deref().unwrap_or("").to_string(), Some(self.limit));
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
pub struct AnalyzeSizesCommand {
    /// S3 bucket name
    #[arg(short, long)]
    pub bucket: String,
    
    /// S3 prefix for data files
    #[arg(short, long, default_value = "lotsa_long_format/")]
    pub prefix: String,
    
    /// AWS region
    #[arg(short, long, default_value = "eu-west-3")]
    pub region: String,
    
    /// Maximum files to analyze (for quick testing)
    #[arg(long)]
    pub max_files: Option<usize>,
    
    /// Show detailed file list (top largest files)
    #[arg(long, help = "Show detailed list of largest files")]
    pub detailed: bool,
}

impl AnalyzeSizesCommand {
    pub async fn execute(self) -> Result<()> {
        info!("🔍 Analyzing file sizes in S3 bucket: {}", self.bucket);
        info!("📁 Prefix: {}", self.prefix);
        info!("🌍 Region: {}", self.region);
        
        if let Some(max) = self.max_files {
            info!("🔢 Max files to analyze: {} (testing mode)", max);
        }
        
        // Initialize S3 client
        let s3_client = crate::s3::S3Client::new(self.bucket.clone(), self.region.clone()).await?;
        
        // List all objects in the prefix
        info!("📋 Scanning S3 objects...");
        let objects = s3_client.list_objects(&self.prefix, self.max_files).await?;
        
        if objects.is_empty() {
            println!("❌ No objects found in bucket {} with prefix {}", self.bucket, self.prefix);
            return Ok(());
        }
        
        // Filter to data files only
        let data_files = s3_client.filter_data_files(&objects);
        
        if data_files.is_empty() {
            println!("❌ No data files found (looking for .parquet, .csv, .csv.gz files)");
            return Ok(());
        }
        
        info!("Found {} data files out of {} total objects", data_files.len(), objects.len());
        
        // Collect file sizes
        let mut sizes: Vec<i64> = data_files.iter().map(|f| f.size).collect();
        sizes.sort();
        
        // Calculate statistics
        let total_files = sizes.len();
        let total_size_bytes: i64 = sizes.iter().sum();
        let min_size = *sizes.first().unwrap_or(&0);
        let max_size = *sizes.last().unwrap_or(&0);
        let median_size = if total_files > 0 {
            sizes[total_files / 2]
        } else {
            0
        };
        let avg_size = if total_files > 0 {
            total_size_bytes / total_files as i64
        } else {
            0
        };
        
        // Percentiles
        let p95_size = if total_files > 0 {
            sizes[(total_files as f64 * 0.95) as usize]
        } else {
            0
        };
        let p99_size = if total_files > 0 {
            sizes[(total_files as f64 * 0.99) as usize]
        } else {
            0
        };
        
        // Display results
        println!("\n📊 File Size Analysis Results");
        println!("{:-<60}", "");
        println!("📂 Total Files: {}", total_files);
        println!("💾 Total Size: {}", format_bytes(total_size_bytes));
        println!("📈 Average Size: {}", format_bytes(avg_size));
        println!("📊 Median Size: {}", format_bytes(median_size));
        println!("🔽 Min Size: {}", format_bytes(min_size));
        println!("🔼 Max Size: {}", format_bytes(max_size));
        println!("📈 95th Percentile: {}", format_bytes(p95_size));
        println!("🚀 99th Percentile: {}", format_bytes(p99_size));
        
        // Size distribution histogram
        println!("\n📊 Size Distribution:");
        self.print_size_histogram(&sizes);
        
        // Show largest files if detailed mode
        if self.detailed {
            println!("\n🔝 Top 20 Largest Files:");
            println!("{:-<80}", "");
            let mut files_with_sizes: Vec<_> = data_files.iter().collect();
            files_with_sizes.sort_by(|a, b| b.size.cmp(&a.size));
            
            for (i, file) in files_with_sizes.iter().take(20).enumerate() {
                println!("{}. {} - {}", 
                        i + 1, 
                        format_bytes(file.size), 
                        file.key.split('/').last().unwrap_or(&file.key));
            }
        }
        
        // Performance analysis
        println!("\n⚡ Performance Impact Analysis:");
        self.analyze_performance_impact(&sizes);
        
        Ok(())
    }
    
    fn print_size_histogram(&self, sizes: &[i64]) {
        let ranges = [
            (0, 1024 * 1024),           // < 1MB
            (1024 * 1024, 10 * 1024 * 1024),     // 1MB - 10MB
            (10 * 1024 * 1024, 100 * 1024 * 1024),   // 10MB - 100MB
            (100 * 1024 * 1024, 1024 * 1024 * 1024), // 100MB - 1GB
            (1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024), // 1GB - 10GB
            (10 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024), // 10GB - 100GB
        ];
        
        let range_names = [
            "< 1MB",
            "1MB - 10MB", 
            "10MB - 100MB",
            "100MB - 1GB",
            "1GB - 10GB",
            "10GB - 100GB",
        ];
        
        for (i, (min_size, max_size)) in ranges.iter().enumerate() {
            let count = sizes.iter().filter(|&&size| size >= *min_size && size < *max_size).count();
            let percentage = (count as f64 / sizes.len() as f64) * 100.0;
            let bar_length = (percentage / 2.0) as usize; // Scale for display
            let bar = "█".repeat(bar_length);
            
            println!("  {:12} │{:40}│ {:5} files ({:5.1}%)", 
                    range_names[i], 
                    format!("{:<40}", bar),
                    count,
                    percentage);
        }
        
        // Handle files larger than 100GB
        let huge_files = sizes.iter().filter(|&&size| size >= 100 * 1024 * 1024 * 1024).count();
        if huge_files > 0 {
            let percentage = (huge_files as f64 / sizes.len() as f64) * 100.0;
            let bar_length = (percentage / 2.0) as usize;
            let bar = "█".repeat(bar_length);
            println!("  {:12} │{:40}│ {:5} files ({:5.1}%)", 
                    "> 100GB", 
                    format!("{:<40}", bar),
                    huge_files,
                    percentage);
        }
    }
    
    fn analyze_performance_impact(&self, sizes: &[i64]) {
        let gb = 1024 * 1024 * 1024;
        let large_files = sizes.iter().filter(|&&size| size > gb).count(); // > 1GB
        let huge_files = sizes.iter().filter(|&&size| size > 10 * gb).count(); // > 10GB
        let massive_files = sizes.iter().filter(|&&size| size > 100 * gb).count(); // > 100GB
        
        println!("{:-<60}", "");
        println!("🚨 Files requiring special handling:");
        println!("  > 1GB:   {} files ({:.1}%)", large_files, (large_files as f64 / sizes.len() as f64) * 100.0);
        println!("  > 10GB:  {} files ({:.1}%)", huge_files, (huge_files as f64 / sizes.len() as f64) * 100.0);
        println!("  > 100GB: {} files ({:.1}%)", massive_files, (massive_files as f64 / sizes.len() as f64) * 100.0);
        
        if massive_files > 0 {
            println!("\n⚠️  CRITICAL: {} files exceed 100GB - will cause memory issues with current implementation", massive_files);
            println!("   Recommendation: Implement selective column reading immediately");
        } else if huge_files > 0 {
            println!("\n⚠️  WARNING: {} files exceed 10GB - may cause memory pressure", huge_files);
            println!("   Recommendation: Consider selective column reading optimization");
        } else if large_files > 0 {
            println!("\n💡 INFO: {} files exceed 1GB - monitor memory usage during processing", large_files);
        } else {
            println!("\n✅ All files are < 1GB - current implementation should handle well");
        }
        
        // Memory estimation
        let total_memory_if_concurrent = sizes.iter()
            .take(8) // Default concurrency
            .sum::<i64>();
        println!("\n💾 Memory usage estimate (8 concurrent workers):");
        println!("   Peak memory: ~{} (loading 8 largest files)", format_bytes(total_memory_if_concurrent));
        
        if total_memory_if_concurrent > 64 * gb {
            println!("   🚨 CRITICAL: Estimated peak memory exceeds 64GB!");
        } else if total_memory_if_concurrent > 16 * gb {
            println!("   ⚠️  WARNING: Estimated peak memory exceeds 16GB");
        } else {
            println!("   ✅ Estimated memory usage is reasonable");
        }
    }
}

/// Format bytes in human-readable format
fn format_bytes(bytes: i64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    const THRESHOLD: f64 = 1024.0;
    
    if bytes == 0 {
        return "0 B".to_string();
    }
    
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= THRESHOLD && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD;
        unit_index += 1;
    }
    
    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

#[derive(Debug, PartialEq)]
enum HierarchicalView {
    DatasetLevel,
    SeriesLevel,
}

#[derive(Debug)]
#[derive(Clone)]
struct DatasetGroup {
    dataset_name: String,
    dataset_info: Option<crate::search::EnhancedSearchResult>,
    series: Vec<crate::search::EnhancedSearchResult>,
    total_series: i64,  // Actual total series count from database
    total_records: i64,
}

impl SearchCommand {
    fn group_results_hierarchically(&self, results: &[crate::search::EnhancedSearchResult]) -> std::collections::HashMap<String, DatasetGroup> {
        use std::collections::HashMap;
        
        let mut groups: HashMap<String, DatasetGroup> = HashMap::new();
        let mut dataset_series_samples: HashMap<String, Vec<crate::search::EnhancedSearchResult>> = HashMap::new();
        
        let is_empty_query = self.query.as_deref().map_or(true, |q| q.trim().is_empty());
        
        // Group search results by dataset, keeping only real series (not fake dataset entries)
        for result in results {
            let dataset_id = &result.search_result.dataset_id;
            let series_id = &result.search_result.series_id;
            
            // Only keep real series results, skip fake dataset entries
            if series_id != dataset_id {
                dataset_series_samples.entry(dataset_id.clone())
                    .or_insert_with(Vec::new)
                    .push(result.clone());
            }
        }
        
        // Create dataset groups
        for (dataset_id, series_samples) in dataset_series_samples {
            let series_count = if is_empty_query {
                // For empty query, show total series count from database
                self.get_dataset_info(&dataset_id).map(|(total_series, _)| total_series).unwrap_or(series_samples.len() as i64)
            } else {
                // For specific queries, show count of matching series
                series_samples.len() as i64
            };
            
            let total_records = series_samples.iter().map(|s| s.search_result.record_count).sum();
            
            groups.insert(dataset_id.clone(), DatasetGroup {
                dataset_name: dataset_id.clone(),
                dataset_info: None,
                series: series_samples,
                total_series: series_count,
                total_records,
            });
        }
        
        // For datasets that appear in search results but have no real series in our samples,
        // add them using the fake dataset entry info (only for empty queries)
        if is_empty_query {
            for result in results {
                let dataset_id = &result.search_result.dataset_id;
                let series_id = &result.search_result.series_id;
                
                if series_id == dataset_id && !groups.contains_key(dataset_id) {
                    let (total_series, total_records) = self.get_dataset_info(dataset_id).unwrap_or((1, result.search_result.record_count));
                    
                    groups.insert(dataset_id.clone(), DatasetGroup {
                        dataset_name: dataset_id.clone(),
                        dataset_info: None,
                        series: vec![result.clone()],
                        total_series,
                        total_records,
                    });
                }
            }
        }
        
        groups
    }
    
    fn get_dataset_info(&self, dataset_id: &str) -> Option<(i64, i64)> {
        // Query the datasets table to get total_series and total_records
        let db = match crate::db::Database::new(&self.database) {
            Ok(db) => db,
            Err(_) => return None,
        };
        
        db.get_dataset_info(dataset_id).ok()
    }
    
    fn get_all_series_for_dataset(&self, db: &Database, dataset_id: &str) -> Result<Vec<crate::search::EnhancedSearchResult>> {
        use tracing::{info, error, debug};
        
        info!("🎯 get_all_series_for_dataset called for dataset: '{}'", dataset_id);
        
        // Query the series_metadata table directly for this specific dataset
        // This avoids the fake dataset entries in the search_index view
        let series_results = match db.get_series_for_dataset(dataset_id, None) {
            Ok(results) => {
                info!("✅ Database query successful: found {} series", results.len());
                if results.len() <= 3 {
                    for (i, result) in results.iter().enumerate() {
                        debug!("📊 Series {}: id='{}', record_count={}", i+1, result.series_id, result.record_count);
                    }
                } else {
                    debug!("📊 First 3 series:");
                    for (i, result) in results.iter().take(3).enumerate() {
                        debug!("📊 Series {}: id='{}', record_count={}", i+1, result.series_id, result.record_count);
                    }
                }
                results
            },
            Err(e) => {
                error!("❌ Database query failed for dataset '{}': {}", dataset_id, e);
                return Err(e);
            }
        };
        
        // Convert to EnhancedSearchResult
        let dataset_series: Vec<crate::search::EnhancedSearchResult> = series_results
            .into_iter()
            .map(|result| crate::search::EnhancedSearchResult {
                search_result: result,
                fuzzy_score: None,
                relevance_score: 10.0, // Default relevance for complete dataset view
                match_reasons: vec!["Dataset Member".to_string()],
            })
            .collect();
        
        info!("✅ get_all_series_for_dataset completed: returning {} enhanced series", dataset_series.len());
        Ok(dataset_series)
    }
    
    fn display_hierarchical_view(
        &self,
        hierarchical_results: &std::collections::HashMap<String, DatasetGroup>,
        current_view: &HierarchicalView,
        dataset_index: usize,
        series_index: usize,
        selected_dataset: &Option<String>,
    ) -> Result<()> {
        use std::io::{self, Write};
        
        // Header
        let query_str = self.query.as_deref().unwrap_or("");
        println!("🔍 Interactive Search: '{}'\n", query_str);
        
        match current_view {
            HierarchicalView::DatasetLevel => {
                println!("📁 DATASETS ({} found)", hierarchical_results.len());
                println!("Use ↑↓ to navigate, Enter to expand, 'q' to quit\n");
                
                let dataset_names: Vec<_> = hierarchical_results.keys().cloned().collect();
                
                for (idx, dataset_name) in dataset_names.iter().enumerate() {
                    let group = &hierarchical_results[dataset_name];
                    let prefix = if idx == dataset_index { "► " } else { "" };
                    
                    let series_count = group.total_series;  // Use actual series count from database
                    let total_records = group.total_records;
                    
                    println!("{}📂 {} ({} series, {} records)", 
                             prefix, dataset_name, series_count, total_records);
                    
                    if idx == dataset_index {
                        if let Some(dataset_info) = &group.dataset_info {
                            if let Some(theme) = &dataset_info.search_result.theme {
                                println!("     Theme: {}", theme);
                            }
                            if let Some(desc) = &dataset_info.search_result.description {
                                println!("     Description: {}", desc);
                            }
                        }
                    }
                }
            }
            HierarchicalView::SeriesLevel => {
                if let Some(dataset_name) = selected_dataset {
                    if let Some(group) = hierarchical_results.get(dataset_name) {
                        println!("📂 {} > 📊 SERIES ({} series)", dataset_name, group.series.len());
                        println!("Use ↑↓ to navigate, Esc to go back, 'q' to quit\n");
                        
                        for (idx, series) in group.series.iter().enumerate() {
                            let prefix = if idx == series_index { "► " } else { "" };
                            
                            println!("{}📊 {} ({} records)", 
                                     prefix, series.search_result.series_id, series.search_result.record_count);
                            
                            if idx == series_index {
                                println!("     Relevance: {:.2}", series.relevance_score);
                                if !series.match_reasons.is_empty() {
                                    println!("     Matched: {}", series.match_reasons.join(", "));
                                }
                                if let Some(theme) = &series.search_result.theme {
                                    println!("     Theme: {}", theme);
                                }
                                println!("     First: {}", series.search_result.first_timestamp.format("%Y-%m-%d"));
                                println!("     Last: {}", series.search_result.last_timestamp.format("%Y-%m-%d"));
                            }
                        }
                    }
                }
            }
        }
        
        // Footer
        println!("\n{}", "─".repeat(80));
        match current_view {
            HierarchicalView::DatasetLevel => {
                println!("💡 Press Enter to view series within a dataset");
            }
            HierarchicalView::SeriesLevel => {
                println!("💡 Press Esc to return to dataset view");
            }
        }
        
        io::stdout().flush()?;
        Ok(())
    }
    
    /// Handle feature metadata display command
    async fn handle_feature_metadata_display(&self, input: &str, option_map: &std::collections::HashMap<usize, &&crate::db::FeatureMetadata>, plotter: &TimeSeriesPlotter) -> Result<()> {
        if let Some(num_str) = input.strip_prefix("/metadata ") {
            if let Ok(feature_num) = num_str.parse::<usize>() {
                if let Some(feature) = option_map.get(&feature_num) {
                    println!("\n{}", plotter.display_feature_metadata(*feature));
                    println!("\nPress Enter to continue...");
                    let mut _dummy = String::new();
                    std::io::stdin().read_line(&mut _dummy)?;
                } else {
                    println!("Invalid feature number: {}", feature_num);
                }
            }
        }
        Ok(())
    }
    
    /// Validate all features in the dataset
    fn validate_all_features(&self, features: &[crate::db::FeatureMetadata]) {
        println!("\n🔍 FEATURE VALIDATION RESULTS:");
        println!("{}", "=".repeat(60));
        
        let mut valid_count = 0;
        let mut invalid_count = 0;
        
        for (i, feature) in features.iter().enumerate() {
            print!("{}. {} - ", i + 1, feature.name);
            match feature.validate() {
                Ok(()) => {
                    println!("✅ Valid");
                    valid_count += 1;
                }
                Err(e) => {
                    println!("❌ Invalid: {}", e);
                    invalid_count += 1;
                }
            }
        }
        
        println!("{}", "=".repeat(60));
        println!("Summary: {} valid, {} invalid features", valid_count, invalid_count);
    }
    
    /// Export enhanced feature data
    async fn export_enhanced_feature_data(&self, enhanced_info: &crate::db::EnhancedDatasetColumnInfo, series_id: &str) -> Result<()> {
        println!("\n📁 EXPORTING ENHANCED FEATURE DATA...");
        
        // Create a JSON export of the enhanced feature information
        let export_data = serde_json::json!({
            "dataset_id": enhanced_info.dataset_id,
            "dataset_name": enhanced_info.dataset_name,
            "series_id": series_id,
            "features": enhanced_info.features,
            "timestamp_info": enhanced_info.timestamp_info,
            "export_timestamp": chrono::Utc::now().to_rfc3339(),
            "feature_summary": {
                "total_features": enhanced_info.features.len(),
                "targets": enhanced_info.features.iter().filter(|f| matches!(f.attribute, crate::db::FeatureAttribute::Targets)).count(),
                "historical_covariates": enhanced_info.features.iter().filter(|f| matches!(f.attribute, crate::db::FeatureAttribute::HistoricalCovariates)).count(),
                "static_covariates": enhanced_info.features.iter().filter(|f| matches!(f.attribute, crate::db::FeatureAttribute::StaticCovariates)).count(),
                "future_covariates": enhanced_info.features.iter().filter(|f| matches!(f.attribute, crate::db::FeatureAttribute::FutureCovariates)).count()
            }
        });
        
        let filename = format!("enhanced_features_{}_{}.json", enhanced_info.dataset_id, series_id.replace(':', "_"));
        
        match std::fs::write(&filename, serde_json::to_string_pretty(&export_data)?) {
            Ok(()) => {
                println!("✅ Exported enhanced feature metadata to: {}", filename);
            }
            Err(e) => {
                println!("❌ Failed to export: {}", e);
            }
        }
        
        println!("\nPress Enter to continue...");
        let mut _dummy = String::new();
        std::io::stdin().read_line(&mut _dummy)?;
        Ok(())
    }
    
    /// Display enhanced feature data with time series plot
    async fn display_enhanced_feature_data(&self, feature: &crate::db::FeatureMetadata, db: &Database, plotter: &TimeSeriesPlotter, series_id: &str) -> Result<()> {
        println!("\n{}", "=".repeat(70));
        println!("🔬 ENHANCED FEATURE ANALYSIS: {}", feature.name);
        println!("{}", "=".repeat(70));
        
        // Show feature metadata
        println!("{}", plotter.display_feature_metadata(feature));
        
        // Show feature data based on temporality
        match feature.temporality {
            crate::db::Temporality::Dynamic => {
                println!("📈 TIME SERIES VISUALIZATION:");
                println!("{}", "-".repeat(50));
                
                // Get the time series data with S3 fallback
                println!("⏳ Loading time series data...");
                let data = match db.get_time_series_data_with_fallback(series_id, &feature.name, Some(1000)).await {
                    Ok(data) => data,
                    Err(e) => {
                        println!("❌ Failed to load time series data: {}", e);
                        println!("💡 This may be due to network issues, AWS credentials, or missing data files.");
                        println!("\n{}", "=".repeat(70));
                        println!("Press Enter to continue...");
                        let mut _dummy = String::new();
                        std::io::stdin().read_line(&mut _dummy)?;
                        return Ok(());
                    }
                };
                
                if !data.is_empty() {
                    let plot = plotter.plot_time_series(&data, &feature.name, None, None)?;
                    println!("{}", plot);
                } else {
                    println!("⚠️  No time series data available for feature: {}", feature.name);
                }
            }
            crate::db::Temporality::Static => {
                println!("💎 STATIC FEATURE VALUE:");
                println!("{}", "-".repeat(50));
                
                // Get a sample value for static features with S3 fallback
                match db.get_time_series_data_with_fallback(series_id, &feature.name, Some(1)).await {
                    Ok(data) => {
                        if let Some(point) = data.first() {
                            println!("💎 Value: {}", point.value);
                            println!("📅 Constant across all time points in the series");
                        } else {
                            println!("⚠️  No data available for static feature: {}", feature.name);
                        }
                    }
                    Err(e) => {
                        println!("❌ Error retrieving static value: {}", e);
                        println!("💡 This feature may exist in metadata but not in actual data");
                    }
                }
            }
        }
        
        println!("\n{}", "=".repeat(70));
        println!("Press Enter to continue...");
        let mut _dummy = String::new();
        std::io::stdin().read_line(&mut _dummy)?;
        Ok(())
    }
    
    /// Show help for enhanced feature system
    fn show_enhanced_feature_help(&self) {
        println!("\n🆘 ENHANCED FEATURE SYSTEM HELP");
        println!("{}", "=".repeat(50));
        println!("This system provides sophisticated feature classification based on:");
        println!();
        println!("🎯 FEATURE ATTRIBUTES:");
        println!("  • Targets: Variables to be predicted");
        println!("  • Historical Covariates: Past information available during training");
        println!("  • Future Covariates: Future information known at prediction time");
        println!("  • Static Covariates: Time-invariant features");
        println!();
        println!("⏱️  TEMPORALITY:");
        println!("  • Dynamic: Changes over time");
        println!("  • Static: Constant across time");
        println!();
        println!("🎨 MODALITY:");
        println!("  • Numerical: Continuous/discrete numbers");
        println!("  • Categorical: Categories/labels");
        println!();
        println!("🌍 SCOPE:");
        println!("  • Local: Series-specific");
        println!("  • Global: Shared across series");
        println!();
        println!("⌨️  COMMANDS:");
        println!("  • /metadata <num>: Show detailed feature metadata");
        println!("  • /validate: Check all features for consistency");
        println!("  • /export: Export feature definitions to JSON");
        println!("  • <num>: Visualize feature (if time-varying)");
        println!("  • h/help: Show this help");
        println!("  • b/back: Return to series level");
        println!("  • q/quit: Exit application");
    }
}

#[derive(Debug, clap::Args)]
pub struct RepairCommand {
    /// Dataset to repair (optional - if not provided, repairs all datasets)
    #[arg(long)]
    pub dataset_id: Option<String>,
    
    /// Dry run - show what would be repaired without making changes
    #[arg(long, default_value = "false")]
    pub dry_run: bool,
    
    /// Database file path
    #[arg(long, default_value = "ts_indexer.db", help = "Path to database file (default: ts_indexer.db)")]
    pub database: String,
}

impl RepairCommand {
    pub async fn execute(self) -> Result<()> {
        use tracing::{info, warn};
        
        info!("🔧 Starting series record count repair...");
        
        // Initialize database
        info!("📊 Using database: {}", self.database);
        let db = Database::new(&self.database)?;
        
        if self.dry_run {
            println!("\n{}", "=".repeat(80));
            println!("🔍 DRY RUN - Series Record Count Analysis");
            println!("{}", "=".repeat(80));
            
            let datasets_with_issues = db.find_datasets_with_zero_record_counts()?;
            
            if datasets_with_issues.is_empty() {
                println!("✅ No datasets found with record_count = 0 issues!");
                return Ok(());
            }
            
            println!("Found {} datasets with record_count = 0 issues:\n", datasets_with_issues.len());
            
            for (dataset_id, total_series, zero_count) in datasets_with_issues {
                // Get expected record count
                match db.get_dataset_info(&dataset_id) {
                    Ok((_, total_records)) => {
                        let expected_per_series = if total_series > 0 { total_records / total_series } else { 0 };
                        println!("📊 Dataset: {}", dataset_id);
                        println!("   - Series with record_count=0: {}/{}", zero_count, total_series);
                        println!("   - Expected records per series: {}", expected_per_series);
                        println!("   - Total records to fix: {}", zero_count * expected_per_series);
                        println!();
                    },
                    Err(e) => {
                        warn!("Could not get info for dataset {}: {}", dataset_id, e);
                    }
                }
            }
            
            println!("Use --dry-run=false to perform the actual repair.");
            
        } else if let Some(dataset_id) = self.dataset_id {
            // Repair specific dataset
            println!("\n{}", "=".repeat(80));
            println!("🔧 Repairing Dataset: {}", dataset_id);
            println!("{}", "=".repeat(80));
            
            match db.update_series_record_counts_from_dataset_totals(&dataset_id) {
                Ok(updated_count) => {
                    println!("✅ Successfully updated {} series in dataset '{}'", updated_count, dataset_id);
                },
                Err(e) => {
                    println!("❌ Failed to repair dataset '{}': {}", dataset_id, e);
                }
            }
            
        } else {
            // Repair all datasets
            println!("\n{}", "=".repeat(80));
            println!("🔧 Repairing All Datasets");
            println!("{}", "=".repeat(80));
            
            match db.repair_all_series_record_counts() {
                Ok((datasets_fixed, series_fixed)) => {
                    println!("🎉 Repair completed successfully!");
                    println!("   - Datasets repaired: {}", datasets_fixed);
                    println!("   - Series updated: {}", series_fixed);
                },
                Err(e) => {
                    println!("❌ Repair failed: {}", e);
                }
            }
        }
        
        Ok(())
    }
}

#[derive(Debug)]
struct DatasetStats {
    pub total_records: i64,
    pub unique_series: i64,
    pub unique_columns: i64,
    pub min_timestamp: String,
    pub max_timestamp: String,
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