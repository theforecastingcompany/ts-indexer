use anyhow::Result;
use clap::Args;
use std::io::{self, Write};
use tracing::info;

use crate::db::Database;
use crate::indexer::Indexer;
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
    
    /// Interactive hierarchical search mode
    #[arg(short, long, help = "Enable interactive hierarchical search with navigation")]
    pub interactive: bool,
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
        
        if self.interactive {
            // Interactive hierarchical search mode
            self.run_interactive_search(search_engine).await
        } else {
            // Traditional search mode
            self.run_traditional_search(search_engine).await
        }
    }
    
    async fn run_interactive_search(&self, search_engine: SearchEngine) -> Result<()> {
        use crossterm::{
            event::{self, Event, KeyCode, KeyEvent},
            execute,
            terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        };
        use std::io::{self, Write};
        
        // Get search results (increased limit for comprehensive search)
        let search_query = create_search_query(self.query.clone(), Some(1000));
        let results = search_engine.search(search_query).await?;
        
        // Group results by dataset for hierarchical display
        let hierarchical_results = self.group_results_hierarchically(&results.results);
        
        // Force simple mode to avoid terminal compatibility issues with Ghostty/zsh
        // let raw_mode_enabled = enable_raw_mode().is_ok();
        // let alternate_screen_enabled = execute!(io::stdout(), EnterAlternateScreen).is_ok();
        
        // Always use simple mode for better compatibility
        return self.run_simple_interactive_search(hierarchical_results).await;
        
        let mut current_view = HierarchicalView::DatasetLevel;
        let mut dataset_index = 0;
        let mut series_index = 0;
        let mut selected_dataset = None;
        
        loop {
            // Simple clear with newlines instead of terminal control sequences
            print!("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"); // Clear with newlines
            
            self.display_hierarchical_view(&hierarchical_results, &current_view, dataset_index, series_index, &selected_dataset)?;
            
            // Handle keyboard input
            if let Event::Key(key_event) = event::read()? {
                match key_event.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Esc => {
                        if current_view == HierarchicalView::SeriesLevel {
                            current_view = HierarchicalView::DatasetLevel;
                            selected_dataset = None;
                            series_index = 0;
                        } else {
                            break;
                        }
                    }
                    KeyCode::Up => {
                        match current_view {
                            HierarchicalView::DatasetLevel => {
                                if dataset_index > 0 {
                                    dataset_index -= 1;
                                }
                            }
                            HierarchicalView::SeriesLevel => {
                                if let Some(dataset_name) = &selected_dataset {
                                    if let Some(dataset_data) = hierarchical_results.get(dataset_name) {
                                        if series_index > 0 {
                                            series_index -= 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    KeyCode::Down => {
                        match current_view {
                            HierarchicalView::DatasetLevel => {
                                if dataset_index < hierarchical_results.len().saturating_sub(1) {
                                    dataset_index += 1;
                                }
                            }
                            HierarchicalView::SeriesLevel => {
                                if let Some(dataset_name) = &selected_dataset {
                                    if let Some(dataset_data) = hierarchical_results.get(dataset_name) {
                                        if series_index < dataset_data.series.len().saturating_sub(1) {
                                            series_index += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    KeyCode::Enter => {
                        if current_view == HierarchicalView::DatasetLevel {
                            // Expand to series level
                            let dataset_names: Vec<_> = hierarchical_results.keys().collect();
                            if let Some(&dataset_name) = dataset_names.get(dataset_index) {
                                selected_dataset = Some(dataset_name.clone());
                                current_view = HierarchicalView::SeriesLevel;
                                series_index = 0;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        
        // Terminal cleanup disabled since we're using simple mode
        // if raw_mode_enabled {
        //     let _ = disable_raw_mode();
        // }
        // if alternate_screen_enabled {
        //     let _ = execute!(io::stdout(), LeaveAlternateScreen);
        // }
        
        Ok(())
    }
    
    async fn run_simple_interactive_search(&self, hierarchical_results: std::collections::HashMap<String, DatasetGroup>) -> Result<()> {
        use std::io::{self, Write};
        
        println!("🔍 Interactive Search: '{}' (Simple Mode)\n", self.query);
        
        // Display dataset-level results
        println!("📁 DATASETS ({} found)", hierarchical_results.len());
        println!("Enter dataset number to view series, or 'q' to quit\n");
        
        let dataset_names: Vec<_> = hierarchical_results.keys().cloned().collect();
        
        for (idx, dataset_name) in dataset_names.iter().enumerate() {
            let group = &hierarchical_results[dataset_name];
            let series_count = group.series.len();
            let total_records = group.total_records;
            
            println!("{}. 📂 {} ({} series, {} records)", 
                     idx + 1, dataset_name, series_count, total_records);
        }
        
        loop {
            print!("\n> ");
            io::stdout().flush()?;
            
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim();
            
            if input == "q" || input == "quit" {
                break;
            }
            
            if let Ok(choice) = input.parse::<usize>() {
                if choice > 0 && choice <= dataset_names.len() {
                    let dataset_name = &dataset_names[choice - 1];
                    let group = &hierarchical_results[dataset_name];
                    
                    println!("\n📂 {} > 📊 SERIES ({} series)", dataset_name, group.series.len());
                    println!("Enter 'b' to go back, or 'q' to quit\n");
                    
                    for (idx, series) in group.series.iter().enumerate() {
                        println!("{}. 📊 {} ({} records, relevance: {:.2})", 
                                 idx + 1, series.search_result.series_id,
                                 series.search_result.record_count,
                                 series.relevance_score);
                    }
                    
                    // Wait for user input to continue
                    loop {
                        print!("\n> ");
                        io::stdout().flush()?;
                        
                        let mut series_input = String::new();
                        io::stdin().read_line(&mut series_input)?;
                        let series_input = series_input.trim();
                        
                        if series_input == "q" || series_input == "quit" {
                            return Ok(());
                        } else if series_input == "b" || series_input == "back" {
                            break;
                        }
                    }
                    
                    // Show datasets again
                    println!("\n📁 DATASETS ({} found)", hierarchical_results.len());
                    for (idx, dataset_name) in dataset_names.iter().enumerate() {
                        let group = &hierarchical_results[dataset_name];
                        let series_count = group.series.len();
                        let total_records = group.total_records;
                        
                        println!("{}. 📂 {} ({} series, {} records)", 
                                 idx + 1, dataset_name, series_count, total_records);
                    }
                } else {
                    println!("Invalid choice. Please enter a number between 1 and {}", dataset_names.len());
                }
            } else {
                println!("Invalid input. Enter a dataset number or 'q' to quit.");
            }
        }
        
        Ok(())
    }
    
    async fn run_traditional_search(&self, search_engine: SearchEngine) -> Result<()> {
        // Perform search
        let search_query = create_search_query(self.query.clone(), Some(self.limit));
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
                progress_bar, state.completed_count, state.total_datasets, progress_pct);
        
        println!("⏰ Runtime: {} (started {})", 
                state.elapsed_time(),
                state.started_at.format("%H:%M:%S"));
        
        if state.processing_rate_per_min > 0.0 {
            println!("🚀 Rate: {:.1} datasets/min", state.processing_rate_per_min);
        }
        
        if let Some(eta) = state.estimated_completion_time {
            let remaining_duration = eta.signed_duration_since(chrono::Utc::now());
            if remaining_duration.num_minutes() > 0 {
                println!("🎯 ETA: {} ({}m remaining)", 
                        eta.format("%H:%M:%S"), 
                        remaining_duration.num_minutes());
            }
        }
        
        println!("\n📈 Queue Status:");
        println!("  ✅ Completed: {} datasets", state.completed_count);
        if state.failed_count > 0 {
            println!("  ❌ Failed: {} datasets", state.failed_count);
        }
        println!("  ⏳ Pending: {} datasets", pending_count);
        
        println!("\n💾 Last Updated: {}", state.last_updated.format("%H:%M:%S"));
        
        if self.detailed {
            // For detailed view, also show database stats for additional context
            if let Ok(db) = Database::new("ts_indexer.db") {
                let stats = db.get_stats()?;
                println!("\n📋 Database Statistics:");
                println!("  🔢 Series Indexed: {}", stats.series_count);
                println!("  📈 Records Indexed: {}", stats.record_count);
            }
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
}

impl CleanCommand {
    pub async fn execute(self) -> Result<()> {
        println!("🧹 Clean Command - Reinitialize All States");
        println!("{:-<60}", "");
        
        // Check for running indexers first
        if let Ok(Some(state)) = ProgressState::read_from_file() {
            if state.is_indexer_alive() {
                println!("❌ Cannot clean while indexer is running!");
                println!("   Active indexer PID: {}", state.indexer_pid);
                println!("   Kill the indexer first: kill {}", state.indexer_pid);
                return Ok(());
            }
        }
        
        // Show what will be cleaned
        let mut items_to_clean = Vec::new();
        
        if std::path::Path::new("ts_indexer_progress.json").exists() {
            items_to_clean.push("📄 Progress tracking file (ts_indexer_progress.json)");
        }
        
        if !self.progress_only {
            if std::path::Path::new("ts_indexer.db").exists() {
                items_to_clean.push("🗄️  Main database (ts_indexer.db)");
            }
            if std::path::Path::new("ts_indexer.db-wal").exists() {
                items_to_clean.push("📝 Database WAL file (ts_indexer.db-wal)");
            }
            if std::path::Path::new("ts_indexer.db-shm").exists() {
                items_to_clean.push("🔗 Database shared memory (ts_indexer.db-shm)");
            }
            if std::path::Path::new("ts_indexer_readonly.db").exists() {
                items_to_clean.push("📖 Temporary readonly database (ts_indexer_readonly.db)");
            }
        }
        
        if items_to_clean.is_empty() {
            println!("✨ Nothing to clean - all states are already clear!");
            return Ok(());
        }
        
        println!("The following items will be removed:");
        for item in &items_to_clean {
            println!("  {}", item);
        }
        
        if self.progress_only {
            println!("\n💡 Progress-only mode: Database will be preserved");
        } else {
            println!("\n⚠️  WARNING: This will delete all indexed data!");
            println!("   You will need to reindex all datasets from scratch.");
        }
        
        // Confirmation prompt (unless --force)
        if !self.force {
            println!("\nAre you sure you want to continue? (y/N)");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            let input = input.trim().to_lowercase();
            
            if input != "y" && input != "yes" {
                println!("❌ Clean operation cancelled.");
                return Ok(());
            }
        }
        
        // Perform the cleaning
        let mut cleaned_count = 0;
        let mut errors = Vec::new();
        
        // Clean progress files
        for file in &["ts_indexer_progress.json", "ts_indexer_progress.json.tmp"] {
            if std::path::Path::new(file).exists() {
                match std::fs::remove_file(file) {
                    Ok(_) => {
                        println!("✅ Removed {}", file);
                        cleaned_count += 1;
                    }
                    Err(e) => {
                        errors.push(format!("Failed to remove {}: {}", file, e));
                    }
                }
            }
        }
        
        // Clean database files (unless progress-only mode)
        if !self.progress_only {
            for file in &["ts_indexer.db", "ts_indexer.db-wal", "ts_indexer.db-shm", "ts_indexer_readonly.db"] {
                if std::path::Path::new(file).exists() {
                    match std::fs::remove_file(file) {
                        Ok(_) => {
                            println!("✅ Removed {}", file);
                            cleaned_count += 1;
                        }
                        Err(e) => {
                            errors.push(format!("Failed to remove {}: {}", file, e));
                        }
                    }
                }
            }
        }
        
        // Report results
        println!("\n🎉 Clean operation completed!");
        println!("   ✅ {} items removed successfully", cleaned_count);
        
        if !errors.is_empty() {
            println!("   ❌ {} errors occurred:", errors.len());
            for error in errors {
                println!("      {}", error);
            }
        }
        
        if !self.progress_only {
            println!("\n💡 Next steps:");
            println!("   • Run 'ts-indexer index' to rebuild the database");
            println!("   • All datasets will need to be reindexed from S3");
        } else {
            println!("\n💡 Database preserved - existing indexed data is still available");
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
struct DatasetGroup {
    dataset_name: String,
    dataset_info: Option<crate::search::EnhancedSearchResult>,
    series: Vec<crate::search::EnhancedSearchResult>,
    total_records: i64,
}

impl SearchCommand {
    fn group_results_hierarchically(&self, results: &[crate::search::EnhancedSearchResult]) -> std::collections::HashMap<String, DatasetGroup> {
        use std::collections::HashMap;
        
        let mut groups: HashMap<String, DatasetGroup> = HashMap::new();
        
        for result in results {
            let dataset_id = &result.search_result.dataset_id;
            
            // Check if this is a dataset-level result (series_id == dataset_id)
            let is_dataset_level = result.search_result.series_id == *dataset_id;
            
            groups.entry(dataset_id.clone())
                .and_modify(|group| {
                    if is_dataset_level {
                        group.dataset_info = Some(result.clone());
                    } else {
                        group.series.push(result.clone());
                    }
                })
                .or_insert_with(|| {
                    let mut group = DatasetGroup {
                        dataset_name: dataset_id.clone(),
                        dataset_info: None,
                        series: Vec::new(),
                        total_records: 0,
                    };
                    
                    if is_dataset_level {
                        group.dataset_info = Some(result.clone());
                        group.total_records = result.search_result.record_count;
                    } else {
                        group.series.push(result.clone());
                    }
                    
                    group
                });
        }
        
        // Calculate total records for datasets without dataset-level info
        for group in groups.values_mut() {
            if group.dataset_info.is_none() {
                group.total_records = group.series.iter().map(|s| s.search_result.record_count).sum();
            }
        }
        
        groups
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
        println!("🔍 Interactive Search: '{}'\n", self.query);
        
        match current_view {
            HierarchicalView::DatasetLevel => {
                println!("📁 DATASETS ({} found)", hierarchical_results.len());
                println!("Use ↑↓ to navigate, Enter to expand, 'q' to quit\n");
                
                let dataset_names: Vec<_> = hierarchical_results.keys().cloned().collect();
                
                for (idx, dataset_name) in dataset_names.iter().enumerate() {
                    let group = &hierarchical_results[dataset_name];
                    let prefix = if idx == dataset_index { "► " } else { "" };
                    
                    let series_count = group.series.len();
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