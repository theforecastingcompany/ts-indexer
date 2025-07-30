use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{info, Level};
use tracing_subscriber::{self, EnvFilter};

mod cli;
mod db;
mod indexer;
mod monitoring;
mod plotting;
mod progress;
mod s3;
mod search;

use cli::commands::{AnalyzeSizesCommand, CleanCommand, IndexCommand, RepairCommand, SearchCommand, StatusCommand};

#[derive(Parser)]
#[command(name = "ts-indexer")]
#[command(about = "High-performance time-series indexer for S3 data")]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Index time-series data from S3
    Index(IndexCommand),
    /// Search indexed time-series
    Search(SearchCommand),
    /// Show database status, statistics, and indexing progress
    Status(StatusCommand),
    /// Clean and reinitialize all states (database + progress files)
    Clean(CleanCommand),
    /// Analyze file sizes in S3 bucket to understand performance characteristics
    AnalyzeSizes(AnalyzeSizesCommand),
    /// Repair database inconsistencies (e.g., fix series record counts)
    Repair(RepairCommand),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Cap Polars threads to prevent CPU thrashing on large files
    // Based on monitoring data showing 800%+ CPU usage
    let max_polars_threads = std::cmp::min(4, num_cpus::get());
    std::env::set_var("POLARS_MAX_THREADS", max_polars_threads.to_string());
    info!("🧵 Limited Polars to {} threads to prevent CPU thrashing", max_polars_threads);
    
    // Initialize logging with filtered levels
    let base_level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    
    // Create comprehensive AWS SDK logging filter
    let aws_filter = "aws_smithy_runtime=error,aws_sdk_s3=error,aws_config=error,hyper_util=error,aws_smithy_types=error,aws_smithy_http=error,aws_smithy_client=error,aws_endpoint=error,aws_credential_types=error,aws_sigv4=error,h2=error,hyper=error,reqwest=error,rustls=error,tower=error,aws_smithy_async=error,aws_smithy_json=error,aws_smithy_query=error,aws_smithy_xml=error,aws_types=error,aws_runtime_api=error";
    
    tracing_subscriber::fmt()
        .with_max_level(base_level)
        .with_env_filter(
            EnvFilter::new(format!(
                "ts_indexer={},{}",
                if cli.verbose { "debug" } else { "info" },
                aws_filter
            ))
        )
        .init();
    
    info!("Starting TS-Indexer v{}", env!("CARGO_PKG_VERSION"));
    
    match cli.command {
        Commands::Index(cmd) => cmd.execute().await,
        Commands::Search(cmd) => cmd.execute().await,
        Commands::Status(cmd) => cmd.execute().await,
        Commands::Clean(cmd) => cmd.execute().await,
        Commands::AnalyzeSizes(cmd) => cmd.execute().await,
        Commands::Repair(cmd) => cmd.execute().await,
    }
}
