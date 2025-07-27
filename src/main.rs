use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{info, Level};
use tracing_subscriber::{self, EnvFilter};

mod cli;
mod db;
mod indexer;
mod s3;
mod search;

use cli::commands::{IndexCommand, SearchCommand, StatusCommand};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging with filtered levels
    let base_level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    
    tracing_subscriber::fmt()
        .with_max_level(base_level)
        .with_env_filter(
            EnvFilter::new(format!(
                "ts_indexer={},aws_smithy_runtime=warn,aws_sdk_s3=warn,aws_config=warn,hyper_util=warn",
                if cli.verbose { "debug" } else { "info" }
            ))
        )
        .init();
    
    info!("Starting TS-Indexer v{}", env!("CARGO_PKG_VERSION"));
    
    match cli.command {
        Commands::Index(cmd) => cmd.execute().await,
        Commands::Search(cmd) => cmd.execute().await,
        Commands::Status(cmd) => cmd.execute().await,
    }
}
