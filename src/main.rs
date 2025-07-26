use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing::{info, Level};
use tracing_subscriber;

mod cli;
mod db;
mod indexer;
mod s3;
mod search;

use cli::commands::{IndexCommand, SearchCommand, StatsCommand};

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
    /// Show indexing statistics
    Stats(StatsCommand),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging
    let level = if cli.verbose { Level::DEBUG } else { Level::INFO };
    tracing_subscriber::fmt()
        .with_max_level(level)
        .init();
    
    info!("Starting TS-Indexer v{}", env!("CARGO_PKG_VERSION"));
    
    match cli.command {
        Commands::Index(cmd) => cmd.execute().await,
        Commands::Search(cmd) => cmd.execute().await,
        Commands::Stats(cmd) => cmd.execute().await,
    }
}
