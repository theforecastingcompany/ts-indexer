use anyhow::Result;
use duckdb::Connection;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};
use std::env;

use super::{Database, Dataset, IndexingStatus, DatabaseStats, IndexingProgress};

/// MotherDuck-enabled database implementation
#[derive(Clone)]
pub struct MotherDuckDatabase {
    conn: Arc<Mutex<Connection>>,
    database_name: String,
}

impl MotherDuckDatabase {
    /// Create new MotherDuck connection
    pub fn new(database_name: &str) -> Result<Self> {
        let token = env::var("MOTHERDUCK_TOKEN")
            .map_err(|_| anyhow::anyhow!("MOTHERDUCK_TOKEN environment variable not set"))?;
        
        // MotherDuck connection string format
        let connection_string = format!("md:{}?token={}", database_name, token);
        
        info!("🦆 Connecting to MotherDuck database: {}", database_name);
        let conn = Connection::open(&connection_string)?;
        
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            database_name: database_name.to_string(),
        };
        
        // Initialize schema
        db.initialize_schema()?;
        
        info!("✅ MotherDuck connection established: {}", database_name);
        Ok(db)
    }
    
    /// Initialize database schema in MotherDuck
    fn initialize_schema(&self) -> Result<()> {
        info!("🔧 Initializing MotherDuck schema...");
        let conn = self.conn.lock().unwrap();
        
        // Create datasets table with MotherDuck optimizations
        conn.execute_batch(r#"
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description TEXT,
                source_bucket VARCHAR,
                source_prefix VARCHAR,
                schema_version VARCHAR,
                dataset_type VARCHAR,
                source VARCHAR,
                storage_location VARCHAR,
                item_id_type VARCHAR,
                target_type VARCHAR,
                tfc_features TEXT,
                
                -- Precise statistics
                total_series BIGINT DEFAULT 0,
                total_records BIGINT DEFAULT 0,
                avg_series_length DOUBLE,
                min_series_length BIGINT,
                max_series_length BIGINT,
                series_id_columns TEXT,
                
                -- Resumable indexing fields
                indexing_status VARCHAR DEFAULT 'pending',
                indexing_started_at TIMESTAMP,
                indexing_completed_at TIMESTAMP,
                indexing_error TEXT,
                
                -- File tracking
                metadata_file_path VARCHAR,
                data_file_count BIGINT,
                
                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Indexes for performance in MotherDuck
            CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(indexing_status);
            CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets(name);
            CREATE INDEX IF NOT EXISTS idx_datasets_series_count ON datasets(total_series);
            CREATE INDEX IF NOT EXISTS idx_datasets_created ON datasets(created_at);
            
            -- Create series_metadata table for detailed series info
            CREATE TABLE IF NOT EXISTS series_metadata (
                series_id VARCHAR,
                dataset_id VARCHAR,
                theme VARCHAR,
                description TEXT,
                tags_text TEXT,
                record_count BIGINT,
                first_timestamp VARCHAR,
                last_timestamp VARCHAR,
                PRIMARY KEY (series_id, dataset_id)
            );
            
            -- Performance indexes for series_metadata
            CREATE INDEX IF NOT EXISTS idx_series_dataset ON series_metadata(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_series_theme ON series_metadata(theme);
            CREATE INDEX IF NOT EXISTS idx_series_count ON series_metadata(record_count);
            
            -- Create search-optimized view
            CREATE OR REPLACE VIEW search_index AS
            SELECT 
                sm.series_id,
                sm.dataset_id,
                d.name as dataset_name,
                d.description as dataset_description,
                sm.theme,
                sm.description,
                sm.tags_text,
                sm.record_count,
                sm.first_timestamp,
                sm.last_timestamp,
                d.total_series,
                d.total_records,
                d.indexing_status
            FROM series_metadata sm
            JOIN datasets d ON sm.dataset_id = d.dataset_id
            WHERE d.indexing_status = 'completed';
        "#)?;
        
        info!("✅ MotherDuck schema initialized successfully");
        Ok(())
    }
    
    /// Get connection info for monitoring
    pub fn get_connection_info(&self) -> String {
        format!("MotherDuck: {}", self.database_name)
    }
    
    /// Sync local database to MotherDuck (for migration)
    pub fn sync_from_local(&self, local_db_path: &str) -> Result<()> {
        info!("🔄 Syncing local database to MotherDuck...");
        let conn = self.conn.lock().unwrap();
        
        // Attach local database
        conn.execute(&format!("ATTACH '{}' AS local_db", local_db_path), [])?;
        
        // Copy datasets
        conn.execute(r#"
            INSERT OR REPLACE INTO datasets 
            SELECT * FROM local_db.datasets
        "#, [])?;
        
        // Copy series metadata if exists
        conn.execute(r#"
            INSERT OR REPLACE INTO series_metadata 
            SELECT * FROM local_db.series_metadata
            WHERE EXISTS (SELECT 1 FROM local_db.series_metadata LIMIT 1)
        "#, []).unwrap_or_else(|e| {
            warn!("Could not sync series_metadata (table may not exist): {}", e);
        });
        
        // Detach local database
        conn.execute("DETACH local_db", [])?;
        
        info!("✅ Local database synced to MotherDuck");
        Ok(())
    }
}

// Implement the Database trait for MotherDuckDatabase
impl Database for MotherDuckDatabase {
    fn insert_dataset(&self, dataset: &Dataset) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        conn.execute(r#"
            INSERT OR REPLACE INTO datasets (
                dataset_id, name, description, source_bucket, source_prefix,
                schema_version, dataset_type, source, storage_location,
                item_id_type, target_type, tfc_features,
                total_series, total_records, avg_series_length,
                min_series_length, max_series_length, series_id_columns,
                indexing_status, indexing_started_at, indexing_completed_at,
                indexing_error, metadata_file_path, data_file_count,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#, [
            &dataset.dataset_id,
            &dataset.name,
            &dataset.description,
            &dataset.source_bucket,
            &dataset.source_prefix,
            &dataset.schema_version,
            &dataset.dataset_type,
            &dataset.source,
            &dataset.storage_location,
            &dataset.item_id_type,
            &dataset.target_type,
            &dataset.tfc_features,
            &dataset.total_series.to_string(),
            &dataset.total_records.to_string(),
            &dataset.avg_series_length.map(|f| f.to_string()),
            &dataset.min_series_length.map(|i| i.to_string()),
            &dataset.max_series_length.map(|i| i.to_string()),
            &dataset.series_id_columns,
            &dataset.indexing_status.to_string(),
            &dataset.indexing_started_at.map(|dt| dt.to_rfc3339()),
            &dataset.indexing_completed_at.map(|dt| dt.to_rfc3339()),
            &dataset.indexing_error,
            &dataset.metadata_file_path,
            &dataset.data_file_count.map(|i| i.to_string()),
            &dataset.created_at.to_rfc3339(),
            &dataset.updated_at.to_rfc3339(),
        ])?;
        
        Ok(())
    }
    
    // ... implement other Database trait methods ...
    // (Same implementation as local database but with MotherDuck optimizations)
}