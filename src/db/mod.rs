use anyhow::Result;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSeriesRecord {
    pub id: Uuid,
    pub series_id: String,
    pub dataset_id: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeriesMetadata {
    pub series_id: String,
    pub dataset_id: String,
    pub theme: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub source_file: String,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
    pub record_count: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dataset {
    pub dataset_id: String,
    pub name: String,
    pub description: Option<String>,
    pub source_bucket: String,
    pub source_prefix: String,
    pub schema_version: String,
    pub dataset_type: Option<String>,
    pub source: Option<String>,
    pub storage_location: Option<String>,
    pub item_id_type: Option<String>,
    pub target_type: Option<String>,
    pub tfc_features: Option<String>, // JSON string of features
    pub total_series: i64,
    pub total_records: i64,
    pub series_id_columns: Option<String>, // JSON array of series ID column names
    pub avg_series_length: Option<f64>,
    pub min_series_length: Option<i64>,
    pub max_series_length: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Database {
    /// Create a new database connection and initialize schema
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let db = Database { 
            conn: Arc::new(Mutex::new(conn)) 
        };
        db.initialize_schema()?;
        Ok(db)
    }
    
    /// Create an in-memory database for testing
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Database { 
            conn: Arc::new(Mutex::new(conn)) 
        };
        db.initialize_schema()?;
        Ok(db)
    }
    
    /// Initialize database schema with optimized tables and indexes
    fn initialize_schema(&self) -> Result<()> {
        info!("Initializing database schema...");
        
        // Time series data table with columnar optimization
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE TABLE IF NOT EXISTS time_series (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                series_id VARCHAR NOT NULL,
                dataset_id VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                value DOUBLE NOT NULL,
                created_at BIGINT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_time_series_series_id ON time_series(series_id);
            CREATE INDEX IF NOT EXISTS idx_time_series_dataset_id ON time_series(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_time_series_timestamp ON time_series(timestamp);
            CREATE INDEX IF NOT EXISTS idx_time_series_composite ON time_series(series_id, timestamp);
        "#)?;
        
        // Series metadata with full-text search capabilities
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE TABLE IF NOT EXISTS series_metadata (
                series_id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                theme VARCHAR,
                description TEXT,
                tags TEXT, -- JSON array of tags
                source_file VARCHAR NOT NULL,
                first_timestamp BIGINT NOT NULL,
                last_timestamp BIGINT NOT NULL,
                record_count BIGINT NOT NULL,
                created_at BIGINT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_series_metadata_dataset_id ON series_metadata(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_series_metadata_theme ON series_metadata(theme);
            CREATE INDEX IF NOT EXISTS idx_series_metadata_tags ON series_metadata(tags);
        "#)?;
        
        // Dataset information
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                description TEXT,
                source_bucket VARCHAR NOT NULL,
                source_prefix VARCHAR NOT NULL,
                schema_version VARCHAR NOT NULL DEFAULT '1.0',
                dataset_type VARCHAR,
                source VARCHAR,
                storage_location VARCHAR,
                item_id_type VARCHAR,
                target_type VARCHAR,
                tfc_features TEXT,
                total_series BIGINT DEFAULT 0,
                total_records BIGINT DEFAULT 0,
                series_id_columns TEXT,
                avg_series_length DOUBLE,
                min_series_length BIGINT,
                max_series_length BIGINT,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            );
        "#)?;
        
        // Search optimization view for fuzzy matching
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE OR REPLACE VIEW search_index AS
            SELECT 
                sm.series_id,
                sm.dataset_id,
                d.name as dataset_name,
                d.description as dataset_description,
                d.dataset_type,
                d.source as dataset_source,
                sm.theme,
                sm.description,
                sm.tags as tags_text,
                sm.source_file,
                sm.first_timestamp,
                sm.last_timestamp,
                sm.record_count
            FROM series_metadata sm
            LEFT JOIN datasets d ON sm.dataset_id = d.dataset_id
            UNION ALL
            SELECT 
                d.dataset_id as series_id,
                d.dataset_id,
                d.name as dataset_name,
                d.description as dataset_description,
                d.dataset_type,
                d.source as dataset_source,
                d.dataset_type as theme,
                d.description,
                NULL as tags_text,
                d.storage_location as source_file,
                NULL as first_timestamp,
                NULL as last_timestamp,
                d.total_records as record_count
            FROM datasets d;
        "#)?;
        
        debug!("Database schema initialized successfully");
        Ok(())
    }
    
    /// Insert a dataset record
    pub fn insert_dataset(&self, dataset: &Dataset) -> Result<()> {
        self.conn.lock().unwrap().execute(
            r#"INSERT OR REPLACE INTO datasets 
               (dataset_id, name, description, source_bucket, source_prefix, 
                schema_version, dataset_type, source, storage_location, 
                item_id_type, target_type, tfc_features, total_series, 
                total_records, series_id_columns, avg_series_length, 
                min_series_length, max_series_length, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            duckdb::params![
                dataset.dataset_id,
                dataset.name,
                dataset.description,
                dataset.source_bucket,
                dataset.source_prefix,
                dataset.schema_version,
                dataset.dataset_type,
                dataset.source,
                dataset.storage_location,
                dataset.item_id_type,
                dataset.target_type,
                dataset.tfc_features,
                dataset.total_series,
                dataset.total_records,
                dataset.series_id_columns,
                dataset.avg_series_length,
                dataset.min_series_length,
                dataset.max_series_length,
                dataset.created_at.timestamp(),
                dataset.updated_at.timestamp()
            ],
        )?;
        Ok(())
    }
    
    /// Insert series metadata
    pub fn insert_series_metadata(&self, metadata: &SeriesMetadata) -> Result<()> {
        self.conn.lock().unwrap().execute(
            r#"INSERT OR REPLACE INTO series_metadata 
               (series_id, dataset_id, theme, description, tags, source_file,
                first_timestamp, last_timestamp, record_count, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            duckdb::params![
                metadata.series_id,
                metadata.dataset_id,
                metadata.theme,
                metadata.description,
                serde_json::to_string(&metadata.tags)?,
                metadata.source_file,
                metadata.first_timestamp.timestamp(),
                metadata.last_timestamp.timestamp(),
                metadata.record_count,
                metadata.created_at.timestamp()  
            ],
        )?;
        Ok(())
    }
    
    /// Batch insert time series data for performance
    pub fn batch_insert_time_series(&self, records: &[TimeSeriesRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "INSERT INTO time_series (series_id, dataset_id, timestamp, value, created_at) 
             VALUES (?, ?, ?, ?, ?)"
        )?;
        
        for record in records {
            stmt.execute(duckdb::params![
                record.series_id,
                record.dataset_id,
                record.timestamp.timestamp(),
                record.value,
                record.created_at.timestamp()
            ])?;
        }
        
        Ok(())
    }
    
    /// Get database statistics
    pub fn get_stats(&self) -> Result<DatabaseStats> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT 
                (SELECT COUNT(*) FROM datasets) as dataset_count,
                (SELECT COUNT(*) FROM series_metadata) as series_count,
                (SELECT COUNT(*) FROM time_series) as record_count,
                (SELECT MIN(timestamp) FROM time_series) as earliest_timestamp,
                (SELECT MAX(timestamp) FROM time_series) as latest_timestamp
        "#)?;
        
        let row = stmt.query_row([], |row| {
            Ok(DatabaseStats {
                dataset_count: row.get(0)?,
                series_count: row.get(1)?,
                record_count: row.get(2)?,
                earliest_timestamp: row.get::<_, Option<i64>>(3)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
                latest_timestamp: row.get::<_, Option<i64>>(4)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
            })
        })?;
        
        Ok(row)
    }
    
    /// Search series with fuzzy matching
    pub fn search_series(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        let search_query = format!("%{}%", query.to_lowercase());
        
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT series_id, dataset_id, dataset_name, theme, description, 
                   tags_text, record_count, first_timestamp, last_timestamp
            FROM search_index
            WHERE LOWER(series_id) LIKE ?
               OR LOWER(dataset_name) LIKE ?
               OR LOWER(dataset_description) LIKE ?
               OR LOWER(dataset_type) LIKE ?
               OR LOWER(dataset_source) LIKE ?
               OR LOWER(theme) LIKE ?
               OR LOWER(description) LIKE ?
               OR LOWER(tags_text) LIKE ?
            ORDER BY record_count DESC
            LIMIT ?
        "#)?;
        
        let rows = stmt.query_map(
            duckdb::params![search_query, search_query, search_query, search_query, search_query, search_query, search_query, search_query, limit],
            |row| {
                Ok(SearchResult {
                    series_id: row.get(0)?,
                    dataset_id: row.get(1)?,
                    dataset_name: row.get(2)?,
                    theme: row.get(3)?,
                    description: row.get(4)?,
                    tags_text: row.get(5)?,
                    record_count: row.get(6)?,
                    first_timestamp: row.get::<_, Option<i64>>(7)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0))
                        .unwrap_or_else(|| Utc::now()),
                    last_timestamp: row.get::<_, Option<i64>>(8)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0))
                        .unwrap_or_else(|| Utc::now()),
                })
            }
        )?;
        
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        
        Ok(results)
    }
}

#[derive(Debug, Serialize)]
pub struct DatabaseStats {
    pub dataset_count: i64,
    pub series_count: i64,
    pub record_count: i64,
    pub earliest_timestamp: Option<DateTime<Utc>>,
    pub latest_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub series_id: String,
    pub dataset_id: String,
    pub dataset_name: Option<String>,
    pub theme: Option<String>,
    pub description: Option<String>,
    pub tags_text: Option<String>,
    pub record_count: i64,
    pub first_timestamp: DateTime<Utc>,
    pub last_timestamp: DateTime<Utc>,
}