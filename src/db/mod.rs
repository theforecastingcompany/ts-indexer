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

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    // Resumable indexing fields
    pub indexing_status: IndexingStatus,
    pub indexing_started_at: Option<DateTime<Utc>>,
    pub indexing_completed_at: Option<DateTime<Utc>>,
    pub indexing_error: Option<String>,
    pub metadata_file_path: Option<String>,
    pub data_file_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum IndexingStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl ToString for IndexingStatus {
    fn to_string(&self) -> String {
        match self {
            IndexingStatus::Pending => "pending".to_string(),
            IndexingStatus::InProgress => "in_progress".to_string(),
            IndexingStatus::Completed => "completed".to_string(),
            IndexingStatus::Failed => "failed".to_string(),
        }
    }
}

impl From<&str> for IndexingStatus {
    fn from(s: &str) -> Self {
        match s {
            "pending" => IndexingStatus::Pending,
            "in_progress" => IndexingStatus::InProgress,
            "completed" => IndexingStatus::Completed,
            "failed" => IndexingStatus::Failed,
            _ => IndexingStatus::Pending,
        }
    }
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
        
        // Dataset information with indexing status tracking
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
                -- Resumable indexing fields
                indexing_status VARCHAR DEFAULT 'pending', -- 'pending', 'in_progress', 'completed', 'failed'
                indexing_started_at BIGINT,
                indexing_completed_at BIGINT,
                indexing_error TEXT,
                metadata_file_path VARCHAR,
                data_file_count BIGINT DEFAULT 0,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(indexing_status);
            CREATE INDEX IF NOT EXISTS idx_datasets_completed ON datasets(indexing_completed_at);
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
                min_series_length, max_series_length, indexing_status,
                indexing_started_at, indexing_completed_at, indexing_error,
                metadata_file_path, data_file_count, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
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
                dataset.indexing_status.to_string(),
                dataset.indexing_started_at.map(|dt| dt.timestamp()),
                dataset.indexing_completed_at.map(|dt| dt.timestamp()),
                dataset.indexing_error,
                dataset.metadata_file_path,
                dataset.data_file_count,
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
                COUNT(*) as dataset_count,
                COALESCE(SUM(total_series), 0) as series_count,
                COALESCE(SUM(total_records), 0) as record_count
            FROM datasets
            WHERE indexing_status = 'completed'
        "#)?;
        
        let row = stmt.query_row([], |row| {
            Ok(DatabaseStats {
                dataset_count: row.get(0)?,
                series_count: row.get(1)?,
                record_count: row.get(2)?,
                earliest_timestamp: None, // Not available from datasets table
                latest_timestamp: None,   // Not available from datasets table
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
    
    /// Get datasets that need to be indexed (pending or failed)
    pub fn get_datasets_to_index(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT dataset_id FROM datasets 
             WHERE indexing_status IN ('pending', 'failed') 
             ORDER BY created_at ASC"
        )?;
        
        let rows = stmt.query_map([], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;
        
        let mut dataset_ids = Vec::new();
        for row in rows {
            dataset_ids.push(row?);
        }
        
        Ok(dataset_ids)
    }
    
    /// Get datasets by status
    pub fn get_datasets_by_status(&self, status: IndexingStatus) -> Result<Vec<Dataset>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"SELECT dataset_id, name, description, source_bucket, source_prefix,
                      schema_version, dataset_type, source, storage_location,
                      item_id_type, target_type, tfc_features, total_series,
                      total_records, series_id_columns, avg_series_length,
                      min_series_length, max_series_length, indexing_status,
                      indexing_started_at, indexing_completed_at, indexing_error,
                      metadata_file_path, data_file_count, created_at, updated_at
               FROM datasets WHERE indexing_status = ? ORDER BY created_at ASC"#
        )?;
        
        let rows = stmt.query_map([status.to_string()], |row| {
            Ok(Dataset {
                dataset_id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                source_bucket: row.get(3)?,
                source_prefix: row.get(4)?,
                schema_version: row.get(5)?,
                dataset_type: row.get(6)?,
                source: row.get(7)?,
                storage_location: row.get(8)?,
                item_id_type: row.get(9)?,
                target_type: row.get(10)?,
                tfc_features: row.get(11)?,
                total_series: row.get(12)?,
                total_records: row.get(13)?,
                series_id_columns: row.get(14)?,
                avg_series_length: row.get(15)?,
                min_series_length: row.get(16)?,
                max_series_length: row.get(17)?,
                indexing_status: IndexingStatus::from(row.get::<_, String>(18)?.as_str()),
                indexing_started_at: row.get::<_, Option<i64>>(19)?
                    .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                indexing_completed_at: row.get::<_, Option<i64>>(20)?
                    .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                indexing_error: row.get(21)?,
                metadata_file_path: row.get(22)?,
                data_file_count: row.get(23)?,
                created_at: DateTime::from_timestamp(row.get::<_, i64>(24)?, 0).unwrap(),
                updated_at: DateTime::from_timestamp(row.get::<_, i64>(25)?, 0).unwrap(),
            })
        })?;
        
        let mut datasets = Vec::new();
        for row in rows {
            datasets.push(row?);
        }
        
        Ok(datasets)
    }
    
    /// Update dataset indexing status
    pub fn update_dataset_status(&self, dataset_id: &str, status: IndexingStatus, error: Option<String>) -> Result<()> {
        let now = Utc::now().timestamp();
        
        let (started_at, completed_at) = match status {
            IndexingStatus::InProgress => (Some(now), None),
            IndexingStatus::Completed | IndexingStatus::Failed => (None, Some(now)),
            IndexingStatus::Pending => (None, None),
        };
        
        self.conn.lock().unwrap().execute(
            r#"UPDATE datasets 
               SET indexing_status = ?, 
                   indexing_started_at = COALESCE(?, indexing_started_at),
                   indexing_completed_at = ?,
                   indexing_error = ?,
                   updated_at = ?
               WHERE dataset_id = ?"#,
            duckdb::params![
                status.to_string(),
                started_at,
                completed_at,
                error,
                now,
                dataset_id
            ],
        )?;
        
        Ok(())
    }
    
    /// Mark dataset as started (sets in_progress status and start time)
    pub fn mark_dataset_started(&self, dataset_id: &str) -> Result<()> {
        self.update_dataset_status(dataset_id, IndexingStatus::InProgress, None)
    }
    
    /// Mark dataset as completed successfully
    pub fn mark_dataset_completed(&self, dataset_id: &str) -> Result<()> {
        self.update_dataset_status(dataset_id, IndexingStatus::Completed, None)
    }
    
    /// Mark dataset as failed with error message
    pub fn mark_dataset_failed(&self, dataset_id: &str, error: &str) -> Result<()> {
        self.update_dataset_status(dataset_id, IndexingStatus::Failed, Some(error.to_string()))
    }
    
    /// Check if dataset exists and get its status
    pub fn get_dataset_status(&self, dataset_id: &str) -> Result<Option<IndexingStatus>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT indexing_status FROM datasets WHERE dataset_id = ?")?;
        
        match stmt.query_row([dataset_id], |row| {
            Ok(IndexingStatus::from(row.get::<_, String>(0)?.as_str()))
        }) {
            Ok(status) => Ok(Some(status)),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    
    /// Get indexing progress statistics
    pub fn get_indexing_progress(&self) -> Result<IndexingProgress> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT 
                indexing_status,
                COUNT(*) as count
            FROM datasets 
            GROUP BY indexing_status
        "#)?;
        
        let rows = stmt.query_map([], |row| {
            Ok((
                IndexingStatus::from(row.get::<_, String>(0)?.as_str()),
                row.get::<_, i64>(1)?
            ))
        })?;
        
        let mut progress = IndexingProgress::default();
        for row in rows {
            let (status, count) = row?;
            match status {
                IndexingStatus::Pending => progress.pending = count,
                IndexingStatus::InProgress => progress.in_progress = count,
                IndexingStatus::Completed => progress.completed = count,
                IndexingStatus::Failed => progress.failed = count,
            }
        }
        
        Ok(progress)
    }
}

#[derive(Debug, Default, Serialize)]
pub struct IndexingProgress {
    pub pending: i64,
    pub in_progress: i64,
    pub completed: i64,
    pub failed: i64,
}

impl IndexingProgress {
    pub fn total(&self) -> i64 {
        self.pending + self.in_progress + self.completed + self.failed
    }
    
    pub fn completion_percentage(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            (self.completed as f64 / total as f64) * 100.0
        }
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