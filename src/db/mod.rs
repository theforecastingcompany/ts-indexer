use anyhow::{Context, Result};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::io::Cursor;
use tracing::{debug, info, warn};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use polars::prelude::*;

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSeriesRecord {
    pub id: Uuid,
    pub series_id: String,
    pub dataset_id: String,
    pub column_name: String,
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub targets: Option<String>, // JSON array of target field info
    pub covariates: Option<String>, // JSON string of covariates (hist/future/static)
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
                column_name VARCHAR NOT NULL,
                timestamp BIGINT NOT NULL,
                value DOUBLE NOT NULL,
                created_at BIGINT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_time_series_series_id ON time_series(series_id);
            CREATE INDEX IF NOT EXISTS idx_time_series_dataset_id ON time_series(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_time_series_timestamp ON time_series(timestamp);
            CREATE INDEX IF NOT EXISTS idx_time_series_composite ON time_series(series_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_time_series_column ON time_series(series_id, column_name);
        "#)?;
        
        // Cached files tracking table
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE TABLE IF NOT EXISTS cached_files (
                file_path VARCHAR PRIMARY KEY,
                dataset_id VARCHAR NOT NULL,
                cached_at BIGINT NOT NULL,
                file_size BIGINT NOT NULL,
                series_count INTEGER NOT NULL,
                created_at BIGINT NOT NULL DEFAULT (extract(epoch from now()))
            );
            
            CREATE INDEX IF NOT EXISTS idx_cached_files_dataset_id ON cached_files(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_cached_files_cached_at ON cached_files(cached_at);
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
                targets TEXT, -- JSON array of target field info
                covariates TEXT, -- JSON string of covariates (hist/future/static)
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
        
        // Search optimization view for fuzzy matching - only series_metadata, no pseudo-entries
        self.conn.lock().unwrap().execute_batch(r#"
            CREATE OR REPLACE VIEW search_index AS
            SELECT 
                sm.series_id,
                sm.dataset_id,
                COALESCE(d.name, sm.dataset_id) as dataset_name,
                d.description as dataset_description,
                d.dataset_type,
                d.source as dataset_source,
                sm.theme,
                sm.description,
                sm.tags as tags_text,
                sm.source_file,
                sm.first_timestamp,
                sm.last_timestamp,
                CASE 
                    WHEN sm.record_count > 0 THEN sm.record_count
                    WHEN d.total_series > 0 THEN d.total_records / d.total_series
                    ELSE 0
                END as record_count
            FROM series_metadata sm
            LEFT JOIN datasets d ON sm.dataset_id = d.dataset_id;
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
                item_id_type, target_type, targets, covariates, tfc_features, total_series, 
                total_records, series_id_columns, avg_series_length, 
                min_series_length, max_series_length, indexing_status,
                indexing_started_at, indexing_completed_at, indexing_error,
                metadata_file_path, data_file_count, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
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
                dataset.targets,
                dataset.covariates,
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
    
    /// Delete all series metadata for a specific dataset
    pub fn delete_series_metadata_for_dataset(&self, dataset_id: &str) -> Result<usize> {
        let rows_deleted = self.conn.lock().unwrap().execute(
            "DELETE FROM series_metadata WHERE dataset_id = ?",
            duckdb::params![dataset_id],
        )?;
        
        if rows_deleted > 0 {
            debug!("Deleted {} existing series metadata records for dataset: {}", rows_deleted, dataset_id);
        }
        
        Ok(rows_deleted)
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
            "INSERT INTO time_series (series_id, dataset_id, column_name, timestamp, value, created_at) 
             VALUES (?, ?, ?, ?, ?, ?)"
        )?;
        
        for record in records {
            stmt.execute(duckdb::params![
                record.series_id,
                record.dataset_id,
                record.column_name,
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
        let conn = self.conn.lock().unwrap();
        
        // For empty queries, return all series without any filtering or limiting
        if query.trim().is_empty() {
            let mut stmt = conn.prepare(r#"
                SELECT series_id, dataset_id, dataset_name, theme, description, 
                       tags_text, record_count, first_timestamp, last_timestamp
                FROM search_index
                ORDER BY record_count DESC
            "#)?;
            
            let rows = stmt.query_map([], |row| {
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
            })?;
            
            let mut results = Vec::new();
            for row in rows {
                results.push(row?);
            }
            return Ok(results);
        }
        
        // For non-empty queries, apply search filters with limit
        let search_query = format!("%{}%", query.to_lowercase());
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
    
    /// Get dataset info (total_series, total_records) by dataset_id
    pub fn get_dataset_info(&self, dataset_id: &str) -> Result<(i64, i64)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT total_series, total_records FROM datasets WHERE dataset_id = ?")?;
        
        let result = stmt.query_row(duckdb::params![dataset_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;
        
        Ok(result)
    }
    
    /// Get all series for a specific dataset by querying series_metadata directly
    /// This avoids the fake dataset entries in the search_index view
    pub fn get_series_for_dataset(&self, dataset_id: &str, limit: Option<usize>) -> Result<Vec<SearchResult>> {
        use tracing::{info, error, debug};
        
        info!("🔍 get_series_for_dataset called with dataset_id: '{}', limit: {:?}", dataset_id, limit);
        
        let conn = self.conn.lock().unwrap();
        
        let query = if let Some(limit) = limit {
            format!(r#"
                SELECT sm.series_id, sm.dataset_id, d.name as dataset_name, 
                       sm.theme, sm.description, sm.tags as tags_text, 
                       CASE 
                           WHEN sm.record_count > 0 THEN sm.record_count
                           WHEN d.total_series > 0 THEN d.total_records / d.total_series
                           ELSE 0
                       END as record_count, 
                       sm.first_timestamp, sm.last_timestamp
                FROM series_metadata sm
                LEFT JOIN datasets d ON sm.dataset_id = d.dataset_id
                WHERE sm.dataset_id = ?
                ORDER BY sm.series_id
                LIMIT {}
            "#, limit)
        } else {
            r#"
                SELECT sm.series_id, sm.dataset_id, d.name as dataset_name, 
                       sm.theme, sm.description, sm.tags as tags_text, 
                       CASE 
                           WHEN sm.record_count > 0 THEN sm.record_count
                           WHEN d.total_series > 0 THEN d.total_records / d.total_series
                           ELSE 0
                       END as record_count, 
                       sm.first_timestamp, sm.last_timestamp
                FROM series_metadata sm
                LEFT JOIN datasets d ON sm.dataset_id = d.dataset_id
                WHERE sm.dataset_id = ?
                ORDER BY sm.series_id
            "#.to_string()
        };
        
        debug!("📝 Executing query: {}", query);
        debug!("🎯 Query parameter: dataset_id = '{}'", dataset_id);
        
        let mut stmt = match conn.prepare(&query) {
            Ok(stmt) => {
                debug!("✅ Query prepared successfully");
                stmt
            },
            Err(e) => {
                error!("❌ Failed to prepare query: {}", e);
                return Err(e.into());
            }
        };
        
        let rows = match stmt.query_map([dataset_id], |row| {
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
        }) {
            Ok(rows) => {
                debug!("✅ Query executed successfully");
                rows
            },
            Err(e) => {
                error!("❌ Failed to execute query: {}", e);
                return Err(e.into());
            }
        };
        
        let mut results = Vec::new();
        let mut row_count = 0;
        for row in rows {
            match row {
                Ok(search_result) => {
                    results.push(search_result);
                    row_count += 1;
                    if row_count <= 3 {
                        debug!("📊 Row {}: series_id = '{}'", row_count, results.last().unwrap().series_id);
                    }
                },
                Err(e) => {
                    error!("❌ Error processing row {}: {}", row_count + 1, e);
                    return Err(e.into());
                }
            }
        }
        
        info!("✅ get_series_for_dataset completed: found {} series for dataset '{}'", results.len(), dataset_id);
        
        if results.is_empty() {
            error!("⚠️  No series found for dataset '{}' - this might indicate a data issue", dataset_id);
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
                      item_id_type, target_type, targets, covariates, tfc_features, total_series,
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
                targets: row.get(11)?,
                covariates: row.get(12)?,
                tfc_features: row.get(13)?,
                total_series: row.get(14)?,
                total_records: row.get(15)?,
                series_id_columns: row.get(16)?,
                avg_series_length: row.get(17)?,
                min_series_length: row.get(18)?,
                max_series_length: row.get(19)?,
                indexing_status: IndexingStatus::from(row.get::<_, String>(20)?.as_str()),
                indexing_started_at: row.get::<_, Option<i64>>(21)?
                    .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                indexing_completed_at: row.get::<_, Option<i64>>(22)?
                    .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                indexing_error: row.get(23)?,
                metadata_file_path: row.get(24)?,
                data_file_count: row.get(25)?,
                created_at: DateTime::from_timestamp(row.get::<_, i64>(26)?, 0).unwrap(),
                updated_at: DateTime::from_timestamp(row.get::<_, i64>(27)?, 0).unwrap(),
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
    /// Clean up stale "in_progress" datasets that have been running too long
    /// This fixes the issue where datasets get stuck in "in_progress" status after crashes
    pub fn cleanup_stale_in_progress(&self, max_duration_hours: i64) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        
        // Calculate cutoff time (current time - max_duration_hours)
        let cutoff_timestamp = chrono::Utc::now() - chrono::Duration::hours(max_duration_hours);
        let cutoff_timestamp_secs = cutoff_timestamp.timestamp();
        
        // Reset stale in_progress datasets to pending
        let mut stmt = conn.prepare(r#"
            UPDATE datasets 
            SET indexing_status = 'pending',
                indexing_started_at = NULL,
                indexing_error = 'Reset from stale in_progress status after ' || ? || ' hours'
            WHERE indexing_status = 'in_progress'
            AND indexing_started_at IS NOT NULL  
            AND indexing_started_at < ?
        "#)?;
        
        let rows_updated = stmt.execute([max_duration_hours.to_string(), cutoff_timestamp_secs.to_string()])?;
        
        if rows_updated > 0 {
            info!("🧹 Cleaned up {} stale in_progress datasets (older than {}h)", rows_updated, max_duration_hours);
        }
        
        Ok(rows_updated)
    }

    pub fn get_indexing_progress(&self) -> Result<IndexingProgress> {
        // First cleanup any stale in_progress datasets (older than 1 hour)
        let _ = self.cleanup_stale_in_progress(1);
        
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

    /// Get a dataset by ID with full metadata
    pub fn get_dataset_by_id(&self, dataset_id: &str) -> Result<Option<Dataset>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT dataset_id, name, description, source_bucket, source_prefix,
                   schema_version, dataset_type, source, storage_location,
                   item_id_type, target_type, targets, covariates, tfc_features, total_series,
                   total_records, series_id_columns, avg_series_length,
                   min_series_length, max_series_length, indexing_status,
                   indexing_started_at, indexing_completed_at, indexing_error,
                   metadata_file_path, data_file_count, created_at, updated_at
            FROM datasets WHERE dataset_id = ?
        "#)?;

        let mut rows = stmt.query_map([dataset_id], |row| {
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
                targets: row.get(11)?,
                covariates: row.get(12)?,
                tfc_features: row.get(13)?,
                total_series: row.get(14)?,
                total_records: row.get(15)?,
                series_id_columns: row.get(16)?,
                avg_series_length: row.get(17)?,
                min_series_length: row.get(18)?,
                max_series_length: row.get(19)?,
                indexing_status: IndexingStatus::from(row.get::<_, String>(20)?.as_str()),
                indexing_started_at: row.get::<_, Option<i64>>(21)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
                indexing_completed_at: row.get::<_, Option<i64>>(22)?.map(|ts| DateTime::from_timestamp(ts, 0).unwrap()),
                indexing_error: row.get(23)?,
                metadata_file_path: row.get(24)?,
                data_file_count: row.get(25)?,
                created_at: DateTime::from_timestamp(row.get::<_, i64>(26)?, 0).unwrap(),
                updated_at: DateTime::from_timestamp(row.get::<_, i64>(27)?, 0).unwrap(),
            })
        })?;

        match rows.next() {
            Some(Ok(dataset)) => Ok(Some(dataset)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None)
        }
    }

    /// Get column metadata for a dataset (simplified version without TFC features)
    pub fn get_dataset_column_metadata(&self, dataset_id: &str) -> Result<Option<DatasetColumnInfo>> {
        if let Some(dataset) = self.get_dataset_by_id(dataset_id)? {
            // Create basic column structure based on dataset metadata
            let mut columns = Vec::new();
            
            // Add a basic target column if we have target type info
            if let Some(target_type) = &dataset.target_type {
                columns.push(ColumnMetadata {
                    name: "target".to_string(),
                    column_type: ColumnType::Target,
                    data_type: target_type.clone(),
                    description: Some("Primary target variable for forecasting".to_string()),
                });
            }
            
            // Add basic time series identifier columns based on series_id_columns
            if let Some(series_id_json) = &dataset.series_id_columns {
                if let Ok(series_cols) = serde_json::from_str::<Vec<String>>(series_id_json) {
                    for col_name in series_cols {
                        columns.push(ColumnMetadata {
                            name: col_name.clone(),
                            column_type: ColumnType::StaticCovariate,
                            data_type: "string".to_string(),
                            description: Some("Time series identifier".to_string()),
                        });
                    }
                }
            }
            
            // NOTE: Timestamp is NOT included in column metadata as it's a special dimension
            // The timestamp will be handled separately in the YAML metadata when that's implemented
            // Timestamps are the index/dimension, not a covariate for forecasting
            
            // If we have no actual feature columns, create a minimal target column
            if columns.is_empty() {
                columns.push(ColumnMetadata {
                    name: "value".to_string(),
                    column_type: ColumnType::Target,
                    data_type: "float".to_string(),
                    description: Some("Time series values".to_string()),
                });
            }
            
            return Ok(Some(DatasetColumnInfo {
                dataset_id: dataset_id.to_string(),
                dataset_name: dataset.name.clone(),
                columns,
            }));
        }
        Ok(None)
    }
    
    /// Get actual data columns from time series for a dataset
    pub fn discover_data_columns(&self, dataset_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT DISTINCT column_name 
            FROM time_series 
            WHERE dataset_id = ?
            ORDER BY column_name
        "#)?;
        
        let rows = stmt.query_map([dataset_id], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;
        
        let mut columns = Vec::new();
        for row in rows {
            columns.push(row?);
        }
        
        Ok(columns)
    }
    
    /// Get enhanced column metadata with proper feature abstractions
    pub fn get_enhanced_dataset_column_metadata(&self, dataset_id: &str) -> Result<Option<EnhancedDatasetColumnInfo>> {
        if let Some(dataset) = self.get_dataset_by_id(dataset_id)? {
            let mut features = Vec::new();
            
            // Parse features from parsed metadata (NEW APPROACH)
            // Parse targets from metadata
            if let Some(targets_json) = &dataset.targets {
                if let Ok(targets) = serde_json::from_str::<Vec<crate::s3::NamedFieldInfo>>(targets_json) {
                    for target in targets {
                        let target_feature = FeatureMetadata {
                            name: target.name,
                            attribute: FeatureAttribute::Targets,
                            temporality: Temporality::Dynamic,
                            modality: if target.field_type == "num" { Modality::Numerical } else { Modality::Categorical },
                            scope: Scope::Local,
                            data_type: target.field_type,
                            description: Some("Target variable from metadata".to_string()),
                        };
                        
                        if target_feature.validate().is_ok() {
                            features.push(target_feature);
                        }
                    }
                }
            }
            
            // Parse covariates from metadata
            if let Some(covariates_json) = &dataset.covariates {
                // Debug logging to see what we're trying to deserialize
                debug!("Deserializing covariates JSON: {}", covariates_json);
                match serde_json::from_str::<crate::s3::Covariates>(covariates_json) {
                    Ok(covariates) => {
                        debug!("Successfully deserialized covariates: hist={}, future={}, static={}", 
                               covariates.hist.len(), covariates.future.len(), covariates.static_vars.len());
                        
                        // Historical covariates
                        for hist_cov in covariates.hist {
                        let hist_feature = FeatureMetadata {
                            name: hist_cov.name,
                            attribute: FeatureAttribute::HistoricalCovariates,
                            temporality: Temporality::Dynamic,
                            modality: if hist_cov.field_type == "num" { Modality::Numerical } else { Modality::Categorical },
                            scope: Scope::Local,
                            data_type: hist_cov.field_type,
                            description: Some("Historical covariate from metadata".to_string()),
                        };
                        
                        if hist_feature.validate().is_ok() {
                            features.push(hist_feature);
                        }
                    }
                    
                    // Future covariates
                    for future_cov in covariates.future {
                        let future_feature = FeatureMetadata {
                            name: future_cov.name,
                            attribute: FeatureAttribute::FutureCovariates,
                            temporality: Temporality::Dynamic,
                            modality: if future_cov.field_type == "num" { Modality::Numerical } else { Modality::Categorical },
                            scope: Scope::Local,
                            data_type: future_cov.field_type,
                            description: Some("Future covariate from metadata".to_string()),
                        };
                        
                        if future_feature.validate().is_ok() {
                            features.push(future_feature);
                        }
                    }
                    
                    // Static covariates
                    for static_cov in covariates.static_vars {
                        let static_feature = FeatureMetadata {
                            name: static_cov.name,
                            attribute: FeatureAttribute::StaticCovariates,
                            temporality: Temporality::Static,
                            modality: if static_cov.field_type == "num" { Modality::Numerical } else { Modality::Categorical },
                            scope: Scope::Local,
                            data_type: static_cov.field_type,
                            description: Some("Static covariate from metadata".to_string()),
                        };
                        
                        if static_feature.validate().is_ok() {
                            features.push(static_feature);
                        }
                    }
                    }
                    Err(e) => {
                        warn!("Failed to deserialize covariates JSON: {}", e);
                        debug!("Covariates JSON that failed: {}", covariates_json);
                    }
                }
            }
            
            // Create timestamp info (separate from features)
            let timestamp_info = Some(TimestampInfo {
                column_name: "timestamp".to_string(),
                data_type: "datetime".to_string(),
                frequency: None, // Will be determined from actual data
                timezone: Some("UTC".to_string()),
                description: Some("Time dimension for the series".to_string()),
            });
            
            // Fallback: if no features from metadata, discover from data columns (legacy behavior)
            if features.is_empty() {
                let data_columns = self.discover_data_columns(dataset_id)?;
                let series_id_columns: Vec<String> = if let Some(series_id_json) = &dataset.series_id_columns {
                    serde_json::from_str(series_id_json).unwrap_or_default()
                } else {
                    vec![]
                };
                
                for column_name in &data_columns {
                    // Skip timestamp column - it's handled separately
                    if column_name == "timestamp" {
                        continue;
                    }
                    
                    // Skip series ID columns
                    if series_id_columns.contains(column_name) {
                        continue;
                    }
                    
                    // Create a default target for unknown columns
                    let default_feature = FeatureMetadata {
                        name: column_name.clone(),
                        attribute: FeatureAttribute::Targets,
                        temporality: Temporality::Dynamic,
                        modality: Modality::Numerical,
                        scope: Scope::Local,
                        data_type: "float".to_string(),
                        description: Some(format!("Discovered target column: {}", column_name)),
                    };
                    
                    if default_feature.validate().is_ok() {
                        features.push(default_feature);
                    }
                }
                
                // Ultimate fallback
                if features.is_empty() {
                    let default_feature = FeatureMetadata {
                        name: "value".to_string(),
                        attribute: FeatureAttribute::Targets,
                        temporality: Temporality::Dynamic,
                        modality: Modality::Numerical,
                        scope: Scope::Local,
                        data_type: "float".to_string(),
                        description: Some("Default time series values".to_string()),
                    };
                    
                    if default_feature.validate().is_ok() {
                        features.push(default_feature);
                    }
                }
            }
            
            return Ok(Some(EnhancedDatasetColumnInfo {
                dataset_id: dataset_id.to_string(),
                dataset_name: dataset.name.clone(),
                features,
                timestamp_info,
            }));
        }
        Ok(None)
    }

    /// Get series metadata with associated dataset column information
    pub fn get_series_with_column_metadata(&self, series_id: &str) -> Result<Option<SeriesWithColumnInfo>> {
        // First get the series metadata
        let series_metadata = self.get_series_by_id(series_id)?;
        
        if let Some(series) = series_metadata {
            // Get column metadata for the dataset
            let column_info = self.get_dataset_column_metadata(&series.dataset_id)?;
            
            return Ok(Some(SeriesWithColumnInfo {
                series_metadata: series,
                column_info,
            }));
        }
        
        Ok(None)
    }

    /// Get series metadata by ID
    pub fn get_series_by_id(&self, series_id: &str) -> Result<Option<SeriesMetadata>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT series_id, dataset_id, theme, description, tags, source_file,
                   first_timestamp, last_timestamp, record_count, created_at
            FROM series_metadata WHERE series_id = ?
        "#)?;

        let mut rows = stmt.query_map([series_id], |row| {
            Ok(SeriesMetadata {
                series_id: row.get(0)?,
                dataset_id: row.get(1)?,
                theme: row.get(2)?,
                description: row.get(3)?,
                tags: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                source_file: row.get(5)?,
                first_timestamp: DateTime::from_timestamp(row.get::<_, i64>(6)?, 0).unwrap(),
                last_timestamp: DateTime::from_timestamp(row.get::<_, i64>(7)?, 0).unwrap(),
                record_count: row.get(8)?,
                created_at: DateTime::from_timestamp(row.get::<_, i64>(9)?, 0).unwrap(),
            })
        })?;

        match rows.next() {
            Some(Ok(series)) => Ok(Some(series)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None)
        }
    }

    /// Get time series data points for plotting with S3 fallback
    pub async fn get_time_series_data_with_fallback(&self, series_id: &str, column_name: &str, limit: Option<usize>) -> Result<Vec<TimeSeriesPoint>> {
        // First try local cache
        let local_data = self.get_time_series_data(series_id, column_name, limit.clone())?;
        
        // If no data found locally, try S3 fallback
        if local_data.is_empty() {
            info!("No local data found for series {} column {}, attempting S3 fallback", series_id, column_name);
            let fallback_data = self.fetch_and_cache_parquet_file(series_id, column_name).await?;
            
            // Apply limit if specified
            if let Some(limit) = limit {
                Ok(fallback_data.into_iter().take(limit).collect())
            } else {
                Ok(fallback_data)
            }
        } else {
            Ok(local_data)
        }
    }

    /// Get time series data points for plotting (local cache only)
    pub fn get_time_series_data(&self, series_id: &str, column_name: &str, limit: Option<usize>) -> Result<Vec<TimeSeriesPoint>> {
        let conn = self.conn.lock().unwrap();
        
        let query = if let Some(limit) = limit {
            format!(r#"
                SELECT timestamp, value
                FROM time_series 
                WHERE series_id = ? AND column_name = ?
                ORDER BY timestamp
                LIMIT {}
            "#, limit)
        } else {
            r#"
                SELECT timestamp, value
                FROM time_series 
                WHERE series_id = ? AND column_name = ?
                ORDER BY timestamp
            "#.to_string()
        };

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map([series_id, column_name], |row| {
            Ok(TimeSeriesPoint {
                timestamp: DateTime::from_timestamp(row.get::<_, i64>(0)?, 0).unwrap(),
                value: row.get::<_, f64>(1)?,
            })
        })?;

        let mut data_points = Vec::new();
        for row in rows {
            data_points.push(row?);
        }

        Ok(data_points)
    }

    /// Check if a parquet file has been cached
    pub fn is_file_cached(&self, file_path: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT 1 FROM cached_files WHERE file_path = ?")?;
        let exists = stmt.exists([file_path])?;
        Ok(exists)
    }

    /// Mark a file as cached
    pub fn mark_file_cached(&self, file_path: &str, dataset_id: &str, file_size: i64, series_count: i32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "INSERT OR REPLACE INTO cached_files (file_path, dataset_id, cached_at, file_size, series_count) 
             VALUES (?, ?, ?, ?, ?)"
        )?;
        
        stmt.execute(duckdb::params![
            file_path,
            dataset_id,
            Utc::now().timestamp(),
            file_size,
            series_count
        ])?;
        
        Ok(())
    }

    /// Extract time series data from a Polars DataFrame
    fn extract_series_data_from_dataframe(
        &self,
        df: &polars::prelude::DataFrame,
        timestamp_col: &str,
        series_id_cols: &[String],
        data_columns: &[&str],
        dataset_id: &str,
    ) -> Result<Vec<TimeSeriesRecord>> {
        use polars::prelude::*;
        let mut time_series_records = Vec::new();
        let height = df.height();
        
        for row_idx in 0..height {
            // Extract timestamp
            let timestamp_value = df.column(timestamp_col)?.get(row_idx)?;
            let timestamp = match timestamp_value {
                AnyValue::Int64(ts) => DateTime::from_timestamp(ts, 0).unwrap_or_else(|| Utc::now()),
                AnyValue::Datetime(ts, _, _) => {
                    // Convert nanoseconds to seconds and nanoseconds
                    let seconds = ts / 1_000_000_000;
                    let nanoseconds = (ts % 1_000_000_000) as u32;
                    if row_idx < 3 {
                        info!("🔍 DEBUG timestamp conversion: raw={}, seconds={}, ns={}", ts, seconds, nanoseconds);
                    }
                    DateTime::from_timestamp(seconds, nanoseconds).unwrap_or_else(|| {
                        warn!("Failed to convert timestamp: seconds={}, nanoseconds={}", seconds, nanoseconds);
                        Utc::now()
                    })
                },
                _ => {
                    warn!("Unexpected timestamp format: {:?}, using current time", timestamp_value);
                    Utc::now()
                }
            };

            // Extract series ID from parquet file (composite if multiple columns)  
            let parquet_series_id = if series_id_cols.len() == 1 {
                df.column(&series_id_cols[0])?.get(row_idx)?.to_string()
            } else if !series_id_cols.is_empty() {
                // Create composite key
                series_id_cols.iter()
                    .map(|col| df.column(col).unwrap().get(row_idx).unwrap().to_string())
                    .collect::<Vec<_>>()
                    .join("::")
            } else {
                // Fallback to generic series ID
                "series_1".to_string()
            };
            
            // Create full series ID in database format: dataset_name:series_id
            let current_series_id = format!("{}:{}", dataset_id, parquet_series_id);

            // Extract all data columns
            for data_col in data_columns {
                let value = df.column(data_col)?.get(row_idx)?;
                let numeric_value = match value {
                    AnyValue::Float64(v) => Some(v),
                    AnyValue::Float32(v) => Some(v as f64),
                    AnyValue::Int64(v) => Some(v as f64),
                    AnyValue::Int32(v) => Some(v as f64),
                    AnyValue::UInt64(v) => Some(v as f64),
                    AnyValue::UInt32(v) => Some(v as f64),
                    AnyValue::Int16(v) => Some(v as f64),
                    AnyValue::UInt16(v) => Some(v as f64),
                    AnyValue::Int8(v) => Some(v as f64),
                    AnyValue::UInt8(v) => Some(v as f64),
                    AnyValue::Null => None, // Skip null values
                    _ => {
                        // Try to parse string representation
                        let str_val = value.to_string();
                        str_val.parse::<f64>().ok()
                    }
                };
                
                if let Some(v) = numeric_value {
                    time_series_records.push(TimeSeriesRecord {
                        id: Uuid::new_v4(),
                        series_id: current_series_id.clone(),
                        dataset_id: dataset_id.to_string(),
                        column_name: data_col.to_string(),
                        timestamp,
                        value: v,
                        created_at: Utc::now(),
                    });
                }
            }
        }
        
        Ok(time_series_records)
    }

    /// Cache all series from a DataFrame (for efficiency when processing large files)
    fn cache_all_series_from_dataframe(
        &self,
        df: &polars::prelude::DataFrame,
        timestamp_col: &str,
        series_id_cols: &[String],
        data_columns: &[&str],
        dataset_id: &str,
    ) -> Result<()> {
        let records = self.extract_series_data_from_dataframe(df, timestamp_col, series_id_cols, data_columns, dataset_id)?;
        if !records.is_empty() {
            self.batch_insert_time_series(&records)?;
        }
        Ok(())
    }

    /// Fetch and cache time series data from S3 parquet file
    pub async fn fetch_and_cache_parquet_file(&self, series_id: &str, column_name: &str) -> Result<Vec<TimeSeriesPoint>> {
        // Extract raw series ID (remove dataset prefix)
        // series_id format: "dataset_name:raw_id" -> we want just "raw_id"
        let raw_series_id = if let Some(colon_pos) = series_id.find(':') {
            &series_id[colon_pos + 1..]
        } else {
            series_id // fallback if no colon found
        };
        
        info!("Looking for raw series ID '{}' (from full series ID '{}')", raw_series_id, series_id);
        // 1. Get series metadata to find source file
        let series_with_column_info = self.get_series_with_column_metadata(series_id)?;
        let series_with_column_info = series_with_column_info.ok_or_else(|| {
            anyhow::anyhow!("Series not found in metadata: {}", series_id)
        })?;
        let series_metadata = &series_with_column_info.series_metadata;

        // 2. Check if file is already cached
        if self.is_file_cached(&series_metadata.source_file)? {
            info!("File already cached, querying local database: {}", series_metadata.source_file);
            return self.get_time_series_data(series_id, column_name, None);
        }

        // 3. Get dataset info for S3 details
        let dataset = self.get_dataset_by_id(&series_metadata.dataset_id)?;
        let dataset = dataset.ok_or_else(|| {
            anyhow::anyhow!("Dataset not found: {}", series_metadata.dataset_id)
        })?;

        // 4. Prepare S3 key
        // The source_file already contains the full path, so use it directly
        let full_s3_key = series_metadata.source_file.trim_start_matches('/');

        info!("Cache miss - downloading parquet file from S3: {}", series_metadata.source_file);
        println!("⬇️  Downloading data from S3...");
        println!("   📁 Dataset: {}", series_metadata.dataset_id);
        println!("   📄 File: {}", series_metadata.source_file);
        println!("   🌐 S3 Key: {}", full_s3_key);

        // 5. Initialize S3 client (default to eu-west-3 region)
        let s3_client = crate::s3::S3Client::new(dataset.source_bucket.clone(), "eu-west-3".to_string()).await
            .context("Failed to initialize S3 client - check AWS credentials")?;

        // 6. Download parquet file
        let parquet_data = s3_client.download_object(&full_s3_key).await
            .context(format!("Failed to download parquet file: {}", full_s3_key))?;

        info!("Downloaded {} bytes from S3: {}", parquet_data.len(), full_s3_key);

        // 7. Read parquet with Polars
        let cursor = Cursor::new(parquet_data.clone());
        let df = polars::io::parquet::ParquetReader::new(cursor)
            .finish()
            .context("Failed to read parquet file")?;

        info!("Read parquet file with {} rows and {} columns", df.height(), df.width());

        // 8. Extract all time series data from the file
        let mut time_series_records: Vec<TimeSeriesRecord> = Vec::new();
        let columns = df.get_column_names();
        
        info!("Parquet file columns: {:?}", columns);
        
        // Find timestamp column (assuming it's named 'timestamp' or 'date' or similar)
        let timestamp_col = columns.iter()
            .find(|&col| col.to_lowercase().contains("timestamp") || col.to_lowercase().contains("date") || col.to_lowercase().contains("time"))
            .ok_or_else(|| anyhow::anyhow!("No timestamp column found in parquet file. Available columns: {:?}", columns))?;

        // Get series ID columns from dataset metadata (don't guess!)
        let series_id_cols: Vec<String> = if let Some(series_id_json) = &dataset.series_id_columns {
            serde_json::from_str(series_id_json).unwrap_or_else(|_| vec!["item_id".to_string()])
        } else {
            vec!["item_id".to_string()] // fallback
        };
            
        info!("Detected timestamp column: {}", timestamp_col);
        info!("Detected series ID columns: {:?}", series_id_cols);

        // Get all numerical columns (excluding timestamp and ID columns)
        let data_columns: Vec<&str> = columns.iter()
            .filter(|&col| {
                let col_lower = col.to_lowercase();
                !col_lower.contains("timestamp") && 
                !col_lower.contains("date") && 
                !col_lower.contains("time") &&
                !series_id_cols.iter().any(|id_col| id_col.to_lowercase() == col_lower)
            })
            .cloned()
            .collect();
            
        info!("Detected data columns: {:?}", data_columns);
        
        // Parse raw series ID components using metadata types
        // Get the field type from metadata to determine correct format
        let item_id_is_string = true; // Based on metadata: item_id type: str, subtype: str
        
        let raw_series_parts: Vec<String> = if raw_series_id.contains("::") {
            // Composite key format: "part1::part2::part3"
            raw_series_id.split("::").map(|s| s.trim_matches('"').to_string()).collect()
        } else if item_id_is_string {
            // item_id is stored as string in parquet, but WITHOUT quotes
            // Database: beijing_subway_30min:"58" -> raw_series_id: "58" -> parquet: 58 (no quotes!)
            vec![raw_series_id.trim_matches('"').to_string()] // Strip quotes: "58" -> 58
        } else {
            // For integer types, try both formats
            let without_quotes = raw_series_id.trim_matches('"');
            vec![
                without_quotes.to_string(), // Remove quotes: "58" -> 58
                raw_series_id.to_string(), // Keep as-is: "58"
            ]
        };
        
        info!("Looking for raw series parts: {:?}", raw_series_parts);
        
        // Debug: Show a sample of actual data in the parquet file to understand the format
        info!("🔍 DEBUG: Showing first 5 rows of parquet data for debugging:");
        for i in 0..std::cmp::min(5, df.height()) {
            let item_id = df.column("item_id").unwrap().get(i).unwrap();
            let timestamp = df.column("timestamp").unwrap().get(i).unwrap();
            let in_flow = df.column("in_flow").unwrap().get(i).unwrap();
            info!("  Row {}: item_id={:?}, timestamp={:?}, in_flow={:?}", i, item_id, timestamp, in_flow);
        }
        
        // Build filter expression for the specific series we want
        use polars::prelude::*;
        let filter_expr = if series_id_cols.len() == 1 && raw_series_parts.len() == 1 {
            // Simple case: single series ID column with exact string match
            col(&series_id_cols[0]).eq(lit(raw_series_parts[0].clone()))
        } else if series_id_cols.len() == 1 {
            // Try multiple values for single column
            let mut expr = col(&series_id_cols[0]).eq(lit(raw_series_parts[0].clone()));
            for i in 1..raw_series_parts.len() {
                expr = expr.or(col(&series_id_cols[0]).eq(lit(raw_series_parts[i].clone())));
            }
            expr
        } else if series_id_cols.len() == raw_series_parts.len() {
            // Composite key: match all parts
            let mut expr = col(&series_id_cols[0]).eq(lit(raw_series_parts[0].clone()));
            for (i, col_name) in series_id_cols.iter().enumerate().skip(1) {
                if i < raw_series_parts.len() {
                    expr = expr.and(col(col_name).eq(lit(raw_series_parts[i].clone())));
                }
            }
            expr
        } else {
            // Fallback: try to match first component to first column
            col(&series_id_cols[0]).eq(lit(raw_series_parts[0].clone()))
        };
        
        // Debug: Log the filter expression
        info!("🔍 DEBUG: Filter expression: Filtering {} column for values: {:?}", 
              &series_id_cols[0], &raw_series_parts);
        
        // Filter DataFrame using lazy evaluation
        let filtered_df = df.clone().lazy()
            .filter(filter_expr)
            .collect()
            .context("Failed to filter DataFrame by series ID")?;
        
        info!("Filtered to {} rows for target series", filtered_df.height());
        
        // Debug: If no rows found, show what values actually exist in the series ID column
        if filtered_df.height() == 0 {
            info!("🔍 DEBUG: No rows found after filtering. Showing unique values in '{}' column:", &series_id_cols[0]);
            let unique_values = df.column(&series_id_cols[0]).unwrap()
                .unique().unwrap()
                .slice(0, 20) // Show first 20 unique values
                .iter()
                .map(|v| format!("{:?}", v))
                .collect::<Vec<_>>();
            info!("  Unique values (first 20): {:?}", unique_values);
            info!("  Looking for: {:?}", &raw_series_parts);
        }
        
        // Process the filtered data + cache all data for efficiency
        let mut time_series_records = Vec::new();
        
        if filtered_df.height() > 0 {
            // Extract data for the target series (what user requested)
            let target_records = self.extract_series_data_from_dataframe(
                &filtered_df, 
                timestamp_col, 
                &series_id_cols, 
                &data_columns, 
                &series_metadata.dataset_id
            )?;
            time_series_records.extend(target_records);
            info!("Extracted {} records for target series", time_series_records.len());
        }
        
        // For now, skip full file caching to get fast results
        // TODO: Implement background caching or smaller threshold
        info!("Skipping full file caching for faster response - caching only target series");

        info!("Total records to cache: {}", time_series_records.len());

        // 9. Batch insert all records
        if !time_series_records.is_empty() {
            self.batch_insert_time_series(&time_series_records)?;
        }

        // 10. Mark file as cached
        let unique_series_count = time_series_records.iter()
            .map(|r| &r.series_id)
            .collect::<std::collections::HashSet<_>>()
            .len() as i32;
        
        self.mark_file_cached(&series_metadata.source_file, &series_metadata.dataset_id, 
                             parquet_data.len() as i64, unique_series_count)?;

        info!("Successfully cached {} series from file: {}", unique_series_count, series_metadata.source_file);
        println!("✅ Cached {} series from {} ({} MB)", 
                unique_series_count, 
                series_metadata.source_file,
                parquet_data.len() / 1_000_000);

        // 11. Return the requested data directly from memory instead of database
        // Convert TimeSeriesRecord to TimeSeriesPoint for the requested column
        let final_data: Vec<TimeSeriesPoint> = time_series_records
            .into_iter()
            .filter(|record| record.series_id == series_id && record.column_name == column_name)
            .map(|record| TimeSeriesPoint {
                timestamp: record.timestamp,
                value: record.value,
            })
            .collect();
        
        // Debug: Show first few data points to verify timestamp conversion
        if !final_data.is_empty() {
            info!("🔍 DEBUG: First 3 direct timestamps (bypassing DB):");
            for (i, point) in final_data.iter().take(3).enumerate() {
                info!("  Point {}: timestamp={}, value={}", i, point.timestamp.format("%Y-%m-%d %H:%M:%S"), point.value);
            }
        }
        
        if final_data.is_empty() {
            warn!("Series {} column {} not found in processed data from file {}", series_id, column_name, series_metadata.source_file);
            return Err(anyhow::anyhow!(
                "Time series data not found for series '{}' column '{}' in file '{}'", 
                series_id, column_name, series_metadata.source_file
            ));
        }
        Ok(final_data)
    }
    
    /// Find datasets where all or most series have record_count = 0
    pub fn find_datasets_with_zero_record_counts(&self) -> Result<Vec<(String, i64, i64)>> {
        use tracing::{info, debug};
        
        info!("🔍 Finding datasets with record_count = 0 issues...");
        
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(r#"
            SELECT 
                sm.dataset_id,
                COUNT(*) as total_series,
                COUNT(CASE WHEN sm.record_count = 0 THEN 1 END) as zero_count_series
            FROM series_metadata sm
            GROUP BY sm.dataset_id
            HAVING zero_count_series > 0
            ORDER BY zero_count_series DESC, total_series DESC
        "#)?;
        
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        
        let mut results = Vec::new();
        for row in rows {
            let (dataset_id, total_series, zero_count) = row?;
            debug!("Dataset '{}': {}/{} series have record_count = 0", dataset_id, zero_count, total_series);
            results.push((dataset_id, total_series, zero_count));
        }
        
        info!("Found {} datasets with record_count = 0 issues", results.len());
        Ok(results)
    }
    
    /// Update series record counts using dataset totals for a specific dataset
    pub fn update_series_record_counts_from_dataset_totals(&self, dataset_id: &str) -> Result<usize> {
        use tracing::{info, warn, debug};
        
        info!("🔧 Repairing record counts for dataset: '{}'", dataset_id);
        
        // First, get dataset information
        let (total_series, total_records) = self.get_dataset_info(dataset_id)?;
        
        if total_series == 0 {
            warn!("Dataset '{}' has total_series = 0, cannot calculate expected record count", dataset_id);
            return Ok(0);
        }
        
        let expected_records_per_series = total_records / total_series;
        info!("Expected records per series: {} (total_records: {}, total_series: {})", 
              expected_records_per_series, total_records, total_series);
        
        // Update all series with record_count = 0 for this dataset
        let conn = self.conn.lock().unwrap();
        let updated_count = conn.execute(
            r#"UPDATE series_metadata 
               SET record_count = ?
               WHERE dataset_id = ? AND record_count = 0"#,
            duckdb::params![expected_records_per_series, dataset_id],
        )?;
        
        info!("✅ Updated {} series record counts for dataset '{}' (set to {} records each)", 
              updated_count, dataset_id, expected_records_per_series);
        
        Ok(updated_count)
    }
    
    /// Repair record counts for all datasets with zero record count issues
    pub fn repair_all_series_record_counts(&self) -> Result<(usize, usize)> {
        use tracing::{info, warn};
        
        info!("🔧 Starting bulk repair of series record counts...");
        
        let datasets_with_issues = self.find_datasets_with_zero_record_counts()?;
        let mut total_datasets_fixed = 0;
        let mut total_series_fixed = 0;
        
        for (dataset_id, total_series, zero_count) in datasets_with_issues {
            info!("Repairing dataset '{}': {}/{} series need fixing", dataset_id, zero_count, total_series);
            
            match self.update_series_record_counts_from_dataset_totals(&dataset_id) {
                Ok(series_updated) => {
                    total_datasets_fixed += 1;
                    total_series_fixed += series_updated;
                    info!("✅ Fixed {} series in dataset '{}'", series_updated, dataset_id);
                }
                Err(e) => {
                    warn!("❌ Failed to fix dataset '{}': {}", dataset_id, e);
                }
            }
        }
        
        info!("🎉 Repair completed: {} datasets fixed, {} series updated", 
              total_datasets_fixed, total_series_fixed);
        
        Ok((total_datasets_fixed, total_series_fixed))
    }

    /// Get dataset progress details (stub implementation)
    pub fn get_dataset_progress_details(&self) -> Result<Vec<(String, i64, i64)>> {
        // Stub implementation - returns empty vec for now
        Ok(vec![])
    }

    /// Get sample data from a dataset (stub implementation)
    pub fn get_sample_data(&self, dataset_id: &str, limit: usize) -> Result<Vec<serde_json::Value>> {
        // Stub implementation - returns empty vec for now
        let _ = (dataset_id, limit); // Suppress unused parameter warnings
        Ok(vec![])
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

/// Column metadata for hierarchical browsing (legacy support)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub column_type: ColumnType,
    pub data_type: String,
    pub description: Option<String>,
}

/// Enhanced dataset column information with proper feature abstractions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedDatasetColumnInfo {
    pub dataset_id: String,
    pub dataset_name: String,
    pub features: Vec<FeatureMetadata>,
    pub timestamp_info: Option<TimestampInfo>,
}

/// Timestamp dimension information (separate from features)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampInfo {
    pub column_name: String,
    pub data_type: String,
    pub frequency: Option<String>, // e.g., "daily", "hourly", "monthly"
    pub timezone: Option<String>,
    pub description: Option<String>,
}

/// Feature temporality classification (based on navi repository abstractions)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Temporality {
    Static,
    Dynamic,
}

/// Feature modality classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Modality {
    Categorical,
    Numerical,
}

/// Feature scope classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Scope {
    Local,
    Global,
}

/// Feature attributes - the core classification system
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FeatureAttribute {
    Targets,
    HistoricalCovariates,
    FutureCovariates,
    StaticCovariates,
}

/// Types of columns available in datasets (backward compatibility)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Target,
    HistoricalCovariate,
    StaticCovariate,
    FutureCovariate,
    Timestamp, // Special dimension - not a covariate but the time index
}

/// Enhanced feature metadata with full classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureMetadata {
    pub name: String,
    pub attribute: FeatureAttribute,
    pub temporality: Temporality,
    pub modality: Modality,
    pub scope: Scope,
    pub data_type: String,
    pub description: Option<String>,
}

impl FeatureMetadata {
    /// Convert to legacy ColumnType for backward compatibility
    pub fn to_column_type(&self) -> ColumnType {
        match self.attribute {
            FeatureAttribute::Targets => ColumnType::Target,
            FeatureAttribute::HistoricalCovariates => ColumnType::HistoricalCovariate,
            FeatureAttribute::FutureCovariates => ColumnType::FutureCovariate,
            FeatureAttribute::StaticCovariates => ColumnType::StaticCovariate,
        }
    }
    
    /// Validate feature consistency based on business rules
    pub fn validate(&self) -> Result<(), String> {
        match (&self.attribute, &self.temporality) {
            (FeatureAttribute::StaticCovariates, Temporality::Dynamic) => {
                Err("Static covariates cannot have dynamic temporality".to_string())
            }
            (FeatureAttribute::HistoricalCovariates, Temporality::Static) => {
                Err("Historical covariates must have dynamic temporality".to_string())
            }
            (FeatureAttribute::FutureCovariates, Temporality::Static) => {
                Err("Future covariates must have dynamic temporality".to_string())
            }
            _ => Ok(())
        }
    }
}

/// Dataset column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetColumnInfo {
    pub dataset_id: String,
    pub dataset_name: String,
    pub columns: Vec<ColumnMetadata>,
}

/// Series metadata with associated column information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesWithColumnInfo {
    pub series_metadata: SeriesMetadata,
    pub column_info: Option<DatasetColumnInfo>,
}

/// Time series data point for plotting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
}