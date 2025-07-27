use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Lock-free progress tracking using atomic counters
/// Follows ChatGPT best practices: simple, fast, observable without blocking
#[derive(Clone)]
pub struct IndexerProgress {
    /// Atomic counter for completed datasets
    completed: Arc<AtomicUsize>,
    /// Atomic counter for failed datasets  
    failed: Arc<AtomicUsize>,
    /// Total number of datasets to process
    total: usize,
    /// When indexing started
    started_at: DateTime<Utc>,
    /// Process ID for liveness detection
    indexer_pid: u32,
    /// Last time we wrote state to file
    last_file_write: Arc<std::sync::Mutex<Instant>>,
}

/// Serializable state for file persistence
#[derive(Debug, Serialize, Deserialize)]
pub struct ProgressState {
    pub indexer_pid: u32,
    pub started_at: DateTime<Utc>,
    pub last_updated: DateTime<Utc>,
    pub total_datasets: usize,
    pub completed_count: usize,
    pub failed_count: usize,
    pub processing_rate_per_min: f64,
    pub estimated_completion_time: Option<DateTime<Utc>>,
}

impl IndexerProgress {
    const STATE_FILE: &'static str = "ts_indexer_progress.json";
    const STATE_FILE_TEMP: &'static str = "ts_indexer_progress.json.tmp";
    
    /// Create new progress tracker
    pub fn new(total_datasets: usize) -> Self {
        Self {
            completed: Arc::new(AtomicUsize::new(0)),
            failed: Arc::new(AtomicUsize::new(0)),
            total: total_datasets,
            started_at: Utc::now(),
            indexer_pid: std::process::id(),
            last_file_write: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }
    
    /// Increment completed counter and return new count
    pub fn increment_completed(&self) -> usize {
        let new_count = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
        self.write_state_if_needed();
        debug!("Progress: {}/{} datasets completed", new_count, self.total);
        new_count
    }
    
    /// Increment failed counter and return new count
    pub fn increment_failed(&self) -> usize {
        let new_count = self.failed.fetch_add(1, Ordering::Relaxed) + 1;
        self.write_state_if_needed();
        debug!("Progress: {} datasets failed", new_count);
        new_count
    }
    
    /// Get current completed count (lock-free read)
    pub fn completed_count(&self) -> usize {
        self.completed.load(Ordering::Relaxed)
    }
    
    /// Get current failed count (lock-free read)
    pub fn failed_count(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }
    
    /// Get pending count (calculated)
    pub fn pending_count(&self) -> usize {
        let completed = self.completed_count();
        let failed = self.failed_count();
        self.total.saturating_sub(completed + failed)
    }
    
    /// Get total datasets
    pub fn total_count(&self) -> usize {
        self.total
    }
    
    /// Get completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.completed_count() as f64 / self.total as f64) * 100.0
        }
    }
    
    /// Calculate processing rate (datasets per minute)
    pub fn processing_rate_per_min(&self) -> f64 {
        let elapsed = Utc::now().signed_duration_since(self.started_at);
        let elapsed_minutes = elapsed.num_minutes() as f64;
        
        if elapsed_minutes > 0.0 {
            self.completed_count() as f64 / elapsed_minutes
        } else {
            0.0
        }
    }
    
    /// Estimate completion time based on current rate
    pub fn estimated_completion_time(&self) -> Option<DateTime<Utc>> {
        let rate = self.processing_rate_per_min();
        let remaining = self.pending_count();
        
        if rate > 0.0 && remaining > 0 {
            let minutes_remaining = remaining as f64 / rate;
            Some(Utc::now() + chrono::Duration::minutes(minutes_remaining as i64))
        } else {
            None
        }
    }
    
    /// Write state to file if conditions are met (avoid I/O bottleneck)
    /// Following ChatGPT advice: don't write on every update
    fn write_state_if_needed(&self) {
        let completed = self.completed_count();
        let should_write_count = completed > 0 && completed % 10 == 0; // Every 10 completions
        
        let should_write_time = {
            if let Ok(last_write) = self.last_file_write.lock() {
                last_write.elapsed() > Duration::from_secs(30) // Every 30 seconds
            } else {
                false
            }
        };
        
        if should_write_count || should_write_time {
            if let Err(e) = self.write_state_to_file() {
                warn!("Failed to write progress state: {}", e);
            }
        }
    }
    
    /// Force write current state to file
    pub fn write_state_to_file(&self) -> Result<()> {
        let state = ProgressState {
            indexer_pid: self.indexer_pid,
            started_at: self.started_at,
            last_updated: Utc::now(),
            total_datasets: self.total,
            completed_count: self.completed_count(),
            failed_count: self.failed_count(),
            processing_rate_per_min: self.processing_rate_per_min(),
            estimated_completion_time: self.estimated_completion_time(),
        };
        
        // Atomic write pattern: write to temp file, then rename
        let json = serde_json::to_string_pretty(&state)?;
        std::fs::write(Self::STATE_FILE_TEMP, json)?;
        std::fs::rename(Self::STATE_FILE_TEMP, Self::STATE_FILE)?;
        
        // Update last write time
        if let Ok(mut last_write) = self.last_file_write.lock() {
            *last_write = Instant::now();
        }
        
        debug!("Progress state written to file: {}/{} completed", 
               state.completed_count, state.total_datasets);
        Ok(())
    }
    
    /// Clean up progress file on shutdown
    pub fn cleanup_on_shutdown(&self) {
        if let Err(e) = std::fs::remove_file(Self::STATE_FILE) {
            debug!("Could not remove progress file: {}", e);
        }
        debug!("Progress tracking cleaned up for PID {}", self.indexer_pid);
    }
}

impl ProgressState {
    /// Read progress state from file
    pub fn read_from_file() -> Result<Option<ProgressState>> {
        match std::fs::read_to_string(IndexerProgress::STATE_FILE) {
            Ok(content) => {
                let state: ProgressState = serde_json::from_str(&content)?;
                Ok(Some(state))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    
    /// Check if the indexer process is still alive
    pub fn is_indexer_alive(&self) -> bool {
        // Use kill -0 to check if process exists (doesn't actually kill)
        std::process::Command::new("kill")
            .args(["-0", &self.indexer_pid.to_string()])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
    
    /// Check if the progress state is stale (old timestamp)
    pub fn is_stale(&self, max_age_minutes: u64) -> bool {
        let now = Utc::now();
        let age = now.signed_duration_since(self.last_updated);
        age.num_minutes() > max_age_minutes as i64
    }
    
    /// Get a human-readable duration string
    pub fn elapsed_time(&self) -> String {
        let elapsed = Utc::now().signed_duration_since(self.started_at);
        let total_seconds = elapsed.num_seconds();
        
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
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_atomic_progress_tracking() {
        let progress = IndexerProgress::new(100);
        
        assert_eq!(progress.completed_count(), 0);
        assert_eq!(progress.failed_count(), 0);
        assert_eq!(progress.pending_count(), 100);
        assert_eq!(progress.total_count(), 100);
        
        // Test increment operations
        assert_eq!(progress.increment_completed(), 1);
        assert_eq!(progress.increment_failed(), 1);
        
        assert_eq!(progress.pending_count(), 98);
        assert_eq!(progress.completion_percentage(), 1.0);
    }
    
    #[test]
    fn test_processing_rate_calculation() {
        let progress = IndexerProgress::new(100);
        
        // Initially should be 0 (no time elapsed)
        assert_eq!(progress.processing_rate_per_min(), 0.0);
        
        // After some progress, rate should be calculable
        progress.increment_completed();
        // Rate will be very high due to small time elapsed, but should not panic
        assert!(progress.processing_rate_per_min() >= 0.0);
    }
}