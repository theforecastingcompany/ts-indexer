use sysinfo::{System, Pid};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tracing::{info, debug};

/// Resource monitoring utilities for performance diagnostics
pub struct ResourceMonitor {
    system: Arc<std::sync::Mutex<System>>,
    process_pid: Pid,
}

/// Snapshot of system resources at a point in time
#[derive(Debug, Clone)]
pub struct ResourceSnapshot {
    pub timestamp: Instant,
    pub total_memory_mb: u64,
    pub used_memory_mb: u64,
    pub process_memory_mb: u64,
    pub process_virtual_memory_mb: u64,
    pub total_threads: usize,
    pub process_threads: usize,
    pub cpu_usage_percent: f32,
    pub available_cores: usize,
}

/// Performance metrics for an operation
#[derive(Debug, Clone)]
pub struct OperationMetrics {
    pub operation_name: String,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub start_snapshot: ResourceSnapshot,
    pub end_snapshot: Option<ResourceSnapshot>,
}

impl ResourceMonitor {
    /// Create a new resource monitor
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();
        
        let process_pid = sysinfo::get_current_pid().expect("Failed to get current process PID");
        
        Self {
            system: Arc::new(std::sync::Mutex::new(system)),
            process_pid,
        }
    }
    
    /// Take a snapshot of current system resources
    pub fn snapshot(&self) -> ResourceSnapshot {
        let mut system = self.system.lock().unwrap();
        system.refresh_all();
        
        // System-wide metrics
        let total_memory_mb = system.total_memory() / 1024 / 1024;
        let used_memory_mb = system.used_memory() / 1024 / 1024;
        let available_cores = num_cpus::get();
        
        // Process-specific metrics
        let process = system.process(self.process_pid);
        let (process_memory_mb, process_virtual_memory_mb, process_threads, cpu_usage_percent) = 
            if let Some(proc) = process {
                (
                    proc.memory() / 1024 / 1024,
                    proc.virtual_memory() / 1024 / 1024,
                    1, // Use fixed thread count estimate since tasks() API changed
                    proc.cpu_usage(),
                )
            } else {
                (0, 0, 0, 0.0)
            };
        
        // Total system threads (approximate count)
        let total_threads = system.processes().len(); // Use process count as proxy for thread estimate
        
        ResourceSnapshot {
            timestamp: Instant::now(),
            total_memory_mb,
            used_memory_mb,
            process_memory_mb,
            process_virtual_memory_mb,
            total_threads,
            process_threads,
            cpu_usage_percent,
            available_cores,
        }
    }
    
    /// Start monitoring an operation
    pub fn start_operation(&self, operation_name: String) -> OperationMetrics {
        let start_snapshot = self.snapshot();
        
        debug!("[PERF] {}_start | mem_mb={} | vmem_mb={} | threads={} | sys_threads={} | cpu={}% | time={:?}", 
               operation_name,
               start_snapshot.process_memory_mb,
               start_snapshot.process_virtual_memory_mb,
               start_snapshot.process_threads,
               start_snapshot.total_threads,
               start_snapshot.cpu_usage_percent,
               start_snapshot.timestamp);
        
        OperationMetrics {
            operation_name,
            start_time: Instant::now(),
            end_time: None,
            start_snapshot,
            end_snapshot: None,
        }
    }
    
    /// Finish monitoring an operation and log results
    pub fn finish_operation(&self, mut metrics: OperationMetrics) -> OperationMetrics {
        let end_snapshot = self.snapshot();
        let end_time = Instant::now();
        let duration = end_time.duration_since(metrics.start_time);
        
        // Calculate deltas
        let mem_delta_mb = end_snapshot.process_memory_mb as i64 - metrics.start_snapshot.process_memory_mb as i64;
        let vmem_delta_mb = end_snapshot.process_virtual_memory_mb as i64 - metrics.start_snapshot.process_virtual_memory_mb as i64;
        let thread_delta = end_snapshot.process_threads as i64 - metrics.start_snapshot.process_threads as i64;
        
        info!("[PERF] {}_end | duration_ms={} | mem_delta_mb={:+} | vmem_delta_mb={:+} | thread_delta={:+} | peak_mem_mb={} | cpu={}%", 
              metrics.operation_name,
              duration.as_millis(),
              mem_delta_mb,
              vmem_delta_mb,
              thread_delta,
              end_snapshot.process_memory_mb,
              end_snapshot.cpu_usage_percent);
        
        metrics.end_time = Some(end_time);
        metrics.end_snapshot = Some(end_snapshot);
        metrics
    }
    
    /// Log a milestone within an operation
    pub fn log_milestone(&self, operation_name: &str, milestone: &str, additional_info: &str) {
        let snapshot = self.snapshot();
        
        debug!("[PERF] {}_{} | mem_mb={} | threads={} | {} | time={:?}", 
               operation_name,
               milestone,
               snapshot.process_memory_mb,
               snapshot.process_threads,
               additional_info,
               snapshot.timestamp);
    }
    
    /// Log system-wide resource summary
    pub fn log_system_summary(&self, context: &str) {
        let snapshot = self.snapshot();
        
        info!("[PERF] system_summary | context={} | total_mem_mb={} | used_mem_mb={}  | process_mem_mb={} | threads={} | cores={} | cpu={}%",
              context,
              snapshot.total_memory_mb,
              snapshot.used_memory_mb,
              snapshot.process_memory_mb,
              snapshot.process_threads,
              snapshot.available_cores,
              snapshot.cpu_usage_percent);
    }
    
    /// Calculate memory pressure (0.0 = no pressure, 1.0 = critical)
    pub fn memory_pressure(&self) -> f64 {
        let snapshot = self.snapshot();
        snapshot.used_memory_mb as f64 / snapshot.total_memory_mb as f64
    }
    
    /// Check if system is under resource pressure
    pub fn is_under_pressure(&self) -> bool {
        let snapshot = self.snapshot();
        
        // Consider under pressure if:
        // - Memory usage > 85%
        // - Process has > 100 threads
        // - CPU usage > 90%
        let memory_pressure = snapshot.used_memory_mb as f64 / snapshot.total_memory_mb as f64;
        
        memory_pressure > 0.85 || 
        snapshot.process_threads > 100 || 
        snapshot.cpu_usage_percent > 90.0
    }
}

/// Convenient timer for measuring operation duration
pub struct Timer {
    start: Instant,
    name: String,
}

impl Timer {
    pub fn new(name: String) -> Self {
        debug!("[PERF] timer_start | name={} | time={:?}", name, Instant::now());
        Self {
            start: Instant::now(),
            name,
        }
    }
    
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
    
    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let duration = self.start.elapsed();
        debug!("[PERF] timer_end | name={} | duration_ms={} | time={:?}", 
               self.name, duration.as_millis(), Instant::now());
    }
}

/// Utility macros for easy monitoring
#[macro_export]
macro_rules! monitor_operation {
    ($monitor:expr, $name:expr, $block:block) => {{
        let metrics = $monitor.start_operation($name.to_string());
        let result = $block;
        $monitor.finish_operation(metrics);
        result
    }};
}

#[macro_export]
macro_rules! timed_operation {
    ($name:expr, $block:block) => {{
        let _timer = $crate::monitoring::Timer::new($name.to_string());
        $block
    }};
}

/// Global resource monitor instance
static mut GLOBAL_MONITOR: Option<ResourceMonitor> = None;
static MONITOR_INIT: std::sync::Once = std::sync::Once::new();

/// Get or initialize the global resource monitor
pub fn get_global_monitor() -> &'static ResourceMonitor {
    unsafe {
        MONITOR_INIT.call_once(|| {
            GLOBAL_MONITOR = Some(ResourceMonitor::new());
        });
        GLOBAL_MONITOR.as_ref().unwrap()
    }
}

/// Initialize monitoring with system summary
pub fn init_monitoring(context: &str) {
    let monitor = get_global_monitor();
    monitor.log_system_summary(&format!("init_{}", context));
}