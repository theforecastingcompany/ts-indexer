use anyhow::Result;
use chrono::{DateTime, Utc, Duration};
use textplots::{Chart, Plot, Shape};

use crate::db::{TimeSeriesPoint, ColumnType, FeatureMetadata, TimestampInfo, EnhancedDatasetColumnInfo};

/// ASCII plotting functionality for time series data
pub struct TimeSeriesPlotter;

impl TimeSeriesPlotter {
    /// Create a new plotter instance
    pub fn new() -> Self {
        Self
    }

    /// Plot time series data as ASCII chart with proper timestamp x-axis
    pub fn plot_time_series(
        &self,
        data: &[TimeSeriesPoint],
        column_name: &str,
        width: Option<usize>,
        height: Option<usize>,
    ) -> Result<String> {
        if data.is_empty() {
            return Ok(format!("📊 No data available for column '{}'", column_name));
        }

        // Calculate statistics
        let values: Vec<f64> = data.iter().map(|p| p.value).collect();
        let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let avg_val = values.iter().sum::<f64>() / values.len() as f64;

        // Get time range for proper x-axis scaling
        let first_time = data.first().unwrap().timestamp;
        let last_time = data.last().unwrap().timestamp;
        let time_span = last_time - first_time;
        
        // SOLUTION: Use index-based x-axis but add time context for interpretation
        // This avoids the sparse data problem with large time ranges
        let plot_data: Vec<(f32, f32)> = data
            .iter()
            .enumerate()
            .map(|(i, point)| {
                let x = i as f32; // Use data point index for even spacing
                let y = point.value as f32;
                (x, y)
            })
            .collect();

        // Create the plot with index-based x-axis for proper visualization
        let chart_width = width.unwrap_or(80);
        let chart_height = height.unwrap_or(20);
        
        let x_min = 0.0; // Start from 0 (first data point)
        let x_max = (data.len() - 1) as f32; // End at last data point index
        
        let mut output = String::new();
        
        // Title and metadata
        output.push_str(&format!("📊 Time Series Plot: {}\n", column_name));
        output.push_str(&format!("📈 Data Points: {} | Min: {:.2} | Max: {:.2} | Avg: {:.2}\n", 
                                data.len(), min_val, max_val, avg_val));
        output.push_str(&format!("📅 Time Range: {} to {} ({} data points)\n", 
                                first_time.format("%Y-%m-%d %H:%M"), 
                                last_time.format("%Y-%m-%d %H:%M"),
                                data.len()));
        output.push_str(&"─".repeat(chart_width));
        output.push('\n');

        // Generate the ASCII plot with time-based x-axis
        let chart_result = Chart::new(
            chart_width as u32, 
            chart_height as u32, 
            x_min, 
            x_max
        )
        .lineplot(&Shape::Lines(&plot_data))
        .to_string();

        output.push_str(&chart_result);
        output.push('\n');
        
        // Add enhanced axis information with time context
        let duration_str = self.format_duration(time_span);
        output.push_str(&format!("⏱️  Duration: {} | 📊 Values: {:.2} to {:.2}\n",
            duration_str, min_val, max_val));
        
        // Add time axis markers to help interpret the plot
        output.push_str(&self.format_time_axis_markers(first_time, last_time, chart_width));
        
        output.push_str(&"─".repeat(chart_width));

        Ok(output)
    }

    /// Display static covariate value (no plotting needed)
    pub fn show_static_value(&self, column_name: &str, value: &str) -> String {
        format!("📊 Static Covariate: {}\n💎 Value: {}", column_name, value)
    }

    /// Plot multiple time series on the same chart
    pub fn plot_multiple_series(
        &self,
        series_data: &[(String, Vec<TimeSeriesPoint>)],
        width: Option<usize>,
        height: Option<usize>,
    ) -> Result<String> {
        if series_data.is_empty() {
            return Ok("📊 No data available for plotting".to_string());
        }

        let chart_width = width.unwrap_or(80);
        let chart_height = height.unwrap_or(20);
        
        let mut output = String::new();
        
        // Title
        let series_names: Vec<String> = series_data.iter().map(|(name, _)| name.clone()).collect();
        output.push_str(&format!("📊 Multi-Series Plot: {}\n", series_names.join(", ")));
        output.push_str(&"─".repeat(chart_width));
        output.push('\n');

        // For simplicity, plot the first series for now
        // TODO: Implement proper multi-series overlay
        if let Some((name, data)) = series_data.first() {
            if !data.is_empty() {
                let single_plot = self.plot_time_series(data, name, Some(chart_width), Some(chart_height))?;
                output.push_str(&single_plot);
            }
        }

        Ok(output)
    }

    /// Get appropriate plot dimensions based on terminal size
    pub fn get_optimal_dimensions(&self) -> (usize, usize) {
        // Try to get terminal size, fallback to defaults
        match crossterm::terminal::size() {
            Ok((cols, rows)) => {
                let width = (cols as usize).min(120).max(60); // Between 60-120 chars
                let height = (rows as usize / 3).min(30).max(15); // About 1/3 of terminal height
                (width, height)
            }
            Err(_) => (80, 20) // Default fallback
        }
    }

    /// Format duration in a human-readable way
    fn format_duration(&self, duration: Duration) -> String {
        let total_seconds = duration.num_seconds();
        
        if total_seconds < 60 {
            format!("{}s", total_seconds)
        } else if total_seconds < 3600 {
            let minutes = total_seconds / 60;
            let seconds = total_seconds % 60;
            if seconds == 0 {
                format!("{}m", minutes)
            } else {
                format!("{}m {}s", minutes, seconds)
            }
        } else if total_seconds < 86400 {
            let hours = total_seconds / 3600;
            let minutes = (total_seconds % 3600) / 60;
            if minutes == 0 {
                format!("{}h", hours)
            } else {
                format!("{}h {}m", hours, minutes)
            }
        } else {
            let days = total_seconds / 86400;
            let hours = (total_seconds % 86400) / 3600;
            if hours == 0 {
                format!("{}d", days)
            } else {
                format!("{}d {}h", days, hours)
            }
        }
    }

    /// Create time axis markers to help interpret the x-axis with time context
    fn format_time_axis_markers(&self, start_time: DateTime<Utc>, end_time: DateTime<Utc>, width: usize) -> String {
        let mut output = String::new();
        
        // Format times based on duration
        let duration = end_time - start_time;
        let time_format = if duration.num_days() > 1 {
            "%m/%d %H:%M" // Show date and time for longer periods
        } else if duration.num_hours() > 1 {
            "%H:%M" // Show hours:minutes for shorter periods
        } else {
            "%H:%M:%S" // Show seconds for very short periods
        };
        
        let start_str = start_time.format(time_format).to_string();
        let end_str = end_time.format(time_format).to_string();
        
        // Create a simple time range indicator
        output.push_str("📍 Time Axis: ");
        output.push_str(&start_str);
        output.push_str(" ──────── ");
        output.push_str(&end_str);
        output.push_str(" (evenly spaced data points)\n");
        
        output
    }

    /// Create a summary plot for column selection (smaller preview)
    pub fn create_preview_plot(&self, data: &[TimeSeriesPoint], column_name: &str) -> Result<String> {
        if data.is_empty() {
            return Ok(format!("📊 {} (No data)", column_name));
        }

        // Take a sample of data for preview (max 50 points)
        let sample_size = data.len().min(50);
        let step = if data.len() > sample_size { data.len() / sample_size } else { 1 };
        let sampled_data: Vec<TimeSeriesPoint> = data.iter().step_by(step).cloned().collect();

        self.plot_time_series(&sampled_data, column_name, Some(60), Some(10))
    }
    
    /// Display enhanced feature metadata information
    pub fn display_feature_metadata(&self, feature: &FeatureMetadata) -> String {
        let mut output = String::new();
        
        output.push_str(&format!("🔍 Feature Analysis: {}\n", feature.name));
        output.push_str(&format!("   📊 Attribute: {}\n", utils::format_feature_attribute(&feature.attribute)));
        output.push_str(&format!("   ⏱️  Temporality: {}\n", utils::format_temporality(&feature.temporality)));
        output.push_str(&format!("   🎯 Modality: {}\n", utils::format_modality(&feature.modality)));
        output.push_str(&format!("   🌍 Scope: {}\n", utils::format_scope(&feature.scope)));
        output.push_str(&format!("   💽 Data Type: {}\n", feature.data_type));
        
        if let Some(description) = &feature.description {
            output.push_str(&format!("   📝 Description: {}\n", description));
        }
        
        // Validation check
        match feature.validate() {
            Ok(()) => output.push_str(&format!("   ✅ Validation: Passed\n")),
            Err(e) => output.push_str(&format!("   ❌ Validation: {}\n", e)),
        }
        
        output
    }
    
    /// Display timestamp dimension information
    pub fn display_timestamp_info(&self, timestamp_info: &TimestampInfo) -> String {
        let mut output = String::new();
        
        output.push_str(&format!("⏰ Timestamp Dimension: {}\n", timestamp_info.column_name));
        output.push_str(&format!("   💽 Data Type: {}\n", timestamp_info.data_type));
        
        if let Some(frequency) = &timestamp_info.frequency {
            output.push_str(&format!("   📅 Frequency: {}\n", frequency));
        }
        
        if let Some(timezone) = &timestamp_info.timezone {
            output.push_str(&format!("   🌍 Timezone: {}\n", timezone));
        }
        
        if let Some(description) = &timestamp_info.description {
            output.push_str(&format!("   📝 Description: {}\n", description));
        }
        
        output.push_str(&format!("   ℹ️  Note: Timestamp is the time index, not a feature covariate\n"));
        
        output
    }
}

/// Utility functions for formatting and display
pub mod utils {
    use super::*;

    /// Format column type for display
    pub fn format_column_type(column_type: &ColumnType) -> &'static str {
        match column_type {
            ColumnType::Target => "🎯 Target",
            ColumnType::HistoricalCovariate => "📈 Historical",
            ColumnType::StaticCovariate => "💎 Static",
            ColumnType::FutureCovariate => "🔮 Future",
            ColumnType::Timestamp => "⏰ Timestamp",
        }
    }
    
    /// Format feature attribute for display (new enhanced system)
    pub fn format_feature_attribute(attribute: &crate::db::FeatureAttribute) -> &'static str {
        match attribute {
            crate::db::FeatureAttribute::Targets => "🎯 Target",
            crate::db::FeatureAttribute::HistoricalCovariates => "📈 Historical",
            crate::db::FeatureAttribute::StaticCovariates => "💎 Static",
            crate::db::FeatureAttribute::FutureCovariates => "🔮 Future",
        }
    }
    
    /// Format temporality for display
    pub fn format_temporality(temporality: &crate::db::Temporality) -> &'static str {
        match temporality {
            crate::db::Temporality::Static => "⚡ Static",
            crate::db::Temporality::Dynamic => "📊 Dynamic",
        }
    }
    
    /// Format modality for display
    pub fn format_modality(modality: &crate::db::Modality) -> &'static str {
        match modality {
            crate::db::Modality::Categorical => "🏷️  Categorical",
            crate::db::Modality::Numerical => "🔢 Numerical",
        }
    }
    
    /// Format scope for display
    pub fn format_scope(scope: &crate::db::Scope) -> &'static str {
        match scope {
            crate::db::Scope::Local => "🏠 Local",
            crate::db::Scope::Global => "🌍 Global",
        }
    }

    /// Create a simple histogram for value distribution
    pub fn create_value_histogram(values: &[f64], bins: usize) -> String {
        if values.is_empty() {
            return "No data for histogram".to_string();
        }

        let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_val = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        if min_val == max_val {
            return format!("All values are {:.2}", min_val);
        }

        let bin_width = (max_val - min_val) / bins as f64;
        let mut histogram = vec![0; bins];

        // Count values in each bin
        for &value in values {
            let bin_index = ((value - min_val) / bin_width).floor() as usize;
            let bin_index = bin_index.min(bins - 1); // Handle edge case
            histogram[bin_index] += 1;
        }

        // Find max count for scaling
        let max_count = *histogram.iter().max().unwrap_or(&1);
        
        let mut output = String::new();
        output.push_str("📊 Value Distribution:\n");
        
        for (i, &count) in histogram.iter().enumerate() {
            let bin_start = min_val + i as f64 * bin_width;
            let bin_end = bin_start + bin_width;
            let bar_length = if max_count > 0 { (count * 20) / max_count } else { 0 };
            let bar = "█".repeat(bar_length);
            
            output.push_str(&format!("  {:.1}-{:.1}: {} ({})\n", 
                                   bin_start, bin_end, bar, count));
        }

        output
    }
}

#[cfg(test)]
mod plot_tests {
    use super::*;
    use chrono::{DateTime, Utc, Duration};

    fn create_sample_data() -> Vec<TimeSeriesPoint> {
        let start_time = DateTime::parse_from_rfc3339("2016-02-29T05:00:00Z").unwrap().with_timezone(&Utc);
        let mut data = Vec::new();
        
        // Generate 10 sample points over a few days with varying values
        for i in 0..10 {
            let timestamp = start_time + Duration::hours(i * 6); // Every 6 hours
            let value = 100.0 + (i as f64 * 50.0); // Linear increase
            data.push(TimeSeriesPoint { timestamp, value });
        }
        data
    }

    #[test]
    fn test_plot_debugging() {
        let plotter = TimeSeriesPlotter::new();
        let data = create_sample_data();
        
        println!("=== DEBUGGING TIME SERIES DATA ===");
        println!("Data points: {}", data.len());
        
        let first_time = data.first().unwrap().timestamp;
        let last_time = data.last().unwrap().timestamp;
        let time_span = last_time - first_time;
        
        println!("Time span: {} seconds ({} hours)", time_span.num_seconds(), time_span.num_hours());
        
        // Show the data points that would be generated
        for (i, point) in data.iter().enumerate() {
            let seconds_since_start = (point.timestamp - first_time).num_seconds() as f32;
            println!("Point {}: x={:.0}s, y={:.1}, time={}", 
                    i, seconds_since_start, point.value, point.timestamp.format("%Y-%m-%d %H:%M"));
        }
        
        // Test the actual plotting function
        let result = plotter.plot_time_series(&data, "test_column", Some(60), Some(10));
        match result {
            Ok(plot) => println!("Generated plot:\n{}", plot),
            Err(e) => println!("Plot error: {}", e),
        }
    }

    #[test] 
    fn test_textplots_directly() {
        // Test textplots library directly with simple data
        let simple_data: Vec<(f32, f32)> = vec![
            (0.0, 100.0),
            (10.0, 150.0), 
            (20.0, 200.0),
            (30.0, 250.0),
            (40.0, 300.0),
        ];
        
        println!("=== TESTING TEXTPLOTS DIRECTLY ===");
        println!("Data points: {:?}", simple_data);
        
        let chart_result = Chart::new(60, 10, 0.0, 40.0)
            .lineplot(&Shape::Lines(&simple_data))
            .to_string();
        
        println!("Direct textplots result:\n{}", chart_result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_empty_data_plotting() {
        let plotter = TimeSeriesPlotter::new();
        let result = plotter.plot_time_series(&[], "test_column", None, None).unwrap();
        assert!(result.contains("No data available"));
    }

    #[test]
    fn test_static_value_display() {
        let plotter = TimeSeriesPlotter::new();
        let result = plotter.show_static_value("category", "electronics");
        assert!(result.contains("Static Covariate"));
        assert!(result.contains("electronics"));
    }

    #[test]
    fn test_value_histogram() {
        let values = vec![1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 5.0];
        let histogram = utils::create_value_histogram(&values, 5);
        assert!(histogram.contains("Distribution"));
    }
}