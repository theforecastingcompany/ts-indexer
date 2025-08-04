use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, List, ListItem, ListState, Paragraph, Wrap,
        Axis, Chart, Dataset
    },
    Frame, Terminal,
    symbols,
};
use std::{collections::{HashMap, HashSet}, fs::OpenOptions, io::Write, time::Duration};
use tracing::{debug, info};
use chrono;
use tokio::sync::mpsc;

use crate::db::{Database, FeatureMetadata, FeatureAttribute};
use crate::search::{EnhancedSearchResult, SearchEngine};

#[derive(Debug, Clone, PartialEq)]
enum ViewLevel {
    Dataset,
    Series,
    Features,
}

#[derive(Debug, Clone, PartialEq)]
enum DatasetSortMode {
    Name,
    SeriesCount,
    RecordCount,
}

#[derive(Debug, Clone)]
enum FeatureDataState {
    NeedsFetch,
    Loading,
    Ready(Vec<(f64, f64)>),
    Error(String),
}

/// Communication between async data fetching tasks and the main UI thread
#[derive(Debug)]
enum FeatureDataResult {
    Success {
        feature_name: String,
        series_id: String,
        data: Vec<(f64, f64)>,
    },
    Error {
        feature_name: String,
        series_id: String,
        error: String,
    },
}

#[derive(Debug, Clone)]
struct DatasetGroup {
    name: String,
    series: Vec<EnhancedSearchResult>,
    total_records: i64,
}

#[derive(Debug, Clone)]
struct FeatureCategory {
    name: String,
    features: Vec<FeatureMetadata>,
    count: usize,
}

#[derive(Debug, Clone)]
struct SeriesFeatures {
    series_id: String,
    dataset_id: String,
    categories: Vec<FeatureCategory>,
}

pub struct InteractiveFinder {
    search_engine: SearchEngine,
    db: Database,
    query: String,
    selected_index: usize,
    list_state: ListState,
    show_preview: bool,
    
    // Hierarchical navigation state
    current_level: ViewLevel,
    dataset_groups: HashMap<String, DatasetGroup>,
    current_dataset: Option<String>,
    current_series: Option<String>,
    
    // Sorting state
    dataset_sort_mode: DatasetSortMode,
    
    // Display data for current level
    dataset_list: Vec<String>,
    series_list: Vec<EnhancedSearchResult>,
    features_list: Vec<String>,
    current_features: Option<SeriesFeatures>,
    current_features_data: Option<Vec<FeatureMetadata>>,
    
    // Feature data caching and loading state
    feature_data_cache: HashMap<String, Vec<(f64, f64)>>,
    loading_features: HashSet<String>,
    data_fetch_errors: HashMap<String, String>,
    
    // Number selection mode
    number_input_mode: bool,
    number_input_buffer: String,
    
    // Loading animation state
    loading_spinner_frame: usize,
    
    // Async communication
    data_receiver: mpsc::UnboundedReceiver<FeatureDataResult>,
    data_sender: mpsc::UnboundedSender<FeatureDataResult>,
}

impl InteractiveFinder {
    pub fn new(db: Database) -> Self {
        let search_engine = SearchEngine::new(db.clone());
        
        Self::with_search_engine(db, search_engine)
    }
    
    /// Format numbers with US-style thousand separators
    fn format_number(num: impl std::fmt::Display) -> String {
        let num_str = num.to_string();
        let mut result = String::new();
        let chars: Vec<char> = num_str.chars().collect();
        
        for (i, ch) in chars.iter().enumerate() {
            if i > 0 && (chars.len() - i) % 3 == 0 {
                result.push(',');
            }
            result.push(*ch);
        }
        
        result
    }
    
    pub fn with_search_engine(db: Database, search_engine: SearchEngine) -> Self {
        let (data_sender, data_receiver) = mpsc::unbounded_channel();
        
        Self {
            search_engine,
            db,
            query: String::new(),
            selected_index: 0,
            list_state: ListState::default(),
            show_preview: true,
            
            // Initialize hierarchical navigation
            current_level: ViewLevel::Dataset,
            dataset_groups: HashMap::new(),
            current_dataset: None,
            current_series: None,
            
            // Initialize sorting state
            dataset_sort_mode: DatasetSortMode::Name,
            
            // Initialize display lists
            dataset_list: Vec::new(),
            series_list: Vec::new(),
            features_list: Vec::new(),
            current_features: None,
            current_features_data: None,
            
            // Initialize data caching
            feature_data_cache: HashMap::new(),
            loading_features: HashSet::new(),
            data_fetch_errors: HashMap::new(),
            
            // Initialize number selection mode
            number_input_mode: false,
            number_input_buffer: String::new(),
            
            // Initialize loading animation
            loading_spinner_frame: 0,
            
            // Initialize async communication
            data_receiver,
            data_sender,
        }
    }
    
    /// Generate sample time series data for placeholder plots
    fn generate_sample_data() -> Vec<(f64, f64)> {
        (0..50)
            .map(|i| {
                let x = i as f64;
                let y = (x * 0.3).sin() + (x * 0.1).cos() * 0.5 + (i % 7) as f64 * 0.1;
                (x, y)
            })
            .collect()
    }
    
    /// Generate cache key for feature data
    fn get_cache_key(&self, feature_name: &str) -> String {
        if let Some(series_id) = &self.current_series {
            format!("{}::{}", series_id, feature_name)
        } else {
            feature_name.to_string()
        }
    }
    
    /// Get current loading spinner character and advance frame
    fn get_loading_spinner(&mut self) -> char {
        let spinners = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let spinner = spinners[self.loading_spinner_frame % spinners.len()];
        self.loading_spinner_frame = (self.loading_spinner_frame + 1) % spinners.len();
        spinner
    }
    
    /// Fetch real time series data for a specific feature
    async fn fetch_feature_data(&self, series_id: &str, feature_name: &str) -> Result<Vec<(f64, f64)>> {
        // Use the existing database method to get time series data with S3 fallback
        let time_series_points = self.db.get_time_series_data_with_fallback(
            series_id, 
            feature_name, 
            Some(100) // Limit to 100 points for performance
        ).await?;
        
        // Convert TimeSeriesPoint to (f64, f64) for chart rendering
        // Filter out invalid values (NaN, infinite) to prevent chart rendering issues
        eprintln!("[DEBUG {}] Raw data points for {}: {} total points", 
                  chrono::Utc::now().format("%H:%M:%S"), feature_name, time_series_points.len());
        
        if !time_series_points.is_empty() {
            let first_few: Vec<String> = time_series_points.iter().take(3)
                .map(|p| format!("{}:{:.2}", p.timestamp.format("%H:%M:%S"), p.value))
                .collect();
            eprintln!("[DEBUG {}] First 3 points: [{}]", 
                      chrono::Utc::now().format("%H:%M:%S"), first_few.join(", "));
        }
        
        let chart_data: Vec<(f64, f64)> = time_series_points
            .iter()
            .enumerate()
            .filter_map(|(i, point)| {
                // Only include finite values
                if point.value.is_finite() {
                    Some((i as f64, point.value))
                } else {
                    eprintln!("[DEBUG {}] Filtered out non-finite value: {}", 
                              chrono::Utc::now().format("%H:%M:%S"), point.value);
                    None
                }
            })
            .collect();
            
        eprintln!("[DEBUG {}] Chart data after filtering: {} points", 
                  chrono::Utc::now().format("%H:%M:%S"), chart_data.len());
        
        Ok(chart_data)
    }
    
    /// Get feature data with caching - handles both loading states and cached data
    fn get_feature_data(&mut self, feature_name: &str) -> FeatureDataState {
        let cache_key = self.get_cache_key(feature_name);
        
        // Check if data is already cached
        if let Some(data) = self.feature_data_cache.get(&cache_key) {
            return FeatureDataState::Ready(data.clone());
        }
        
        // Check if data is currently being loaded
        if self.loading_features.contains(&cache_key) {
            return FeatureDataState::Loading;
        }
        
        // Check if there was an error loading this data
        if let Some(error) = self.data_fetch_errors.get(&cache_key) {
            return FeatureDataState::Error(error.clone());
        }
        
        // Data needs to be fetched
        FeatureDataState::NeedsFetch
    }
    
    /// Render a time series chart using Ratatui's Chart widget or static value display
    fn render_feature_chart(&mut self, f: &mut Frame, area: Rect, feature_name: &str, feature: &FeatureMetadata) {
        // Handle static covariates differently - show actual value instead of time series
        if feature.attribute == FeatureAttribute::StaticCovariates {
            self.render_static_value_display(f, area, feature_name, feature);
        } else {
            self.render_time_series_chart(f, area, feature_name);
        }
    }
    
    /// Render static value display for static covariates
    fn render_static_value_display(&mut self, f: &mut Frame, area: Rect, feature_name: &str, feature: &FeatureMetadata) {
        let cache_key = if let Some(series_id) = &self.current_series {
            format!("{}::{}", series_id, feature_name)
        } else {
            feature_name.to_string()
        };
        
        // If loading, show animated spinner using the same loading chart as time series
        if self.loading_features.contains(&cache_key) {
            self.render_loading_chart(f, area, feature_name);
            return;
        }
        
        // If error, show error chart
        if let Some(error) = self.data_fetch_errors.get(&cache_key) {
            self.render_error_chart(f, area, feature_name, error);
            return;
        }
        
        // Check if we have cached static value data
        let display_value = if let Some(cached_data) = self.feature_data_cache.get(&cache_key) {
            // For static covariates, take the first value (they should all be the same)
            if let Some((_, value)) = cached_data.first() {
                match feature.modality {
                    crate::db::Modality::Numerical => format!("{:.2}", value),
                    crate::db::Modality::Categorical => {
                        // For categorical, convert the numerical value back to category
                        let category_index = *value as usize;
                        let categories = ["Category A", "Category B", "Category C", "High", "Medium", "Low", "Type 1", "Type 2"];
                        categories.get(category_index).unwrap_or(&"Unknown").to_string()
                    }
                }
            } else {
                "No data available".to_string()
            }
        } else {
            "Press Enter to load value".to_string()
        };
        
        // Determine colors and status based on data state
        let (title_color, value_color, status_text) = if self.feature_data_cache.contains_key(&cache_key) {
            (Color::Green, Color::Cyan, "Real value from S3")
        } else {
            (Color::Blue, Color::Blue, "Press Enter to load")
        };
        
        // Create content lines for the static value display
        let content_lines = vec![
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "📐 STATIC VALUE",
                Style::default().fg(title_color).add_modifier(Modifier::BOLD)
            )),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                format!("Current Value: {}", display_value),
                Style::default().fg(value_color).add_modifier(Modifier::BOLD)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "This value remains constant across all time points",
                Style::default().fg(Color::Gray)
            )),
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                status_text,
                Style::default().fg(Color::DarkGray)
            )),
        ];
        
        let static_display = Paragraph::new(Text::from(content_lines))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    " Static Covariate Value ",
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                ))
            )
            .alignment(Alignment::Center);
        
        f.render_widget(static_display, area);
    }
    
    /// Render time series chart for dynamic features
    fn render_time_series_chart(&mut self, f: &mut Frame, area: Rect, feature_name: &str) {
        let cache_key = if let Some(series_id) = &self.current_series {
            format!("{}::{}", series_id, feature_name)
        } else {
            feature_name.to_string()
        };
        
        // Check if we have cached data
        let data = if let Some(cached_data) = self.feature_data_cache.get(&cache_key) {
            cached_data.clone()
        } else if self.loading_features.contains(&cache_key) {
            // Show loading state
            self.render_loading_chart(f, area, feature_name);
            return;
        } else if let Some(error) = self.data_fetch_errors.get(&cache_key) {
            // Show error state
            self.render_error_chart(f, area, feature_name, error);
            return;
        } else {
            // No data available yet, show placeholder and note that data needs fetching
            // We'll trigger data fetching from the main event loop
            self.render_needs_fetch_chart(f, area, feature_name);
            return;
        };
        
        // Handle empty data case
        if data.is_empty() {
            self.render_error_chart(f, area, feature_name, "No valid data points available");
            return;
        }
        
        // Find data bounds for axis scaling - ensure finite values
        let x_values: Vec<f64> = data.iter().map(|(x, _)| *x).filter(|x| x.is_finite()).collect();
        let y_values: Vec<f64> = data.iter().map(|(_, y)| *y).filter(|y| y.is_finite()).collect();
        
        if x_values.is_empty() || y_values.is_empty() {
            self.render_error_chart(f, area, feature_name, "No finite data values available");
            return;
        }
        
        let x_min = x_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let x_max = x_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let y_min = y_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let y_max = y_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        
        // Add some padding to the bounds (handle case where y_min == y_max)
        let y_range = y_max - y_min;
        let y_padding = if y_range > 0.0 { y_range * 0.1 } else { 1.0 }; // Default padding for constant values
        let y_min = y_min - y_padding;
        let y_max = y_max + y_padding;
        
        // Create dataset
        let datasets = vec![
            Dataset::default()
                .name(feature_name)
                .marker(symbols::Marker::Braille)
                .style(Style::default().fg(Color::Cyan))
                .data(&data)
        ];
        
        // Create x-axis labels
        let x_labels = vec![
            Span::raw("0"),
            Span::raw("10"), 
            Span::raw("20"),
            Span::raw("30"),
            Span::raw("40"),
            Span::raw("50")
        ];
        
        // Create y-axis labels
        let y_labels = vec![
            Span::raw(format!("{:.1}", y_min)),
            Span::raw(format!("{:.1}", (y_min + y_max) / 2.0)),
            Span::raw(format!("{:.1}", y_max))
        ];
        
        // Split area to show chart and statistics
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(75), Constraint::Percentage(25)])
            .split(area);
        
        // Calculate statistics
        let original_y_min = y_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let original_y_max = y_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let y_avg = y_values.iter().sum::<f64>() / y_values.len() as f64;
        let data_count = y_values.len();
        
        // Create and render the chart
        let chart = Chart::new(datasets)
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    " Time Series Preview ",
                    Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
                ))
            )
            .x_axis(
                Axis::default()
                    .title("Time")
                    .style(Style::default().fg(Color::Gray))
                    .labels(x_labels)
                    .bounds([x_min, x_max])
            )
            .y_axis(
                Axis::default()
                    .title("Value")
                    .style(Style::default().fg(Color::Gray))
                    .labels(y_labels)
                    .bounds([y_min, y_max])
            );
        
        f.render_widget(chart, chunks[0]);
        
        // Render statistics panel
        let stats_lines = vec![
            Line::from(vec![
                Span::styled("📊 Statistics: ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            ]),
            Line::from(vec![
                Span::styled("Min: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{:.3}", original_y_min)),
                Span::styled("  Max: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{:.3}", original_y_max)),
                Span::styled("  Avg: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{:.3}", y_avg)),
            ]),
            Line::from(vec![
                Span::styled("Data Points: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{}", data_count)),
                Span::styled("  Range: ", Style::default().fg(Color::Cyan)),
                Span::raw(format!("{:.3}", original_y_max - original_y_min)),
            ]),
        ];
        
        let stats_panel = Paragraph::new(Text::from(stats_lines))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(" Statistics ", Style::default().fg(Color::Blue)))
            );
        
        f.render_widget(stats_panel, chunks[1]);
    }
    
    /// Render loading state for chart
    fn render_loading_chart(&mut self, f: &mut Frame, area: Rect, feature_name: &str) {
        let spinner = self.get_loading_spinner();
        let loading_text = vec![
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                format!("{} LOADING DATA...", spinner),
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Fetching time series from S3",
                Style::default().fg(Color::Cyan)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Please wait...",
                Style::default().fg(Color::Gray)
            )),
        ];
        
        let loading_display = Paragraph::new(Text::from(loading_text))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    format!(" {} - Loading ", feature_name),
                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                ))
            )
            .alignment(Alignment::Center);
        
        f.render_widget(loading_display, area);
    }
    
    /// Render error state for chart
    fn render_error_chart(&self, f: &mut Frame, area: Rect, feature_name: &str, error: &str) {
        let error_text = vec![
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "❌ DATA FETCH ERROR",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
            )),
            Line::from(""),
            Line::from(Span::styled(
                error,
                Style::default().fg(Color::Red)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Press 'r' to retry fetching data",
                Style::default().fg(Color::Gray)
            )),
        ];
        
        let error_display = Paragraph::new(Text::from(error_text))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    format!(" {} - Error ", feature_name),
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                ))
            )
            .alignment(Alignment::Center);
        
        f.render_widget(error_display, area);
    }
    
    /// Render "needs fetch" state for chart
    fn render_needs_fetch_chart(&self, f: &mut Frame, area: Rect, feature_name: &str) {
        let fetch_text = vec![
            Line::from(""),
            Line::from(""),
            Line::from(Span::styled(
                "📊 REAL DATA AVAILABLE",
                Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "Press 'Enter' to load time series from S3",
                Style::default().fg(Color::Cyan)
            )),
            Line::from(""),
            Line::from(Span::styled(
                "This will fetch actual parquet data",
                Style::default().fg(Color::Gray)
            )),
        ];
        
        let fetch_display = Paragraph::new(Text::from(fetch_text))
            .block(Block::default()
                .borders(Borders::ALL)
                .title(Span::styled(
                    format!(" {} - Press Enter to Load ", feature_name),
                    Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD)
                ))
            )
            .alignment(Alignment::Center);
        
        f.render_widget(fetch_display, area);
    }
    
    /// Check if an index in the features list is selectable (not a header or spacer)
    fn is_feature_selectable(&self, index: usize) -> bool {
        if let Some(features_data) = &self.current_features_data {
            if let Some(feature) = features_data.get(index) {
                return !feature.name.starts_with("HEADER:") && feature.name != "SPACER";
            }
        }
        true // Default to selectable for other view levels
    }
    
    /// Find the next selectable feature index when navigating up/down
    fn find_next_selectable(&self, current_index: usize, direction: i32) -> usize {
        if self.current_level != ViewLevel::Features {
            return current_index;
        }
        
        let max_index = self.features_list.len().saturating_sub(1);
        let mut new_index = current_index;
        
        loop {
            if direction > 0 {
                // Moving down
                if new_index >= max_index {
                    break;
                }
                new_index += 1;
            } else {
                // Moving up
                if new_index == 0 {
                    break;
                }
                new_index -= 1;
            }
            
            if self.is_feature_selectable(new_index) {
                return new_index;
            }
        }
        
        current_index // Return original if no selectable item found
    }
    
    pub async fn run(mut self) -> Result<Option<Vec<EnhancedSearchResult>>> {
        // Redirect stderr to log file during TUI session to prevent UI interference
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("ts_indexer_interactive.log")?;
        
        let _redirect_guard = {
            use std::os::unix::io::AsRawFd;
            
            // Save original stdout and stderr
            let original_stdout = unsafe { libc::dup(libc::STDOUT_FILENO) };
            let original_stderr = unsafe { libc::dup(libc::STDERR_FILENO) };
            
            // Redirect both stdout and stderr to log file
            unsafe { 
                libc::dup2(log_file.as_raw_fd(), libc::STDOUT_FILENO);
                libc::dup2(log_file.as_raw_fd(), libc::STDERR_FILENO);
            }
            
            // Log the start of interactive session (this will go to the log file now)
            println!("🎯 Interactive finder started at {} - all logs redirected to ts_indexer_interactive.log", 
                     chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"));
            eprintln!("DEBUG: Logging setup complete, stderr and stdout redirected to log file");
            
            // Return guard to restore both stdout and stderr on drop
            struct RedirectGuard(i32, i32);
            impl Drop for RedirectGuard {
                fn drop(&mut self) {
                    unsafe {
                        libc::dup2(self.0, libc::STDOUT_FILENO);
                        libc::dup2(self.1, libc::STDERR_FILENO);
                        libc::close(self.0);
                        libc::close(self.1);
                    }
                }
            }
            RedirectGuard(original_stdout, original_stderr)
        };
        
        // Setup terminal with a direct TTY handle to avoid redirection
        enable_raw_mode()?;
        
        // Create a direct TTY handle that bypasses stdout redirection
        let tty = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/tty")?;
        
        let mut tty_writer = tty.try_clone()?;
        execute!(tty_writer, EnterAlternateScreen, EnableMouseCapture)?;
        let backend = CrosstermBackend::new(tty_writer);
        let mut terminal = Terminal::new(backend)?;
        
        // Initial search with empty query to show all results
        self.update_search().await?;
        
        let result = self.run_app(&mut terminal).await;
        
        // Log the end of interactive session
        info!("🏁 Interactive finder session ended");
        
        // Restore terminal
        disable_raw_mode()?;
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )?;
        terminal.show_cursor()?;
        
        // stdout and stderr will be restored when _redirect_guard is dropped
        result
    }
    
    async fn run_app<B: Backend>(
        &mut self,
        terminal: &mut Terminal<B>,
    ) -> Result<Option<Vec<EnhancedSearchResult>>> {
        let mut needs_update = false;
        let mut last_query_change = std::time::Instant::now();
        let debounce_duration = std::time::Duration::from_millis(150); // 150ms debounce
        
        loop {
            // Only update search if enough time has passed since last change (debouncing)
            if needs_update && last_query_change.elapsed() >= debounce_duration {
                self.update_search().await?;
                needs_update = false;
            }
            
            terminal.draw(|f| self.ui(f))?;
            
            // Process any completed async data fetches
            while let Ok(message) = self.data_receiver.try_recv() {
                self.process_data_result(message);
            }
            
            // Use non-blocking event polling with shorter timeout for smoother spinner animation
            if crossterm::event::poll(std::time::Duration::from_millis(100))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                        KeyCode::Char(c) => {
                            if self.number_input_mode && self.current_level == ViewLevel::Features {
                                // Number input mode
                                if c.is_ascii_digit() {
                                    self.number_input_buffer.push(c);
                                } else if c == ' ' && !self.number_input_buffer.is_empty() {
                                    // Space to confirm number selection
                                    if let Ok(number) = self.number_input_buffer.parse::<usize>() {
                                        if let Some(feature_index) = self.get_nth_selectable_feature(number.saturating_sub(1)) {
                                            self.selected_index = feature_index;
                                            self.list_state.select(Some(self.selected_index));
                                            
                                            // Auto-load cached features
                                            if let Some(feature_name) = self.get_current_feature_name() {
                                                self.try_auto_load_cached_feature(&feature_name).await;
                                            }
                                        }
                                    }
                                    self.number_input_buffer.clear();
                                }
                            } else {
                                // Regular query input mode
                                self.query.push(c);
                                needs_update = true;
                                last_query_change = std::time::Instant::now();
                            }
                        }
                        KeyCode::Backspace => {
                            if self.number_input_mode && self.current_level == ViewLevel::Features {
                                // In number input mode, clear the buffer
                                self.number_input_buffer.pop();
                            } else {
                                // Regular search mode
                                self.query.pop();
                                needs_update = true;
                                last_query_change = std::time::Instant::now();
                            }
                        }
                        KeyCode::Enter => {
                            eprintln!("[DEBUG {}] ⌨️ Enter key pressed at level: {:?}", chrono::Utc::now().format("%H:%M:%S"), self.current_level);
                            match self.current_level {
                                ViewLevel::Dataset => {
                                    eprintln!("[DEBUG {}] 📊 Processing Enter at Dataset level, dataset_list.len(): {}", chrono::Utc::now().format("%H:%M:%S"), self.dataset_list.len());
                                    if !self.dataset_list.is_empty() {
                                        self.enter_dataset().await?;
                                    }
                                }
                                ViewLevel::Series => {
                                    eprintln!("[DEBUG {}] 📈 Processing Enter at Series level, series_list.len(): {}, selected_index: {}", chrono::Utc::now().format("%H:%M:%S"), self.series_list.len(), self.selected_index);
                                    if !self.series_list.is_empty() {
                                        self.enter_series().await?;
                                    }
                                }
                                ViewLevel::Features => {
                                    eprintln!("[DEBUG {}] 🔧 Enter key pressed at Features level - checking for data to fetch", chrono::Utc::now().format("%H:%M:%S"));
                                    
                                    // Check if current feature needs data fetching
                                    if let Some(feature_name) = self.current_feature_needs_fetch() {
                                        eprintln!("[DEBUG {}] 📊 Starting data fetch for feature: {}", chrono::Utc::now().format("%H:%M:%S"), feature_name);
                                        
                                        if self.start_feature_data_fetch(&feature_name) {
                                            // Start async data fetching
                                            if let (Some(current_series), Some(current_dataset)) = 
                                                (&self.current_series, &self.current_dataset) {
                                                
                                                eprintln!("[DEBUG {}] 🚀 Fetching data for series: {}, feature: {}", 
                                                    chrono::Utc::now().format("%H:%M:%S"), current_series, feature_name);
                                                
                                                let series_id = current_series.clone();
                                                let feature_name_clone = feature_name.clone();
                                                let db = self.db.clone();
                                                let sender = self.data_sender.clone();
                                                
                                                // Spawn async task for data fetching with timeout
                                                tokio::spawn(async move {
                                                    let timeout_duration = Duration::from_secs(30);
                                                    let result = tokio::time::timeout(
                                                        timeout_duration,
                                                        db.get_time_series_data_with_fallback(&series_id, &feature_name_clone, Some(100))
                                                    ).await;
                                                    
                                                    let message = match result {
                                                        Ok(Ok(time_series_points)) => {
                                                            let chart_data: Vec<(f64, f64)> = time_series_points
                                                                .iter()
                                                                .enumerate()
                                                                .filter_map(|(i, point)| {
                                                                    if point.value.is_finite() {
                                                                        Some((i as f64, point.value))
                                                                    } else {
                                                                        None
                                                                    }
                                                                })
                                                                .collect();
                                                            
                                                            eprintln!("[DEBUG {}] ✅ Successfully fetched {} data points for feature: {}", 
                                                                chrono::Utc::now().format("%H:%M:%S"), chart_data.len(), feature_name_clone);
                                                            
                                                            FeatureDataResult::Success {
                                                                feature_name: feature_name_clone.clone(),
                                                                series_id: series_id.clone(),
                                                                data: chart_data,
                                                            }
                                                        }
                                                        Ok(Err(e)) => {
                                                            eprintln!("[DEBUG {}] ❌ Failed to fetch data for feature {}: {}", 
                                                                chrono::Utc::now().format("%H:%M:%S"), feature_name_clone, e);
                                                            
                                                            FeatureDataResult::Error {
                                                                feature_name: feature_name_clone.clone(),
                                                                series_id: series_id.clone(),
                                                                error: e.to_string(),
                                                            }
                                                        }
                                                        Err(_) => {
                                                            eprintln!("[DEBUG {}] ⏰ Timeout fetching data for feature: {}", 
                                                                chrono::Utc::now().format("%H:%M:%S"), feature_name_clone);
                                                            
                                                            FeatureDataResult::Error {
                                                                feature_name: feature_name_clone.clone(),
                                                                series_id: series_id.clone(),
                                                                error: "Request timed out after 30 seconds".to_string(),
                                                            }
                                                        }
                                                    };
                                                    
                                                    // Send result back to main thread
                                                    if let Err(e) = sender.send(message) {
                                                        eprintln!("[DEBUG {}] ❌ Failed to send result back to UI: {}", 
                                                            chrono::Utc::now().format("%H:%M:%S"), e);
                                                    }
                                                });
                                            }
                                        }
                                    } else {
                                        eprintln!("[DEBUG {}] ℹ️ No data fetch needed for current feature", chrono::Utc::now().format("%H:%M:%S"));
                                    }
                                }
                            }
                        }
                        KeyCode::Tab => {
                            if self.current_level == ViewLevel::Features {
                                // Toggle between search and number input modes
                                self.number_input_mode = !self.number_input_mode;
                                self.number_input_buffer.clear();
                                eprintln!("[DEBUG {}] 🔄 Toggled input mode: {} mode", 
                                    chrono::Utc::now().format("%H:%M:%S"), 
                                    if self.number_input_mode { "Number" } else { "Search" });
                            } else {
                                // For other levels, same as Enter
                                match self.current_level {
                                    ViewLevel::Dataset => {
                                        if !self.dataset_list.is_empty() {
                                            self.enter_dataset().await?;
                                        }
                                    }
                                    ViewLevel::Series => {
                                        if !self.series_list.is_empty() {
                                            self.enter_series().await?;
                                        }
                                    }
                                    ViewLevel::Features => {
                                        // Already handled above
                                    }
                                }
                            }
                        }
                        KeyCode::Esc => {
                            match self.current_level {
                                ViewLevel::Dataset => return Ok(None),
                                ViewLevel::Series => {
                                    self.go_back_to_dataset().await?;
                                }
                                ViewLevel::Features => {
                                    self.go_back_to_series().await?;
                                }
                            }
                        }
                        KeyCode::Up => {
                            if self.current_level == ViewLevel::Features {
                                let new_index = self.find_next_selectable(self.selected_index, -1);
                                if new_index != self.selected_index {
                                    self.selected_index = new_index;
                                    self.list_state.select(Some(self.selected_index));
                                    
                                    // Auto-load cached features
                                    if let Some(feature_name) = self.get_current_feature_name() {
                                        self.try_auto_load_cached_feature(&feature_name).await;
                                    }
                                }
                            } else {
                                if self.selected_index > 0 {
                                    self.selected_index -= 1;
                                    self.list_state.select(Some(self.selected_index));
                                }
                            }
                        }
                        KeyCode::Down => {
                            if self.current_level == ViewLevel::Features {
                                let new_index = self.find_next_selectable(self.selected_index, 1);
                                if new_index != self.selected_index {
                                    self.selected_index = new_index;
                                    self.list_state.select(Some(self.selected_index));
                                    
                                    // Auto-load cached features
                                    if let Some(feature_name) = self.get_current_feature_name() {
                                        self.try_auto_load_cached_feature(&feature_name).await;
                                    }
                                }
                            } else {
                                let max_index = match self.current_level {
                                    ViewLevel::Dataset => self.dataset_list.len().saturating_sub(1),
                                    ViewLevel::Series => self.series_list.len().saturating_sub(1),
                                    ViewLevel::Features => self.features_list.len().saturating_sub(1),
                                };
                                if self.selected_index < max_index {
                                    self.selected_index += 1;
                                    self.list_state.select(Some(self.selected_index));
                                }
                            }
                        }
                        KeyCode::F(1) => {
                            self.show_preview = !self.show_preview;
                        }
                        KeyCode::F(2) => {
                            if self.current_level == ViewLevel::Dataset {
                                // Cycle through sorting modes
                                self.dataset_sort_mode = match self.dataset_sort_mode {
                                    DatasetSortMode::Name => DatasetSortMode::SeriesCount,
                                    DatasetSortMode::SeriesCount => DatasetSortMode::RecordCount,
                                    DatasetSortMode::RecordCount => DatasetSortMode::Name,
                                };
                                // Re-sort the list and reset selection
                                self.sort_dataset_list();
                                self.selected_index = 0;
                                self.list_state.select(Some(self.selected_index));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        }
    }
    
    async fn update_search(&mut self) -> Result<()> {
        debug!("Updating search for query: '{}'", self.query);
        
        // Use very large limits to handle massive searches - we have 1M+ series total
        // Empty queries return all series (no limit needed at DB level)
        let limit = Some(2000000); // Large enough to handle over 1 million series
        
        // Perform search using existing search engine
        let search_query = crate::search::create_search_query(self.query.clone(), limit);
        
        // Add timeout and error handling for search
        let search_result = tokio::time::timeout(
            std::time::Duration::from_secs(60), // Large timeout for processing over 1 million series
            self.search_engine.search(search_query)
        ).await;
        
        let search_summary = match search_result {
            Ok(Ok(summary)) => {
                debug!("Search completed successfully: {} results", summary.results.len());
                summary
            },
            Ok(Err(e)) => {
                debug!("Search error: {}", e);
                // Return early with empty results on search error
                self.dataset_groups.clear();
                self.dataset_list.clear();
                self.selected_index = 0;
                self.list_state.select(Some(0));
                return Ok(());
            }
            Err(_) => {
                debug!("Search timeout after 5 seconds");
                // Return early with empty results on timeout
                self.dataset_groups.clear();
                self.dataset_list.clear();
                self.selected_index = 0;
                self.list_state.select(Some(0));
                return Ok(());
            }
        };
        
        // Group search results by dataset - no filtering needed since search_index has no pseudo-entries
        self.dataset_groups.clear();
        
        for result in search_summary.results {
            let dataset_name = result.search_result.dataset_name
                .clone()
                .unwrap_or_else(|| result.search_result.dataset_id.clone());
            
            let group = self.dataset_groups.entry(dataset_name.clone()).or_insert_with(|| {
                DatasetGroup {
                    name: dataset_name.clone(),
                    series: Vec::new(),
                    total_records: 0,
                }
            });
            
            // Add all series - no filtering needed since search_index only contains legitimate series
            group.series.push(result.clone());
            group.total_records += result.search_result.record_count;
            debug!("Added series {} to dataset {} (record_count: {})", 
                   result.search_result.series_id, dataset_name, result.search_result.record_count);
        }
        
        // Update dataset list for display
        self.dataset_list = self.dataset_groups.keys().cloned().collect();
        self.sort_dataset_list();
        
        // Reset to dataset level if we have new results
        if self.current_level != ViewLevel::Dataset {
            self.current_level = ViewLevel::Dataset;
            self.current_dataset = None;
            self.current_series = None;
        }
        
        // Reset selection if results changed
        if self.selected_index >= self.dataset_list.len() {
            self.selected_index = 0;
        }
        self.list_state.select(Some(self.selected_index));
        
        info!("Search updated: {} datasets with {} total series", 
              self.dataset_groups.len(),
              self.dataset_groups.values().map(|g| g.series.len()).sum::<usize>());
        Ok(())
    }
    
    /// Sort the dataset list according to the current sort mode
    fn sort_dataset_list(&mut self) {
        match self.dataset_sort_mode {
            DatasetSortMode::Name => {
                self.dataset_list.sort();
            }
            DatasetSortMode::SeriesCount => {
                self.dataset_list.sort_by(|a, b| {
                    let a_count = self.dataset_groups.get(a).map(|g| g.series.len()).unwrap_or(0);
                    let b_count = self.dataset_groups.get(b).map(|g| g.series.len()).unwrap_or(0);
                    b_count.cmp(&a_count) // Descending order (most series first)
                });
            }
            DatasetSortMode::RecordCount => {
                self.dataset_list.sort_by(|a, b| {
                    let a_records = self.dataset_groups.get(a).map(|g| g.total_records).unwrap_or(0);
                    let b_records = self.dataset_groups.get(b).map(|g| g.total_records).unwrap_or(0);
                    b_records.cmp(&a_records) // Descending order (most records first)
                });
            }
        }
    }
    
    async fn enter_dataset(&mut self) -> Result<()> {
        if let Some(dataset_name) = self.dataset_list.get(self.selected_index) {
            self.current_dataset = Some(dataset_name.clone());
            self.current_level = ViewLevel::Series;
            
            // Load series for this dataset
            if let Some(group) = self.dataset_groups.get(dataset_name) {
                self.series_list = group.series.clone();
            }
            
            self.selected_index = 0;
            self.list_state.select(Some(0));
        }
        Ok(())
    }
    
    async fn enter_series(&mut self) -> Result<()> {
        eprintln!("[DEBUG {}] 🚀 enter_series called, selected_index: {}, series_list.len(): {}", chrono::Utc::now().format("%H:%M:%S"), self.selected_index, self.series_list.len());
        
        if let Some(series) = self.series_list.get(self.selected_index) {
            let dataset_id = series.search_result.dataset_id.clone();
            let series_id = series.search_result.series_id.clone();
            
            eprintln!("[DEBUG {}] 📝 Setting up features view for series: {}, dataset: {}", chrono::Utc::now().format("%H:%M:%S"), series_id, dataset_id);
            
            self.current_series = Some(series_id);
            self.current_level = ViewLevel::Features;
            
            // Load actual features from database
            eprintln!("[DEBUG {}] 🔄 About to call load_features_for_dataset...", chrono::Utc::now().format("%H:%M:%S"));
            self.load_features_for_dataset(&dataset_id).await?;
            
            // Start at the first selectable feature (skip headers/spacers)
            self.selected_index = 0;
            let first_selectable = self.find_next_selectable(0, 1);
            if first_selectable != 0 || self.is_feature_selectable(0) {
                self.selected_index = if self.is_feature_selectable(0) { 0 } else { first_selectable };
            }
            self.list_state.select(Some(self.selected_index));
            
            eprintln!("[DEBUG {}] ✅ enter_series completed successfully", chrono::Utc::now().format("%H:%M:%S"));
        } else {
            eprintln!("[DEBUG {}] ❌ No series found at selected_index: {}", chrono::Utc::now().format("%H:%M:%S"), self.selected_index);
        }
        Ok(())
    }
    
    async fn load_features_for_dataset(&mut self, dataset_id: &str) -> Result<()> {
        eprintln!("[DEBUG {}] 🔍 load_features_for_dataset called with dataset_id: '{}'", chrono::Utc::now().format("%H:%M:%S"), dataset_id);
        
        // Get enhanced dataset metadata
        eprintln!("[DEBUG {}] 📞 About to call get_enhanced_dataset_column_metadata...", chrono::Utc::now().format("%H:%M:%S"));
        match self.db.get_enhanced_dataset_column_metadata(dataset_id) {
            Ok(Some(enhanced_info)) => {
                eprintln!("[DEBUG {}] ✅ Enhanced metadata found with {} features", chrono::Utc::now().format("%H:%M:%S"), enhanced_info.features.len());
                // Group features by attribute type
                let mut feature_categories = HashMap::new();
                
                for feature in enhanced_info.features {
                    let category_name = match feature.attribute {
                        FeatureAttribute::Targets => "Targets",
                        FeatureAttribute::HistoricalCovariates => "Historical Covariates", 
                        FeatureAttribute::FutureCovariates => "Future Covariates",
                        FeatureAttribute::StaticCovariates => "Static Covariates",
                    };
                    
                    feature_categories.entry(category_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(feature);
                }
                
                // Create segmented list with category headers and individual features
                let mut all_features = Vec::new();
                self.features_list.clear();
                
                // Add features in specific order with category headers
                let category_order = vec![
                    ("Targets", "🎯"),
                    ("Historical Covariates", "📊"), 
                    ("Static Covariates", "📐"),
                    ("Future Covariates", "🔮")
                ];
                
                for &(category_name, icon) in &category_order {
                    if let Some(features) = feature_categories.remove(category_name) {
                        if !features.is_empty() {
                            // Store the first attribute for header and spacer
                            let first_attribute = features[0].attribute.clone();
                            
                            // Add category header (non-selectable display item)
                            self.features_list.push(format!("─── {} {} ───", icon, category_name.to_uppercase()));
                            all_features.push(FeatureMetadata {
                                name: format!("HEADER: {}", category_name),
                                modality: crate::db::Modality::Categorical,
                                temporality: crate::db::Temporality::Static,
                                attribute: first_attribute.clone(),
                                scope: crate::db::Scope::Global,
                                data_type: "header".to_string(),
                                description: None,
                            });
                            
                            // Add individual features under this category
                            for feature in features {
                                let modality_str = match feature.modality {
                                    crate::db::Modality::Numerical => "Numeric",
                                    crate::db::Modality::Categorical => "Categorical",
                                };
                                let temporality_str = match feature.temporality {
                                    crate::db::Temporality::Static => "Static",
                                    crate::db::Temporality::Dynamic => "Dynamic",
                                };
                                
                                let display_text = format!("  {} {} ({} | {})", 
                                    icon, feature.name, modality_str, temporality_str);
                                self.features_list.push(display_text);
                                all_features.push(feature);
                            }
                            
                            // Add spacing between categories
                            if category_name != "Future Covariates" { // Don't add space after last category
                                self.features_list.push("".to_string());
                                all_features.push(FeatureMetadata {
                                    name: "SPACER".to_string(),
                                    modality: crate::db::Modality::Categorical,
                                    temporality: crate::db::Temporality::Static,
                                    attribute: first_attribute.clone(),
                                    scope: crate::db::Scope::Global,
                                    data_type: "spacer".to_string(),
                                    description: None,
                                });
                            }
                        }
                    }
                }
                
                // Add any remaining features not in the standard order
                for (_category_name, features) in feature_categories {
                    for feature in features {
                        let modality_str = match feature.modality {
                            crate::db::Modality::Numerical => "Numeric",
                            crate::db::Modality::Categorical => "Categorical",
                        };
                        let temporality_str = match feature.temporality {
                            crate::db::Temporality::Static => "Static",
                            crate::db::Temporality::Dynamic => "Dynamic",
                        };
                        
                        let display_text = format!("📋 {} ({} | {})", 
                            feature.name, modality_str, temporality_str);
                        self.features_list.push(display_text);
                        all_features.push(feature);
                    }
                }
                
                // Store both the enhanced info and the flat features list
                self.current_features_data = Some(all_features);
                
                eprintln!("[DEBUG {}] ✅ Loaded {} individual features for dataset {}", 
                         chrono::Utc::now().format("%H:%M:%S"), self.features_list.len(), dataset_id);
            }
            Ok(None) => {
                eprintln!("[DEBUG {}] ❌ No enhanced metadata found for dataset: {}", chrono::Utc::now().format("%H:%M:%S"), dataset_id);
                // Fallback to basic features
                self.features_list = vec![
                    "📋 Basic Features: 2 features".to_string(),
                ];
                self.current_features = None;
                self.current_features_data = None;
            }
            Err(e) => {
                eprintln!("[DEBUG {}] 💥 Error loading features for dataset {}: {}", chrono::Utc::now().format("%H:%M:%S"), dataset_id, e);
                // Fallback to basic features
                self.features_list = vec![
                    "📋 Error loading features".to_string(),
                ];
                self.current_features = None;
                self.current_features_data = None;
            }
        }
        
        Ok(())
    }
    
    async fn go_back_to_dataset(&mut self) -> Result<()> {
        self.current_level = ViewLevel::Dataset;
        self.current_dataset = None;
        self.current_series = None;
        self.current_features = None;
        // Re-sort the dataset list according to current sort mode
        self.sort_dataset_list();
        self.selected_index = 0;
        self.list_state.select(Some(0));
        Ok(())
    }
    
    async fn go_back_to_series(&mut self) -> Result<()> {
        self.current_level = ViewLevel::Series;
        self.current_series = None;
        self.current_features = None;
        self.selected_index = 0;
        self.list_state.select(Some(0));
        Ok(())
    }
    
    fn ui(&mut self, f: &mut Frame) {
        let area = f.area();
        let chunks = if self.show_preview {
            Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                .split(area)
        } else {
            vec![area].into()
        };
        
        // Left panel: search input and results
        let left_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
            .split(chunks[0]);
        
        // Search input with level indicator
        let level_name = match self.current_level {
            ViewLevel::Dataset => "Datasets",
            ViewLevel::Series => "Series",
            ViewLevel::Features => "Features",
        };
        
        let result_count = match self.current_level {
            ViewLevel::Dataset => self.dataset_list.len(),
            ViewLevel::Series => self.series_list.len(),
            ViewLevel::Features => self.features_list.len(),
        };
        
        let title = if let Some(dataset) = &self.current_dataset {
            if let Some(series) = &self.current_series {
                format!(" Search > {} > {} ({} {}) ", dataset, series, Self::format_number(result_count), level_name.to_lowercase())
            } else {
                format!(" Search > {} ({} {}) ", dataset, Self::format_number(result_count), level_name.to_lowercase())
            }
        } else {
            // Show sort mode when at dataset level
            let sort_indicator = if self.current_level == ViewLevel::Dataset {
                match self.dataset_sort_mode {
                    DatasetSortMode::Name => " [Sort: Name]",
                    DatasetSortMode::SeriesCount => " [Sort: Series Count]",
                    DatasetSortMode::RecordCount => " [Sort: Record Count]",
                }
            } else {
                ""
            };
            format!(" Search ({} {}){} ", Self::format_number(result_count), level_name.to_lowercase(), sort_indicator)
        };
        
        // Determine what to display in the input field
        let (input_text, input_style, input_title) = if self.number_input_mode && self.current_level == ViewLevel::Features {
            // Number input mode
            let display_text = if self.number_input_buffer.is_empty() {
                "Type feature number + Space".to_string()
            } else {
                format!("Feature #{}", self.number_input_buffer)
            };
            let mode_title = format!("{} [NUMBER MODE]", title);
            (display_text, Style::default().fg(Color::Green), mode_title)
        } else {
            // Regular search mode
            (self.query.clone(), Style::default().fg(Color::Yellow), title)
        };
        
        let input = Paragraph::new(input_text.as_str())
            .style(input_style)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(input_title)
            );
        f.render_widget(input, left_chunks[0]);
        
        // Results list based on current level
        let items: Vec<ListItem> = match self.current_level {
            ViewLevel::Dataset => {
                self.dataset_list
                    .iter()
                    .enumerate()
                    .map(|(i, dataset_name)| {
                        let group = self.dataset_groups.get(dataset_name).unwrap();
                        let content = format!("📊 {} ({} matching series, {} records)", 
                            dataset_name,
                            Self::format_number(group.series.len()),
                            Self::format_number(group.total_records)
                        );
                        let style = if i == self.selected_index {
                            Style::default().bg(Color::Blue).fg(Color::White)
                        } else {
                            Style::default()
                        };
                        ListItem::new(content).style(style)
                    })
                    .collect()
            }
            ViewLevel::Series => {
                self.series_list
                    .iter()
                    .enumerate()
                    .map(|(i, result)| {
                        let content = format!("📈 {} [{} records]", 
                            result.search_result.series_id,
                            Self::format_number(result.search_result.record_count)
                        );
                        let style = if i == self.selected_index {
                            Style::default().bg(Color::Blue).fg(Color::White)
                        } else {
                            Style::default()
                        };
                        ListItem::new(content).style(style)
                    })
                    .collect()
            }
            ViewLevel::Features => {
                self.features_list
                    .iter()
                    .enumerate()
                    .map(|(i, feature)| {
                        let is_header = feature.starts_with("───");
                        let is_spacer = feature.is_empty();
                        let is_selectable = self.is_feature_selectable(i);
                        
                        let content = if is_header {
                            // Header styling - no prefix
                            feature.clone()
                        } else if is_spacer {
                            // Spacer - just empty
                            "".to_string()
                        } else {
                            // Regular feature - no automatic prefixes
                            feature.clone()
                        };
                        
                        let style = if is_header {
                            // Headers are bold and colored but not selectable
                            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
                        } else if is_spacer {
                            // Spacers are invisible
                            Style::default()
                        } else if i == self.selected_index && is_selectable {
                            // Selected feature
                            Style::default().bg(Color::Blue).fg(Color::White)
                        } else {
                            // Regular feature
                            Style::default()
                        };
                        
                        ListItem::new(content).style(style)
                    })
                    .collect()
            }
        };
        
        let results_list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(" Results "))
            .highlight_style(Style::default().add_modifier(Modifier::BOLD))
            .highlight_symbol("▶ ");
        
        f.render_stateful_widget(results_list, left_chunks[1], &mut self.list_state);
        
        // Status line with hierarchical navigation help
        let help_text = match self.current_level {
            ViewLevel::Dataset => "↑/↓: navigate, Enter: enter dataset, F1: toggle preview, F2: toggle sort, Esc: quit",
            ViewLevel::Series => "↑/↓: navigate, Enter: view features, F1: toggle preview, Esc: back to datasets", 
            ViewLevel::Features => {
                if self.number_input_mode {
                    "Number + Space: select feature, Tab: toggle search mode, Enter: load data, Esc: back"
                } else {
                    "↑/↓: navigate, Tab: toggle number mode, Enter: load data, F1: toggle preview, Esc: back"
                }
            },
        };
        let status = Paragraph::new(help_text)
            .style(Style::default().bg(Color::DarkGray).fg(Color::White));
        f.render_widget(status, left_chunks[2]);
        
        // Right panel: preview (if enabled)
        if self.show_preview && chunks.len() > 1 {
            self.render_preview(f, chunks[1]);
        }
    }
    
    // Removed format_result_line method to fix borrowing issues
    // Formatting is now done inline in the ui method
    
    fn render_preview(&mut self, f: &mut Frame, area: Rect) {
        let mut preview_lines = Vec::new();
        
        match self.current_level {
            ViewLevel::Dataset => {
                if let Some(dataset_name) = self.dataset_list.get(self.selected_index) {
                    if let Some(group) = self.dataset_groups.get(dataset_name) {
                        // Dataset preview
                        preview_lines.push(Line::from(Span::styled(
                            "Dataset Overview",
                            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                        )));
                        preview_lines.push(Line::from(""));
                        
                        preview_lines.push(Line::from(vec![
                            Span::styled("Name: ", Style::default().fg(Color::Cyan)),
                            Span::raw(&group.name),
                        ]));
                        
                        preview_lines.push(Line::from(vec![
                            Span::styled("Series Count: ", Style::default().fg(Color::Cyan)),
                            Span::raw(Self::format_number(group.series.len())),
                        ]));
                        
                        preview_lines.push(Line::from(vec![
                            Span::styled("Total Records: ", Style::default().fg(Color::Cyan)),
                            Span::raw(Self::format_number(group.total_records)),
                        ]));
                        
                        // Show some sample series
                        if !group.series.is_empty() {
                            preview_lines.push(Line::from(""));
                            preview_lines.push(Line::from(vec![
                                Span::styled("Sample Series:", Style::default().fg(Color::Cyan)),
                            ]));
                            
                            for (i, series) in group.series.iter().take(5).enumerate() {
                                preview_lines.push(Line::from(format!(
                                    "{}. {} ({} records)",
                                    i + 1,
                                    series.search_result.series_id,
                                    Self::format_number(series.search_result.record_count)
                                )));
                            }
                            
                            if group.series.len() > 5 {
                                preview_lines.push(Line::from(format!("... and {} more", Self::format_number(group.series.len() - 5))));
                            }
                        }
                    }
                }
            }
            ViewLevel::Series => {
                if let Some(result) = self.series_list.get(self.selected_index) {
                    // Series preview
                    preview_lines.push(Line::from(Span::styled(
                        "Series Details",
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                    )));
                    preview_lines.push(Line::from(""));
                    
                    preview_lines.push(Line::from(vec![
                        Span::styled("ID: ", Style::default().fg(Color::Cyan)),
                        Span::raw(&result.search_result.series_id),
                    ]));
                    
                    if let Some(dataset_name) = &result.search_result.dataset_name {
                        preview_lines.push(Line::from(vec![
                            Span::styled("Dataset: ", Style::default().fg(Color::Cyan)),
                            Span::raw(dataset_name),
                        ]));
                    }
                    
                    if let Some(theme) = &result.search_result.theme {
                        preview_lines.push(Line::from(vec![
                            Span::styled("Theme: ", Style::default().fg(Color::Cyan)),
                            Span::raw(theme),
                        ]));
                    }
                    
                    preview_lines.push(Line::from(vec![
                        Span::styled("Records: ", Style::default().fg(Color::Cyan)),
                        Span::raw(Self::format_number(result.search_result.record_count)),
                    ]));
                    
                    preview_lines.push(Line::from(vec![
                        Span::styled("Date Range: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!(
                            "{} to {}",
                            result.search_result.first_timestamp.format("%Y-%m-%d"),
                            result.search_result.last_timestamp.format("%Y-%m-%d")
                        )),
                    ]));
                    
                    if let Some(description) = &result.search_result.description {
                        preview_lines.push(Line::from(""));
                        preview_lines.push(Line::from(vec![
                            Span::styled("Description:", Style::default().fg(Color::Cyan)),
                        ]));
                        preview_lines.push(Line::from(description.clone()));
                    }
                }
            }
            ViewLevel::Features => {
                if let Some(features_data) = &self.current_features_data {
                    if let Some(selected_feature_display) = self.features_list.get(self.selected_index) {
                        // Get the actual feature from the features list
                        if let Some(selected_feature) = features_data.get(self.selected_index) {
                            // Skip headers and spacers
                            if selected_feature.name.starts_with("HEADER:") || selected_feature.name == "SPACER" {
                                preview_lines.push(Line::from(Span::styled(
                                    "📋 FEATURE CATEGORY",
                                    Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                                )));
                                preview_lines.push(Line::from(""));
                                preview_lines.push(Line::from("Select an individual feature to view details"));
                            } else {
                            // Feature header with icon and name
                            let icon = if selected_feature_display.starts_with("🎯") { "🎯" }
                                     else if selected_feature_display.starts_with("📊") { "📊" }
                                     else if selected_feature_display.starts_with("📐") { "📐" }
                                     else if selected_feature_display.starts_with("🔮") { "🔮" }
                                     else { "📋" };
                                     
                            preview_lines.push(Line::from(Span::styled(
                                format!("{} FEATURE DETAILS", icon),
                                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                            )));
                            preview_lines.push(Line::from(""));
                            
                            // Feature name
                            preview_lines.push(Line::from(vec![
                                Span::styled("Name: ", Style::default().fg(Color::Cyan)),
                                Span::styled(selected_feature.name.clone(), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
                            ]));
                            
                            // Feature type information
                            let modality_str = match selected_feature.modality {
                                crate::db::Modality::Numerical => "Numerical",
                                crate::db::Modality::Categorical => "Categorical",
                            };
                            preview_lines.push(Line::from(vec![
                                Span::styled("Type: ", Style::default().fg(Color::Cyan)),
                                Span::raw(modality_str),
                            ]));
                            
                            let temporality_str = match selected_feature.temporality {
                                crate::db::Temporality::Static => "Static",
                                crate::db::Temporality::Dynamic => "Dynamic",
                            };
                            preview_lines.push(Line::from(vec![
                                Span::styled("Temporality: ", Style::default().fg(Color::Cyan)),
                                Span::raw(temporality_str),
                            ]));
                            
                            let attribute_str = match selected_feature.attribute {
                                FeatureAttribute::Targets => "Target Variable",
                                FeatureAttribute::HistoricalCovariates => "Historical Covariate",
                                FeatureAttribute::FutureCovariates => "Future Covariate", 
                                FeatureAttribute::StaticCovariates => "Static Covariate",
                            };
                            preview_lines.push(Line::from(vec![
                                Span::styled("Category: ", Style::default().fg(Color::Cyan)),
                                Span::raw(attribute_str),
                            ]));
                            
                            preview_lines.push(Line::from(""));
                            
                            // Additional metadata note
                            preview_lines.push(Line::from(Span::styled(
                                "📊 METADATA:",
                                Style::default().fg(Color::Blue).add_modifier(Modifier::BOLD),
                            )));
                            preview_lines.push(Line::from(vec![
                                Span::styled("Source: ", Style::default().fg(Color::Cyan)),
                                Span::raw("Enhanced dataset analysis"),
                            ]));
                            preview_lines.push(Line::from(""));
                            preview_lines.push(Line::from(Span::styled(
                                "Note: Real S3 data will replace placeholder chart",
                                Style::default().fg(Color::DarkGray),
                            )));
                            }
                        }
                    }
                } else {
                    // Fallback for when no enhanced features are available
                    preview_lines.push(Line::from(Span::styled(
                        "Basic Features", 
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                    )));
                    preview_lines.push(Line::from(""));
                    preview_lines.push(Line::from("No detailed feature metadata available"));
                    preview_lines.push(Line::from("This dataset may need enhanced indexing"));
                }
            }
        }
        
        // Handle Features view with chart layout separately
        if self.current_level == ViewLevel::Features && self.current_features_data.is_some() {
            // Extract the selected feature data without borrowing self
            let selected_feature_data = if let Some(features_data) = &self.current_features_data {
                if let Some(selected_feature) = features_data.get(self.selected_index) {
                    if !selected_feature.name.starts_with("HEADER:") && selected_feature.name != "SPACER" {
                        Some((selected_feature.name.clone(), selected_feature.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };
            
            if let Some((feature_name, selected_feature)) = selected_feature_data {
                // Split the preview area: top for text, bottom for chart
                let preview_chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Min(6),      // Text area minimum (reduced)
                        Constraint::Length(16),  // Chart area bigger height
                    ])
                    .split(area);
                
                // Render text content
                let preview = Paragraph::new(Text::from(preview_lines))
                    .block(Block::default().borders(Borders::ALL).title(" Feature Details "))
                    .wrap(Wrap { trim: true });
                f.render_widget(preview, preview_chunks[0]);
                
                // Render chart or static value display
                self.render_feature_chart(f, preview_chunks[1], &feature_name, &selected_feature);
                return;
            }
        }
        
        // Default preview rendering for all other cases
        let preview = Paragraph::new(Text::from(preview_lines))
            .block(Block::default().borders(Borders::ALL).title(" Preview "))
            .wrap(Wrap { trim: true });
        
        f.render_widget(preview, area);
    }
    
    /// Start fetching data for the currently selected feature (non-blocking)
    pub fn start_feature_data_fetch(&mut self, feature_name: &str) -> bool {
        if let Some(series_id) = &self.current_series {
            let cache_key = format!("{}::{}", series_id, feature_name);
            
            // Don't start if already loading or cached
            if self.loading_features.contains(&cache_key) || 
               self.feature_data_cache.contains_key(&cache_key) {
                return false;
            }
            
            // Mark as loading
            self.loading_features.insert(cache_key.clone());
            
            // Clear any previous error
            self.data_fetch_errors.remove(&cache_key);
            
            return true;
        }
        false
    }
    
    /// Complete data fetch operation (to be called after async fetch completes)
    pub fn complete_feature_data_fetch(&mut self, feature_name: &str, result: Result<Vec<(f64, f64)>>) {
        if let Some(series_id) = &self.current_series {
            let cache_key = format!("{}::{}", series_id, feature_name);
            
            // Remove from loading set
            self.loading_features.remove(&cache_key);
            
            match result {
                Ok(data) => {
                    // Cache the successful result
                    self.feature_data_cache.insert(cache_key.clone(), data);
                    self.data_fetch_errors.remove(&cache_key);
                }
                Err(error) => {
                    // Store the error
                    self.data_fetch_errors.insert(cache_key, error.to_string());
                }
            }
        }
    }
    
    /// Process data result from async channel
    fn process_data_result(&mut self, result: FeatureDataResult) {
        match result {
            FeatureDataResult::Success { feature_name, series_id, data } => {
                let cache_key = format!("{}::{}", series_id, feature_name);
                
                eprintln!("[DEBUG {}] 📬 Received successful data for feature: {} ({} points)", 
                    chrono::Utc::now().format("%H:%M:%S"), feature_name, data.len());
                
                // Remove from loading set
                self.loading_features.remove(&cache_key);
                
                // Cache the successful result
                self.feature_data_cache.insert(cache_key.clone(), data);
                self.data_fetch_errors.remove(&cache_key);
            }
            FeatureDataResult::Error { feature_name, series_id, error } => {
                let cache_key = format!("{}::{}", series_id, feature_name);
                
                eprintln!("[DEBUG {}] 📬 Received error for feature: {} - {}", 
                    chrono::Utc::now().format("%H:%M:%S"), feature_name, error);
                
                // Remove from loading set
                self.loading_features.remove(&cache_key);
                
                // Store the error
                self.data_fetch_errors.insert(cache_key, error);
            }
        }
    }
    
    /// Check if the currently visible feature needs data fetching
    pub fn current_feature_needs_fetch(&self) -> Option<String> {
        if self.current_level != ViewLevel::Features {
            return None;
        }
        
        if let Some(features_data) = &self.current_features_data {
            if let Some(selected_feature) = features_data.get(self.selected_index) {
                if !selected_feature.name.starts_with("HEADER:") && selected_feature.name != "SPACER" {
                    let cache_key = self.get_cache_key(&selected_feature.name);
                    
                    // Check if needs fetching (applies to both dynamic and static features)
                    if !self.feature_data_cache.contains_key(&cache_key) &&
                       !self.loading_features.contains(&cache_key) &&
                       !self.data_fetch_errors.contains_key(&cache_key) {
                        return Some(selected_feature.name.clone());
                    }
                }
            }
        }
        
        None
    }
    
    /// Retry fetching data for a feature that had an error
    pub fn retry_feature_fetch(&mut self, feature_name: &str) -> bool {
        if let Some(series_id) = &self.current_series {
            let cache_key = format!("{}::{}", series_id, feature_name);
            
            // Clear error and try again
            self.data_fetch_errors.remove(&cache_key);
            self.start_feature_data_fetch(feature_name)
        } else {
            false
        }
    }
    
    /// Get the index of the nth selectable feature (0-based)
    fn get_nth_selectable_feature(&self, n: usize) -> Option<usize> {
        let mut selectable_count = 0;
        
        for (i, feature) in self.features_list.iter().enumerate() {
            if self.is_feature_selectable(i) {
                if selectable_count == n {
                    return Some(i);
                }
                selectable_count += 1;
            }
        }
        
        None
    }
    
    /// Get the current feature name at the selected index
    fn get_current_feature_name(&self) -> Option<String> {
        if let Some(feature_text) = self.features_list.get(self.selected_index) {
            // Extract feature name from the display text
            // Format is usually "🔧 feature_name [X records]" or similar
            let parts: Vec<&str> = feature_text.split_whitespace().collect();
            if parts.len() >= 2 {
                // Get the feature name (second part after emoji)
                let feature_name = parts[1];
                // Remove any brackets or metadata
                let clean_name = feature_name.split('[').next().unwrap_or(feature_name);
                return Some(clean_name.to_string());
            }
        }
        None
    }
    
    /// Try to auto-load a cached feature without requiring Enter key
    async fn try_auto_load_cached_feature(&mut self, feature_name: &str) {
        if let Some(series_id) = &self.current_series {
            let cache_key = format!("{}::{}", series_id, feature_name);
            
            // Check if data is already cached
            if self.feature_data_cache.contains_key(&cache_key) {
                // Data is already cached, no action needed - it will display automatically
                eprintln!("[DEBUG {}] 🎯 Auto-displaying cached feature: {}", 
                    chrono::Utc::now().format("%H:%M:%S"), feature_name);
            } else {
                // Data not cached - could trigger fetch here if desired
                eprintln!("[DEBUG {}] 🔄 Feature not cached, press Enter to load: {}", 
                    chrono::Utc::now().format("%H:%M:%S"), feature_name);
            }
        }
    }
}