use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use std::{collections::HashMap, fs::OpenOptions};
use tracing::{debug, info};
use chrono;

use crate::db::Database;
use crate::search::{EnhancedSearchResult, SearchEngine};

#[derive(Debug, Clone, PartialEq)]
enum ViewLevel {
    Dataset,
    Series,
    Features,
}

#[derive(Debug, Clone)]
struct DatasetGroup {
    name: String,
    series: Vec<EnhancedSearchResult>,
    total_records: i64,
}

#[derive(Debug, Clone)]
struct SeriesFeatures {
    series_id: String,
    features: Vec<String>,
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
    
    // Display data for current level
    dataset_list: Vec<String>,
    series_list: Vec<EnhancedSearchResult>,
    features_list: Vec<String>,
}

impl InteractiveFinder {
    pub fn new(db: Database) -> Self {
        let search_engine = SearchEngine::new(db.clone());
        
        Self::with_search_engine(db, search_engine)
    }
    
    pub fn with_search_engine(db: Database, search_engine: SearchEngine) -> Self {
        
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
            
            // Initialize display lists
            dataset_list: Vec::new(),
            series_list: Vec::new(),
            features_list: Vec::new(),
        }
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
            
            // Use non-blocking event polling with a small timeout
            if crossterm::event::poll(std::time::Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    if key.kind == KeyEventKind::Press {
                        match key.code {
                        KeyCode::Char(c) => {
                            self.query.push(c);
                            needs_update = true;
                            last_query_change = std::time::Instant::now();
                        }
                        KeyCode::Backspace => {
                            self.query.pop();
                            needs_update = true;
                            last_query_change = std::time::Instant::now();
                        }
                        KeyCode::Enter => {
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
                                    // Return the currently selected series (the one we're viewing features for)
                                    if let Some(series_id) = &self.current_series {
                                        if let Some(dataset_name) = &self.current_dataset {
                                            if let Some(group) = self.dataset_groups.get(dataset_name) {
                                                if let Some(series) = group.series.iter().find(|s| s.search_result.series_id == *series_id) {
                                                    return Ok(Some(vec![series.clone()]));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        KeyCode::Tab => {
                            // TODO: Multi-select mode - for now, same as Enter
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
                                    if let Some(series_id) = &self.current_series {
                                        if let Some(dataset_name) = &self.current_dataset {
                                            if let Some(group) = self.dataset_groups.get(dataset_name) {
                                                if let Some(series) = group.series.iter().find(|s| s.search_result.series_id == *series_id) {
                                                    return Ok(Some(vec![series.clone()]));
                                                }
                                            }
                                        }
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
                            if self.selected_index > 0 {
                                self.selected_index -= 1;
                                self.list_state.select(Some(self.selected_index));
                            }
                        }
                        KeyCode::Down => {
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
                        KeyCode::F(1) => {
                            self.show_preview = !self.show_preview;
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
        
        // Perform search using existing search engine
        let search_query = crate::search::create_search_query(self.query.clone(), Some(1000));
        
        // Add timeout and error handling for search
        let search_result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
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
        
        // Group results by dataset
        self.dataset_groups.clear();
        for result in search_summary.results {
            let dataset_name = result.search_result.dataset_name
                .clone()
                .unwrap_or_else(|| result.search_result.dataset_id.clone());
            
            let group = self.dataset_groups.entry(dataset_name.clone()).or_insert_with(|| {
                DatasetGroup {
                    name: dataset_name,
                    series: Vec::new(),
                    total_records: 0,
                }
            });
            
            group.total_records += result.search_result.record_count;
            group.series.push(result);
        }
        
        // Update dataset list for display
        self.dataset_list = self.dataset_groups.keys().cloned().collect();
        self.dataset_list.sort();
        
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
        if let Some(series) = self.series_list.get(self.selected_index) {
            self.current_series = Some(series.search_result.series_id.clone());
            self.current_level = ViewLevel::Features;
            
            // TODO: Load actual features from database
            // For now, show some sample features
            self.features_list = vec![
                "timestamp".to_string(),
                "value".to_string(),
                "target".to_string(),
                "features".to_string(),
            ];
            
            self.selected_index = 0;
            self.list_state.select(Some(0));
        }
        Ok(())
    }
    
    async fn go_back_to_dataset(&mut self) -> Result<()> {
        self.current_level = ViewLevel::Dataset;
        self.current_dataset = None;
        self.current_series = None;
        self.selected_index = 0;
        self.list_state.select(Some(0));
        Ok(())
    }
    
    async fn go_back_to_series(&mut self) -> Result<()> {
        self.current_level = ViewLevel::Series;
        self.current_series = None;
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
                format!(" Search > {} > {} ({} {}) ", dataset, series, result_count, level_name.to_lowercase())
            } else {
                format!(" Search > {} ({} {}) ", dataset, result_count, level_name.to_lowercase())
            }
        } else {
            format!(" Search ({} {}) ", result_count, level_name.to_lowercase())
        };
        
        let input = Paragraph::new(self.query.as_str())
            .style(Style::default().fg(Color::Yellow))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
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
                        let content = format!("📊 {} ({} series, {} total records)", 
                            dataset_name,
                            group.series.len(),
                            group.total_records
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
                            result.search_result.record_count
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
                        let content = format!("🔧 {}", feature);
                        let style = if i == self.selected_index {
                            Style::default().bg(Color::Blue).fg(Color::White)
                        } else {
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
            ViewLevel::Dataset => "↑/↓: navigate, Enter: enter dataset, F1: toggle preview, Esc: quit",
            ViewLevel::Series => "↑/↓: navigate, Enter: view features, F1: toggle preview, Esc: back to datasets", 
            ViewLevel::Features => "↑/↓: navigate, Enter: select series, F1: toggle preview, Esc: back to series",
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
    
    fn render_preview(&self, f: &mut Frame, area: Rect) {
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
                            Span::raw(group.series.len().to_string()),
                        ]));
                        
                        preview_lines.push(Line::from(vec![
                            Span::styled("Total Records: ", Style::default().fg(Color::Cyan)),
                            Span::raw(group.total_records.to_string()),
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
                                    series.search_result.record_count
                                )));
                            }
                            
                            if group.series.len() > 5 {
                                preview_lines.push(Line::from(format!("... and {} more", group.series.len() - 5)));
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
                        Span::raw(result.search_result.record_count.to_string()),
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
                if let Some(feature) = self.features_list.get(self.selected_index) {
                    // Feature preview
                    preview_lines.push(Line::from(Span::styled(
                        "Feature Details",
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                    )));
                    preview_lines.push(Line::from(""));
                    
                    preview_lines.push(Line::from(vec![
                        Span::styled("Feature: ", Style::default().fg(Color::Cyan)),
                        Span::raw(feature),
                    ]));
                    
                    if let Some(series_id) = &self.current_series {
                        preview_lines.push(Line::from(vec![
                            Span::styled("Series: ", Style::default().fg(Color::Cyan)),
                            Span::raw(series_id),
                        ]));
                    }
                    
                    if let Some(dataset) = &self.current_dataset {
                        preview_lines.push(Line::from(vec![
                            Span::styled("Dataset: ", Style::default().fg(Color::Cyan)),
                            Span::raw(dataset),
                        ]));
                    }
                    
                    // TODO: Add actual feature metadata when available
                    preview_lines.push(Line::from(""));
                    preview_lines.push(Line::from(vec![
                        Span::styled("Type: ", Style::default().fg(Color::Cyan)),
                        Span::raw("Time Series Column"),  // Placeholder
                    ]));
                }
            }
        }
        
        let preview = Paragraph::new(Text::from(preview_lines))
            .block(Block::default().borders(Borders::ALL).title(" Preview "))
            .wrap(Wrap { trim: true });
        
        f.render_widget(preview, area);
    }
}