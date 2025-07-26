use anyhow::Result;
use fuzzy_matcher::{skim::SkimMatcherV2, FuzzyMatcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use crate::db::{Database, SearchResult};

pub struct SearchEngine {
    db: Database,
    fuzzy_matcher: SkimMatcherV2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: String,
    pub limit: usize,
    pub filters: SearchFilters,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SearchFilters {
    pub dataset_ids: Option<Vec<String>>,
    pub themes: Option<Vec<String>>,
    pub date_range: Option<DateRange>,
    pub min_records: Option<i64>,
    pub max_records: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DateRange {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnhancedSearchResult {
    pub search_result: SearchResult,
    pub fuzzy_score: Option<i64>,
    pub relevance_score: f64,
    pub match_reasons: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchSummary {
    pub total_results: usize,
    pub search_time_ms: u128,
    pub top_themes: HashMap<String, usize>,
    pub top_datasets: HashMap<String, usize>,
    pub results: Vec<EnhancedSearchResult>,
}

impl SearchEngine {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            fuzzy_matcher: SkimMatcherV2::default(),
        }
    }
    
    /// Perform a comprehensive search across all indexed data
    pub async fn search(&self, query: SearchQuery) -> Result<SearchSummary> {
        let start_time = std::time::Instant::now();
        info!("Performing search for: '{}'", query.text);
        
        // Get base results from database
        let base_results = self.db.search_series(&query.text, query.limit * 2)?; // Get more for ranking
        
        // Apply additional filtering
        let filtered_results = self.apply_filters(base_results, &query.filters);
        
        // Enhance results with fuzzy scoring and ranking
        let mut enhanced_results = self.enhance_results(filtered_results, &query.text);
        
        // Sort by relevance score and limit
        enhanced_results.sort_by(|a, b| b.relevance_score.partial_cmp(&a.relevance_score).unwrap());
        enhanced_results.truncate(query.limit);
        
        // Generate summary statistics
        let summary = self.generate_search_summary(
            enhanced_results,
            start_time.elapsed().as_millis(),
        );
        
        info!("Search completed: {} results in {}ms", 
              summary.results.len(), summary.search_time_ms);
        
        Ok(summary)
    }
    
    /// Apply additional filters to search results
    fn apply_filters(&self, results: Vec<SearchResult>, filters: &SearchFilters) -> Vec<SearchResult> {
        results.into_iter()
            .filter(|result| {
                // Dataset ID filter
                if let Some(dataset_ids) = &filters.dataset_ids {
                    if !dataset_ids.contains(&result.dataset_id) {
                        return false;
                    }
                }
                
                // Theme filter
                if let Some(themes) = &filters.themes {
                    if let Some(theme) = &result.theme {
                        if !themes.contains(theme) {
                            return false;
                        }
                    } else {
                        return false; // No theme but filter requires one
                    }
                }
                
                // Record count filters
                if let Some(min_records) = filters.min_records {
                    if result.record_count < min_records {
                        return false;
                    }
                }
                
                if let Some(max_records) = filters.max_records {
                    if result.record_count > max_records {
                        return false;
                    }
                }
                
                // Date range filter
                if let Some(date_range) = &filters.date_range {
                    if result.last_timestamp < date_range.start || 
                       result.first_timestamp > date_range.end {
                        return false;
                    }
                }
                
                true
            })
            .collect()
    }
    
    /// Enhance search results with fuzzy scoring and relevance ranking
    fn enhance_results(&self, results: Vec<SearchResult>, query: &str) -> Vec<EnhancedSearchResult> {
        results.into_iter()
            .map(|result| {
                let (fuzzy_score, match_reasons) = self.calculate_fuzzy_score(&result, query);
                let relevance_score = self.calculate_relevance_score(&result, fuzzy_score);
                
                EnhancedSearchResult {
                    search_result: result,
                    fuzzy_score,
                    relevance_score,
                    match_reasons,
                }
            })
            .collect()
    }
    
    /// Calculate fuzzy match score and identify match reasons
    fn calculate_fuzzy_score(&self, result: &SearchResult, query: &str) -> (Option<i64>, Vec<String>) {
        let mut best_score = None;
        let mut match_reasons = Vec::new();
        
        // Check series ID match
        if let Some(score) = self.fuzzy_matcher.fuzzy_match(&result.series_id, query) {
            best_score = Some(best_score.map_or(score, |s: i64| s.max(score)));
            match_reasons.push("Series ID".to_string());
        }
        
        // Check dataset name match
        if let Some(dataset_name) = &result.dataset_name {
            if let Some(score) = self.fuzzy_matcher.fuzzy_match(dataset_name, query) {
                best_score = Some(best_score.map_or(score, |s: i64| s.max(score)));
                match_reasons.push("Dataset Name".to_string());
            }
        }
        
        // Check theme match
        if let Some(theme) = &result.theme {
            if let Some(score) = self.fuzzy_matcher.fuzzy_match(theme, query) {
                best_score = Some(best_score.map_or(score, |s: i64| s.max(score)));
                match_reasons.push("Theme".to_string());
            }
        }
        
        // Check description match
        if let Some(description) = &result.description {
            if let Some(score) = self.fuzzy_matcher.fuzzy_match(description, query) {
                best_score = Some(best_score.map_or(score, |s: i64| s.max(score)));
                match_reasons.push("Description".to_string());
            }
        }
        
        // Check tags match
        if let Some(tags_text) = &result.tags_text {
            if let Some(score) = self.fuzzy_matcher.fuzzy_match(tags_text, query) {
                best_score = Some(best_score.map_or(score, |s: i64| s.max(score)));
                match_reasons.push("Tags".to_string());
            }
        }
        
        (best_score, match_reasons)
    }
    
    /// Calculate overall relevance score combining multiple factors
    fn calculate_relevance_score(&self, result: &SearchResult, fuzzy_score: Option<i64>) -> f64 {
        let mut score = 0.0;
        
        // Base fuzzy match score (0-100)
        if let Some(fs) = fuzzy_score {
            score += fs as f64 / 100.0 * 50.0; // Max 50 points for fuzzy match
        }
        
        // Bonus for larger datasets (more data = potentially more useful)
        let record_bonus = (result.record_count as f64).log10().min(10.0); // Max 10 points
        score += record_bonus;
        
        // Bonus for recent data
        let days_since_last = (chrono::Utc::now() - result.last_timestamp).num_days();
        let recency_bonus = if days_since_last < 30 {
            10.0 // Very recent
        } else if days_since_last < 365 {
            5.0 // Recent
        } else {
            0.0 // Old
        };
        score += recency_bonus;
        
        // Bonus for having theme and description
        if result.theme.is_some() {
            score += 5.0;
        }
        if result.description.is_some() {
            score += 5.0;
        }
        
        // Bonus for data span (longer time series might be more valuable)
        let span_days = (result.last_timestamp - result.first_timestamp).num_days();
        let span_bonus = (span_days as f64 / 365.0).min(5.0); // Max 5 points for multi-year data
        score += span_bonus;
        
        score
    }
    
    /// Generate comprehensive search summary
    fn generate_search_summary(&self, results: Vec<EnhancedSearchResult>, search_time_ms: u128) -> SearchSummary {
        let mut top_themes = HashMap::new();
        let mut top_datasets = HashMap::new();
        
        for result in &results {
            // Count themes
            if let Some(theme) = &result.search_result.theme {
                *top_themes.entry(theme.clone()).or_insert(0) += 1;
            }
            
            // Count datasets
            if let Some(dataset_name) = &result.search_result.dataset_name {
                *top_datasets.entry(dataset_name.clone()).or_insert(0) += 1;
            }
        }
        
        SearchSummary {
            total_results: results.len(),
            search_time_ms,
            top_themes,
            top_datasets,
            results,
        }
    }
    
    /// Get search suggestions based on partial query
    pub async fn get_suggestions(&self, partial_query: &str, limit: usize) -> Result<Vec<String>> {
        debug!("Getting suggestions for: '{}'", partial_query);
        
        // Get potential matches from database
        let results = self.db.search_series(partial_query, limit * 3)?;
        
        let mut suggestions = Vec::new();
        
        // Extract unique suggestions from various fields
        for result in results {
            // Add series ID if it's a good match
            if result.series_id.to_lowercase().contains(&partial_query.to_lowercase()) {
                suggestions.push(result.series_id);
            }
            
            // Add dataset name if it's a good match
            if let Some(dataset_name) = result.dataset_name {
                if dataset_name.to_lowercase().contains(&partial_query.to_lowercase()) {
                    suggestions.push(dataset_name);
                }
            }
            
            // Add theme if it's a good match
            if let Some(theme) = result.theme {
                if theme.to_lowercase().contains(&partial_query.to_lowercase()) {
                    suggestions.push(theme);
                }
            }
        }
        
        // Remove duplicates and limit results
        suggestions.sort();
        suggestions.dedup();
        suggestions.truncate(limit);
        
        Ok(suggestions)
    }
    
    /// Get available filter options
    pub async fn get_filter_options(&self) -> Result<FilterOptions> {
        // This would query the database for available themes, datasets, etc.
        // For now, return a simple implementation
        Ok(FilterOptions {
            themes: vec!["financial".to_string(), "environmental".to_string(), "business".to_string()],
            datasets: vec![], // Would be populated from actual data
            date_range: None, // Would be calculated from actual data
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterOptions {
    pub themes: Vec<String>,
    pub datasets: Vec<String>,
    pub date_range: Option<DateRange>,
}

/// Create a basic search query
pub fn create_search_query(text: String, limit: Option<usize>) -> SearchQuery {
    SearchQuery {
        text,
        limit: limit.unwrap_or(10),
        filters: SearchFilters::default(),
    }
}

/// Create a search query with filters
pub fn create_filtered_search_query(
    text: String,
    limit: Option<usize>,
    filters: SearchFilters,
) -> SearchQuery {
    SearchQuery {
        text,
        limit: limit.unwrap_or(10),
        filters,
    }
}