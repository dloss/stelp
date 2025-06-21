use crate::error::ProcessingError;
use crate::variables::GlobalVariables;
use serde_json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// A record that flows through the pipeline - either text or structured data
#[derive(Debug, Clone)]
pub enum RecordData {
    /// Text content (original line-based processing)
    Text(String),
    /// Structured data (JSON objects/arrays, CSV rows, etc.)
    Structured(serde_json::Value),
}

impl RecordData {
    /// Create a text record
    pub fn text(content: String) -> Self {
        RecordData::Text(content)
    }

    /// Create a structured record
    pub fn structured(data: serde_json::Value) -> Self {
        RecordData::Structured(data)
    }

    /// Check if this is a text record
    pub fn is_text(&self) -> bool {
        matches!(self, RecordData::Text(_))
    }

    /// Check if this is a structured record
    pub fn is_structured(&self) -> bool {
        matches!(self, RecordData::Structured(_))
    }

    /// Get text content if this is a text record
    pub fn as_text(&self) -> Option<&str> {
        match self {
            RecordData::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Get structured data if this is a structured record
    pub fn as_structured(&self) -> Option<&serde_json::Value> {
        match self {
            RecordData::Structured(data) => Some(data),
            _ => None,
        }
    }
}

/// Context passed to each processor for a record
pub struct RecordContext<'a> {
    pub line_number: usize,
    pub record_count: usize, // Records processed in current file
    pub file_name: Option<&'a str>,
    pub global_vars: &'a GlobalVariables,
    pub debug: bool,
}

/// Result of processing a single record
#[derive(Debug)]
pub enum ProcessResult {
    /// Transform record
    Transform(RecordData),
    /// Multiple output records (fan-out)
    FanOut(Vec<RecordData>),
    /// Transform with additional emitted records
    TransformWithEmissions {
        primary: Option<RecordData>,
        emissions: Vec<RecordData>,
    },
    /// Skip this record (filter out)
    Skip,
    /// Stop processing entirely, with optional final output and exit code
    Exit { data: Option<RecordData>, code: i32 },
    /// Processing error
    Error(ProcessingError),
}

/// Parse error details for deferred reporting
#[derive(Debug, Clone)]
pub struct ParseErrorInfo {
    pub line_number: usize,
    pub format_name: String,
    pub error: String,
}

/// Runtime statistics
#[derive(Debug, Default, Clone)]
pub struct ProcessingStats {
    pub records_processed: usize,
    pub records_output: usize,
    pub records_skipped: usize,
    pub errors: usize,
    pub processing_time: Duration,
    pub parse_errors: Vec<ParseErrorInfo>,
    // Enhanced stats for structured data analysis
    pub earliest_timestamp: Option<i64>,
    pub latest_timestamp: Option<i64>,
    pub keys_seen: HashSet<String>,
    pub levels_seen: HashMap<String, String>, // level_value -> key_name that contained it
    pub lines_seen: usize, // Total input lines (including unparseable ones)
}

impl ProcessingStats {
    /// Update stats with structured data record
    pub fn update_with_structured_data(&mut self, data: &serde_json::Value) {
        use crate::pipeline::config::{TIMESTAMP_KEYS, LEVEL_KEYS};
        
        if let Some(obj) = data.as_object() {
            // Track all keys seen
            for key in obj.keys() {
                self.keys_seen.insert(key.clone());
            }
            
            // Look for timestamps
            for &ts_key in TIMESTAMP_KEYS {
                if let Some(ts_value) = obj.get(ts_key) {
                    if let Some(ts_str) = ts_value.as_str() {
                        // Try to parse timestamp using guess_ts logic
                        if let Ok(timestamp) = self.parse_timestamp_value(ts_str) {
                            self.update_timestamp_range(timestamp);
                        }
                    } else if let Some(ts_num) = ts_value.as_i64() {
                        // Handle numeric timestamps
                        self.update_timestamp_range(ts_num);
                    } else if let Some(ts_float) = ts_value.as_f64() {
                        // Handle float timestamps (convert to seconds)
                        self.update_timestamp_range(ts_float as i64);
                    }
                    break; // Found a timestamp, don't check other keys
                }
            }
            
            // Look for log levels
            for &level_key in LEVEL_KEYS {
                if let Some(level_value) = obj.get(level_key) {
                    if let Some(level_str) = level_value.as_str() {
                        self.levels_seen.insert(level_str.to_lowercase(), level_key.to_string());
                    }
                    break; // Found a level, don't check other keys
                }
            }
        }
    }
    
    /// Update timestamp range with a new timestamp
    fn update_timestamp_range(&mut self, timestamp: i64) {
        // Update earliest timestamp
        if let Some(earliest) = self.earliest_timestamp {
            if timestamp < earliest {
                self.earliest_timestamp = Some(timestamp);
            }
        } else {
            self.earliest_timestamp = Some(timestamp);
        }
        
        // Update latest timestamp
        if let Some(latest) = self.latest_timestamp {
            if timestamp > latest {
                self.latest_timestamp = Some(timestamp);
            }
        } else {
            self.latest_timestamp = Some(timestamp);
        }
    }
    
    /// Parse timestamp string using simplified logic from guess_ts
    fn parse_timestamp_value(&self, text: &str) -> Result<i64, ()> {
        use chrono::{DateTime, NaiveDateTime};
        
        // Try RFC3339/ISO 8601 first (most common in logs)
        if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
            return Ok(dt.timestamp());
        }
        
        // Try ISO 8601 without timezone (assume UTC)
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S") {
            return Ok(dt.and_utc().timestamp());
        }
        
        // Try common log format
        if let Ok(dt) = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
            return Ok(dt.and_utc().timestamp());
        }
        
        // Try Unix timestamp as string
        if let Ok(ts) = text.parse::<i64>() {
            // Reasonable range check for Unix timestamps (1970-2100)
            if ts > 0 && ts < 4102444800 {
                return Ok(ts);
            }
        }
        
        Err(())
    }
}

/// Shared context across all processors
pub struct PipelineContext {
    pub global_vars: GlobalVariables,
    pub line_number: usize,
    pub record_count: usize,
    pub total_processed: usize,
    pub file_name: Option<String>,
}

impl Default for PipelineContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineContext {
    pub fn new() -> Self {
        PipelineContext {
            global_vars: GlobalVariables::new(),
            line_number: 0,
            record_count: 0,
            total_processed: 0,
            file_name: None,
        }
    }
}

// Thread-local storage for parsed data (add this to your existing thread-locals)
thread_local! {
    static PARSED_DATA: RefCell<Option<serde_json::Value>> = const { RefCell::new(None) };
}

/// Set parsed data for the current line (called by InputFormatWrapper)
pub fn set_parsed_data(data: Option<serde_json::Value>) {
    PARSED_DATA.with(|cell| {
        *cell.borrow_mut() = data;
    });
}

/// Get parsed data for the current line (called by StarlarkProcessor)
pub fn get_parsed_data() -> Option<serde_json::Value> {
    PARSED_DATA.with(|cell| cell.borrow().clone())
}

/// Clear parsed data (called between lines)
pub fn clear_parsed_data() {
    PARSED_DATA.with(|cell| {
        *cell.borrow_mut() = None;
    });
}
