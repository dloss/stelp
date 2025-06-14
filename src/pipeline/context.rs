use crate::error::ProcessingError;
use crate::variables::GlobalVariables;
use serde_json;
use std::cell::RefCell;
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
    /// Stop processing entirely, with optional final output
    Exit(Option<RecordData>),
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
    static PARSED_DATA: RefCell<Option<serde_json::Value>> = RefCell::new(None);
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
