// src/processors/window.rs
use crate::pipeline::context::{RecordContext, RecordData, ProcessResult};
use crate::pipeline::stream::RecordProcessor;
use std::collections::VecDeque;
use std::cell::RefCell;
use serde_json;

/// Record stored in window buffer
#[derive(Debug, Clone)]
pub struct WindowRecord {
    /// Text content (if text record)
    pub line: Option<String>,
    /// Structured data (if structured record)  
    pub data: Option<serde_json::Value>,
    /// Metadata for debugging
    pub line_number: usize,
    pub record_count: usize,
}

impl WindowRecord {
    fn from_record_data(record: &RecordData, ctx: &RecordContext) -> Self {
        WindowRecord {
            line: record.as_text().map(|s| s.to_string()),
            data: record.as_structured().cloned(),
            line_number: ctx.line_number,
            record_count: ctx.record_count,
        }
    }
}

/// Processor that maintains a sliding window of recent records
pub struct WindowProcessor {
    window_size: usize,
    buffer: VecDeque<WindowRecord>,
    inner_processor: Box<dyn RecordProcessor>,
}

impl WindowProcessor {
    pub fn new(window_size: usize, inner_processor: Box<dyn RecordProcessor>) -> Self {
        WindowProcessor {
            window_size,
            buffer: VecDeque::with_capacity(window_size),
            inner_processor,
        }
    }

    fn add_to_buffer(&mut self, record: &RecordData, ctx: &RecordContext) {
        let window_record = WindowRecord::from_record_data(record, ctx);
        
        self.buffer.push_back(window_record);
        
        // Keep buffer at target size
        while self.buffer.len() > self.window_size {
            self.buffer.pop_front();
        }
    }
}

impl RecordProcessor for WindowProcessor {
    fn process(&mut self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        // Add current record to window buffer
        self.add_to_buffer(record, ctx);
        
        // Set up window context for functions to access
        WINDOW_CONTEXT.with(|window_ctx| {
            *window_ctx.borrow_mut() = Some(self.buffer.clone());
        });
        
        // Process with inner processor (which will have access to window variables)
        let result = self.inner_processor.process(record, ctx);
        
        // Clear window context
        WINDOW_CONTEXT.with(|window_ctx| {
            *window_ctx.borrow_mut() = None;
        });
        
        result
    }

    fn name(&self) -> &str {
        // Return combination of window and inner processor name
        "window_wrapper"
    }
}

// Thread-local storage for window context
thread_local! {
    pub static WINDOW_CONTEXT: RefCell<Option<VecDeque<WindowRecord>>> = RefCell::new(None);
}