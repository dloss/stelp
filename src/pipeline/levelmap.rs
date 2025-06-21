use crate::pipeline::context::{ProcessResult, RecordContext, RecordData};
use crate::pipeline::stream::RecordProcessor;
use regex::Regex;
use std::io::{self, Write};
use terminal_size::{Width, terminal_size};

/// Processor that extracts log levels and outputs first character mappings
pub struct LevelMapProcessor {
    name: String,
    use_color: bool,
    timestamp_regex: Regex,
    current_line_length: usize,
    terminal_width: usize,
    last_timestamp: Option<String>,
}

impl LevelMapProcessor {
    pub fn new(name: &str, use_color: bool) -> Self {
        // Regex for extracting timestamps from text
        let timestamp_regex = Regex::new(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}").unwrap();
        
        // Get terminal width, default to 80 if not available
        let terminal_width = if let Some((Width(w), _)) = terminal_size() {
            w as usize
        } else {
            80
        };
        
        Self {
            name: name.to_string(),
            use_color,
            timestamp_regex,
            current_line_length: 0,
            terminal_width,
            last_timestamp: None,
        }
    }

    /// Extract log level from a record (only from structured data)
    pub fn extract_level(&self, record: &RecordData) -> Option<String> {
        match record {
            RecordData::Text(_) => None, // No level extraction from text
            RecordData::Structured(data) => self.extract_level_from_structured(data),
        }
    }

    /// Extract level from structured data
    fn extract_level_from_structured(&self, data: &serde_json::Value) -> Option<String> {
        if let serde_json::Value::Object(obj) = data {
            // Common level field names in order of preference
            let level_fields = ["level", "loglevel", "log_level", "lvl", "severity", "levelname", "@l"];
            
            for field in &level_fields {
                if let Some(level_value) = obj.get(*field) {
                    let level_str = match level_value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    return Some(level_str.to_lowercase());
                }
            }
        }
        None
    }


    /// Extract timestamp from a record
    pub fn extract_timestamp(&self, record: &RecordData) -> Option<String> {
        match record {
            RecordData::Text(text) => self.extract_timestamp_from_text(text),
            RecordData::Structured(data) => self.extract_timestamp_from_structured(data),
        }
    }

    /// Extract timestamp from structured data
    fn extract_timestamp_from_structured(&self, data: &serde_json::Value) -> Option<String> {
        if let serde_json::Value::Object(obj) = data {
            // Common timestamp field names in order of preference
            let timestamp_fields = crate::pipeline::config::TIMESTAMP_KEYS;
            
            for field in timestamp_fields {
                if let Some(timestamp_value) = obj.get(*field) {
                    let timestamp_str = match timestamp_value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => continue,
                    };
                    return Some(timestamp_str);
                }
            }
        }
        None
    }

    /// Extract timestamp from text using regex
    fn extract_timestamp_from_text(&self, text: &str) -> Option<String> {
        if let Some(timestamp_match) = self.timestamp_regex.find(text) {
            return Some(timestamp_match.as_str().to_string());
        }
        None
    }

    /// Convert level string to character representation
    pub fn level_to_char(&self, level: &str) -> char {
        if level.is_empty() {
            return '.';
        }

        let level_lower = level.to_lowercase();
        let first_char = level.chars().next().unwrap();

        match level_lower.as_str() {
            "error" | "err" => first_char,
            "fatal" => first_char,
            "panic" => first_char,
            "warn" | "warning" => first_char,
            "info" => first_char,
            "notice" => first_char,
            "debug" => first_char,
            "trace" => first_char,
            "unknown" => first_char,
            _ => {
                // For unknown levels, return first character
                // Special case: preserve original case
                if level.chars().next().unwrap().is_uppercase() {
                    first_char.to_uppercase().next().unwrap()
                } else {
                    first_char.to_lowercase().next().unwrap()
                }
            }
        }
    }

    /// Output a character to stdout with optional coloring
    fn output_char(&self, ch: char) {
        if self.use_color {
            // Add color based on level severity
            let colored_char = match ch.to_lowercase().next().unwrap() {
                'e' | 'f' | 'p' => format!("\x1b[31m{}\x1b[0m", ch), // Red for errors/fatal/panic
                'w' => format!("\x1b[33m{}\x1b[0m", ch),              // Yellow for warnings
                'i' | 'n' => format!("\x1b[32m{}\x1b[0m", ch),       // Green for info/notice
                'd' | 't' => format!("\x1b[36m{}\x1b[0m", ch),       // Cyan for debug/trace
                _ => ch.to_string(),                                   // No color for unknown
            };
            print!("{}", colored_char);
        } else {
            print!("{}", ch);
        }
        
        // Flush immediately for real-time output
        io::stdout().flush().ok();
    }
}

impl RecordProcessor for LevelMapProcessor {
    fn reset(&mut self) {
        // Add newline at the end when processing is done
        if self.current_line_length > 0 {
            println!();
            self.current_line_length = 0;
        }
    }

    fn process(&mut self, record: &RecordData, _ctx: &RecordContext) -> ProcessResult {
        // Extract timestamp and level
        let timestamp = self.extract_timestamp(record);
        let level = self.extract_level(record).unwrap_or_default();
        let level_char = self.level_to_char(&level);
        
        // Update last timestamp if we have one (for use on new lines)
        if let Some(ts) = timestamp {
            self.last_timestamp = Some(ts);
        }
        
        // Check if we need to start a new line
        if self.current_line_length == 0 {
            // Start of new line - output timestamp if we have one
            if let Some(ref ts) = self.last_timestamp {
                print!("{} ", ts);
                self.current_line_length = ts.len() + 1; // +1 for space
            }
        }
        
        // Check if adding this character would exceed terminal width
        let char_width = if self.use_color {
            // ANSI color codes don't count toward display width
            1
        } else {
            1
        };
        
        if self.current_line_length + char_width >= self.terminal_width {
            // Need to wrap to new line
            println!(); // End current line
            self.current_line_length = 0;
            
            // Start new line with timestamp
            if let Some(ref ts) = self.last_timestamp {
                print!("{} ", ts);
                self.current_line_length = ts.len() + 1;
            }
        }
        
        // Output the level character
        self.output_char(level_char);
        self.current_line_length += char_width;
        
        // Always skip the record since we're outputting directly
        ProcessResult::Skip
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::context::RecordContext;
    use crate::variables::GlobalVariables;

    #[test]
    fn test_level_extraction_structured() {
        let processor = LevelMapProcessor::new("test", false);
        
        let json_data = serde_json::json!({
            "level": "error",
            "message": "Test error"
        });
        let record = RecordData::structured(json_data);
        assert_eq!(processor.extract_level(&record), Some("error".to_string()));
    }

    #[test]
    fn test_level_extraction_text() {
        let processor = LevelMapProcessor::new("test", false);
        
        // Text records should not extract levels (only structured data should)
        let text_record = RecordData::text("[ERROR] Database error".to_string());
        assert_eq!(processor.extract_level(&text_record), None);
    }

    #[test]
    fn test_level_to_char_mapping() {
        let processor = LevelMapProcessor::new("test", false);

        // Test basic mappings
        assert_eq!(processor.level_to_char("error"), 'e');
        assert_eq!(processor.level_to_char("fatal"), 'f');
        assert_eq!(processor.level_to_char("panic"), 'p');
        assert_eq!(processor.level_to_char("warn"), 'w');
        assert_eq!(processor.level_to_char("warning"), 'w');
        assert_eq!(processor.level_to_char("info"), 'i');
        assert_eq!(processor.level_to_char("notice"), 'n');
        assert_eq!(processor.level_to_char("debug"), 'd');
        assert_eq!(processor.level_to_char("trace"), 't');
        assert_eq!(processor.level_to_char("unknown"), 'u');
        assert_eq!(processor.level_to_char(""), '.');
        assert_eq!(processor.level_to_char("ERROR"), 'E'); // Should preserve case
    }

    #[test]
    fn test_timestamp_extraction() {
        let processor = LevelMapProcessor::new("test", false);

        // Test structured timestamp extraction
        let json_data = serde_json::json!({
            "timestamp": "2024-01-01T10:00:00Z",
            "level": "info"
        });
        let record = RecordData::structured(json_data);
        assert_eq!(processor.extract_timestamp(&record), Some("2024-01-01T10:00:00Z".to_string()));

        // Test text timestamp extraction
        let text_record = RecordData::text("2024-01-01T10:00:00 INFO Starting".to_string());
        assert_eq!(processor.extract_timestamp(&text_record), Some("2024-01-01T10:00:00".to_string()));
    }

    #[test]
    fn test_process_returns_skip() {
        let globals = GlobalVariables::new();
        let ctx = RecordContext {
            line_number: 1,
            record_count: 1,
            file_name: None,
            global_vars: &globals,
            debug: false,
        };

        let mut processor = LevelMapProcessor::new("test", false);
        let record = RecordData::text("[ERROR] Test".to_string());
        let result = processor.process(&record, &ctx);
        
        // Should always skip since levelmap outputs directly
        assert!(matches!(result, ProcessResult::Skip));
    }
}