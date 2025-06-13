// src/input_format.rs - Complete integration in a single file

use serde_json;
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum InputFormat {
    #[value(name = "jsonl")]
    Jsonl,
    #[value(name = "csv")]
    Csv,
    #[value(name = "logfmt")]
    Logfmt,
}

pub trait LineParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String>;
}

pub struct JsonlParser;
pub struct CsvParser {
    headers: Option<Vec<String>>,
}

impl JsonlParser {
    pub fn new() -> Self {
        Self
    }
}

impl LineParser for JsonlParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        serde_json::from_str(line.trim()).map_err(|e| format!("Failed to parse JSONL: {}", e))
    }
}

impl CsvParser {
    pub fn new() -> Self {
        Self { headers: None }
    }

    pub fn parse_headers(&mut self, header_line: &str) -> Result<(), String> {
        let headers: Vec<String> = self
            .parse_csv_fields(header_line.trim())
            .map_err(|e| format!("Failed to parse CSV headers: {}", e))?
            .into_iter()
            .map(|h| h.trim().trim_matches('"').to_string())
            .filter(|h| !h.is_empty()) // Remove empty headers after trimming
            .collect();

        if headers.is_empty() {
            return Err("CSV headers cannot be empty".to_string());
        }

        self.headers = Some(headers);
        Ok(())
    }

    // Proper CSV field parsing that handles quoted fields with commas
    fn parse_csv_fields(&self, line: &str) -> Result<Vec<String>, String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    // Check for escaped quotes (double quotes)
                    if in_quotes && chars.peek() == Some(&'"') {
                        chars.next(); // consume the second quote
                        current_field.push('"');
                    } else {
                        // Toggle quote state
                        in_quotes = !in_quotes;
                    }
                }
                ',' => {
                    if in_quotes {
                        // Inside quotes, comma is part of the field
                        current_field.push(',');
                    } else {
                        // Outside quotes, comma is field separator
                        fields.push(current_field.trim().to_string());
                        current_field.clear();
                    }
                }
                _ => {
                    current_field.push(ch);
                }
            }
        }

        // Don't forget the last field
        fields.push(current_field.trim().to_string());

        if in_quotes {
            return Err("Unclosed quote in CSV line".to_string());
        }

        Ok(fields)
    }
}

impl LineParser for CsvParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let headers = self.headers.as_ref().ok_or("CSV headers not initialized")?;

        let values = self.parse_csv_fields(line)?;

        if values.len() != headers.len() {
            return Err(format!(
                "CSV line has {} fields but expected {} headers",
                values.len(),
                headers.len()
            ));
        }

        let mut map = serde_json::Map::new();
        for (header, value) in headers.iter().zip(values.iter()) {
            // Remove surrounding quotes if present, but preserve inner content
            let cleaned_value = if value.starts_with('"') && value.ends_with('"') && value.len() > 1
            {
                value[1..value.len() - 1].to_string()
            } else {
                value.clone()
            };
            map.insert(header.clone(), serde_json::Value::String(cleaned_value));
        }

        Ok(serde_json::Value::Object(map))
    }
}

pub struct LogfmtParser;

impl LogfmtParser {
    pub fn new() -> Self {
        Self
    }

    // Parse logfmt line: key1=value1 key2="value with spaces" key3=value3
    fn parse_logfmt_pairs(&self, line: &str) -> Result<Vec<(String, String)>, String> {
        let mut pairs = Vec::new();
        let mut chars = line.chars().peekable();

        while chars.peek().is_some() {
            // Skip whitespace
            while chars.peek() == Some(&' ') || chars.peek() == Some(&'\t') {
                chars.next();
            }

            if chars.peek().is_none() {
                break;
            }

            // Parse key
            let mut key = String::new();
            while let Some(&ch) = chars.peek() {
                if ch == '=' {
                    break;
                } else if ch == ' ' || ch == '\t' {
                    return Err("Key cannot contain spaces".to_string());
                } else {
                    key.push(chars.next().unwrap());
                }
            }

            if key.is_empty() {
                return Err("Empty key found".to_string());
            }

            // Expect '='
            if chars.next() != Some('=') {
                return Err(format!("Expected '=' after key '{}'", key));
            }

            // Parse value
            let mut value = String::new();
            if chars.peek() == Some(&'"') {
                // Quoted value
                chars.next(); // consume opening quote
                while let Some(ch) = chars.next() {
                    if ch == '"' {
                        // Check for escaped quote
                        if chars.peek() == Some(&'"') {
                            chars.next(); // consume escaped quote
                            value.push('"');
                        } else {
                            break; // end of quoted value
                        }
                    } else if ch == '\\' {
                        // Handle escape sequences
                        if let Some(escaped_ch) = chars.next() {
                            match escaped_ch {
                                'n' => value.push('\n'),
                                't' => value.push('\t'),
                                'r' => value.push('\r'),
                                '\\' => value.push('\\'),
                                '"' => value.push('"'),
                                _ => {
                                    value.push('\\');
                                    value.push(escaped_ch);
                                }
                            }
                        }
                    } else {
                        value.push(ch);
                    }
                }
            } else {
                // Unquoted value - read until space or end
                while let Some(&ch) = chars.peek() {
                    if ch == ' ' || ch == '\t' {
                        break;
                    } else {
                        value.push(chars.next().unwrap());
                    }
                }
            }

            pairs.push((key, value));
        }

        Ok(pairs)
    }
}

impl LineParser for LogfmtParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let pairs = self.parse_logfmt_pairs(line.trim())?;

        let mut map = serde_json::Map::new();
        for (key, value) in pairs {
            map.insert(key, serde_json::Value::String(value));
        }

        Ok(serde_json::Value::Object(map))
    }
}

/// Simple parse error info for summary reporting
#[derive(Debug)]
struct ParseError {
    line_number: usize,
    error: String,
}

/// Wrapper that integrates input format parsing with existing StreamPipeline
pub struct InputFormatWrapper<'a> {
    format: Option<&'a InputFormat>,
}

impl<'a> InputFormatWrapper<'a> {
    pub fn new(format: Option<&'a InputFormat>) -> Self {
        Self { format }
    }

    pub fn process_with_pipeline<R: Read, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        match self.format {
            Some(InputFormat::Jsonl) => {
                self.process_jsonl(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Csv) => {
                self.process_csv(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Logfmt) => {
                self.process_logfmt(BufReader::new(reader), pipeline, output, filename)
            }
            None => {
                // Raw text - use existing pipeline unchanged
                pipeline.process_stream_with_data(BufReader::new(reader), output, filename)
            }
        }
    }

    fn process_jsonl<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = JsonlParser::new();
        let mut records = Vec::new();
        let config = pipeline.get_config();
        let mut line_number = 0;
        let mut parse_errors = Vec::new();

        // Read all lines and parse them
        for line_result in reader.lines() {
            let line = line_result?;
            line_number += 1;
            let line_content = line.trim();

            if line_content.is_empty() {
                continue;
            }

            // Parse JSONL and create structured records
            match parser.parse_line(&line_content) {
                Ok(data) => {
                    records.push(crate::context::RecordData::structured(data));
                }
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match config.error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "JSON parse error on line {}: {}",
                                line_number, parse_error
                            )
                            .into());
                        }
                        crate::config::ErrorStrategy::Skip => {
                            // Collect error for later reporting
                            parse_errors.push(ParseError {
                                line_number,
                                error: parse_error,
                            });
                            // Skip the malformed line entirely to maintain output format consistency
                            continue;
                        }
                    }
                }
            }
        }

        // Process records directly
        let mut result = pipeline.process_records(records, output, filename)?;
        
        // Add parse errors to the statistics
        result.errors += parse_errors.len();
        for parse_error in parse_errors {
            result.parse_errors.push(crate::context::ParseErrorInfo {
                line_number: parse_error.line_number,
                format_name: "JSON".to_string(),
                error: parse_error.error,
            });
        }
        
        Ok(result)
    }

    fn process_csv<R: BufRead, W: Write>(
        &self,
        mut reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        // Read headers
        let mut header_line = String::new();
        reader.read_line(&mut header_line)?;

        if header_line.trim().is_empty() {
            return Err("CSV file is empty".into());
        }

        let mut parser = CsvParser::new();
        parser.parse_headers(&header_line).map_err(|e| e)?;

        let mut records = Vec::new();
        let config = pipeline.get_config();
        let mut line_number = 1; // Start at 1 since we already read the header line
        let mut parse_errors = Vec::new();

        // Read and parse remaining lines
        for line_result in reader.lines() {
            let line = line_result?;
            line_number += 1;
            let line_content = line.trim();

            if line_content.is_empty() {
                continue;
            }

            // Parse CSV and create structured record
            match parser.parse_line(&line) {
                Ok(data) => {
                    records.push(crate::context::RecordData::structured(data));
                }
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match config.error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "CSV parse error on line {}: {}",
                                line_number, parse_error
                            )
                            .into());
                        }
                        crate::config::ErrorStrategy::Skip => {
                            // Collect error for later reporting
                            parse_errors.push(ParseError {
                                line_number,
                                error: parse_error,
                            });
                            // Skip the malformed line entirely to maintain output format consistency
                            continue;
                        }
                    }
                }
            }
        }

        // Process records directly
        let mut result = pipeline.process_records(records, output, filename)?;
        
        // Add parse errors to the statistics
        result.errors += parse_errors.len();
        for parse_error in parse_errors {
            result.parse_errors.push(crate::context::ParseErrorInfo {
                line_number: parse_error.line_number,
                format_name: "CSV".to_string(),
                error: parse_error.error,
            });
        }
        
        Ok(result)
    }

    fn process_logfmt<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = LogfmtParser::new();
        let mut records = Vec::new();
        let config = pipeline.get_config();
        let mut line_number = 0;
        let mut parse_errors = Vec::new();

        // Read all lines and parse them
        for line_result in reader.lines() {
            let line = line_result?;
            line_number += 1;
            let line_content = line.trim();

            if line_content.is_empty() {
                continue;
            }

            // Parse logfmt and create structured record
            match parser.parse_line(&line_content) {
                Ok(data) => {
                    records.push(crate::context::RecordData::structured(data));
                }
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match config.error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "logfmt parse error on line {}: {}",
                                line_number, parse_error
                            )
                            .into());
                        }
                        crate::config::ErrorStrategy::Skip => {
                            // Collect error for later reporting
                            parse_errors.push(ParseError {
                                line_number,
                                error: parse_error,
                            });
                            // Skip the malformed line entirely to maintain output format consistency
                            continue;
                        }
                    }
                }
            }
        }

        // Process records directly
        let mut result = pipeline.process_records(records, output, filename)?;
        
        // Add parse errors to the statistics
        result.errors += parse_errors.len();
        for parse_error in parse_errors {
            result.parse_errors.push(crate::context::ParseErrorInfo {
                line_number: parse_error.line_number,
                format_name: "logfmt".to_string(),
                error: parse_error.error,
            });
        }
        
        Ok(result)
    }
}
