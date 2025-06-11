// src/input_format.rs - Complete integration in a single file

use serde_json;
use std::io::{BufRead, BufReader, Read, Write};

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum InputFormat {
    #[value(name = "jsonl")]
    Jsonl,
    #[value(name = "csv")]
    Csv,
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
        let headers: Vec<String> = header_line
            .split(',')
            .map(|h| h.trim().trim_matches('"').to_string())
            .collect();

        if headers.is_empty() {
            return Err("CSV headers cannot be empty".to_string());
        }

        self.headers = Some(headers);
        Ok(())
    }
}

impl LineParser for CsvParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let headers = self.headers.as_ref().ok_or("CSV headers not initialized")?;

        let values: Vec<&str> = line.split(',').collect();

        if values.len() != headers.len() {
            return Err(format!(
                "CSV line has {} fields but expected {} headers",
                values.len(),
                headers.len()
            ));
        }

        let mut map = serde_json::Map::new();
        for (header, value) in headers.iter().zip(values.iter()) {
            let cleaned_value = value.trim().trim_matches('"').to_string();
            map.insert(header.clone(), serde_json::Value::String(cleaned_value));
        }

        Ok(serde_json::Value::Object(map))
    }
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
        let mut enhanced_lines = Vec::new();

        // Read all lines and parse them
        for line_result in reader.lines() {
            let line = line_result?;

            // Parse JSONL and store in context
            if let Ok(data) = parser.parse_line(&line) {
                crate::context::set_parsed_data(Some(data));
            } else {
                crate::context::clear_parsed_data();
            }

            enhanced_lines.push(line);
        }

        // Process enhanced lines through existing pipeline
        let enhanced_reader = std::io::Cursor::new(enhanced_lines.join("\n"));
        pipeline.process_stream_with_data(enhanced_reader, output, filename)
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

        let mut enhanced_lines = Vec::new();

        // Read and parse remaining lines
        for line_result in reader.lines() {
            let line = line_result?;

            // Parse CSV and store in context
            if let Ok(data) = parser.parse_line(&line) {
                crate::context::set_parsed_data(Some(data));
            } else {
                crate::context::clear_parsed_data();
            }

            enhanced_lines.push(line);
        }

        // Process through existing pipeline
        let enhanced_reader = std::io::Cursor::new(enhanced_lines.join("\n"));
        pipeline.process_stream_with_data(enhanced_reader, output, filename)
    }
}
