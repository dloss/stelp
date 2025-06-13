// src/input_format.rs - Complete integration in a single file

use serde_json;
use std::io::{BufRead, BufReader, Read, Write};
use regex::Regex;
use crate::chunking::{ChunkConfig, chunk_lines};

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum InputFormat {
    #[value(name = "jsonl")]
    Jsonl,
    #[value(name = "csv")]
    Csv,
    #[value(name = "logfmt")]
    Logfmt,
    #[value(name = "syslog")]
    Syslog,
    #[value(name = "weblog")]
    Weblog,
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

pub struct SyslogParser {
    rfc5424_regex: Regex,
    rfc3164_regex: Regex,
}

impl SyslogParser {
    pub fn new() -> Self {
        // RFC5424: <165>1 2023-10-11T22:14:15.003Z hostname appname 1234 msgid structured_data message
        let rfc5424_regex = Regex::new(
            r"^<(\d{1,3})>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)(?:\s+(.*))?$"
        ).expect("RFC5424 regex should compile");

        // RFC3164: Oct 11 22:14:15 hostname appname[1234]: message
        let rfc3164_regex = Regex::new(
            r"^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+(\S+)\s+([^:\[\s]+)(?:\[(\d+)\])?\s*:\s*(.*)$"
        ).expect("RFC3164 regex should compile");

        Self {
            rfc5424_regex,
            rfc3164_regex,
        }
    }

    fn parse_priority(priority: u32) -> (u32, u32) {
        let facility = priority >> 3;
        let severity = priority & 7;
        (facility, severity)
    }
}

impl LineParser for SyslogParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let line = line.trim();
        
        // Try RFC5424 format first
        if let Some(captures) = self.rfc5424_regex.captures(line) {
            let priority_str = captures.get(1).unwrap().as_str();
            let priority = priority_str.parse::<u32>()
                .map_err(|_| format!("Invalid priority value: {}", priority_str))?;
            
            if priority > 191 {
                return Err(format!("Priority value {} out of range (0-191)", priority));
            }
            
            let (facility, severity) = Self::parse_priority(priority);
            
            let _version = captures.get(2).unwrap().as_str();
            let timestamp = captures.get(3).unwrap().as_str();
            let hostname = captures.get(4).unwrap().as_str();
            let appname = captures.get(5).unwrap().as_str();
            let procid = captures.get(6).unwrap().as_str();
            let msgid = captures.get(7).unwrap().as_str();
            let _structured_data = captures.get(8).unwrap().as_str();
            let message = captures.get(9).map(|m| m.as_str()).unwrap_or("");
            
            let mut map = serde_json::Map::new();
            map.insert("pri".to_string(), serde_json::Value::Number(priority.into()));
            map.insert("facility".to_string(), serde_json::Value::Number(facility.into()));
            map.insert("severity".to_string(), serde_json::Value::Number(severity.into()));
            map.insert("ts".to_string(), serde_json::Value::String(timestamp.to_string()));
            map.insert("host".to_string(), serde_json::Value::String(hostname.to_string()));
            
            // Handle optional fields
            if appname != "-" {
                map.insert("prog".to_string(), serde_json::Value::String(appname.to_string()));
            }
            if procid != "-" {
                if let Ok(pid) = procid.parse::<u32>() {
                    map.insert("pid".to_string(), serde_json::Value::Number(pid.into()));
                }
            }
            if msgid != "-" {
                map.insert("msgid".to_string(), serde_json::Value::String(msgid.to_string()));
            }
            
            map.insert("msg".to_string(), serde_json::Value::String(message.to_string()));
            
            return Ok(serde_json::Value::Object(map));
        }
        
        // Try RFC3164 format
        if let Some(captures) = self.rfc3164_regex.captures(line) {
            let timestamp = captures.get(1).unwrap().as_str();
            let hostname = captures.get(2).unwrap().as_str();
            let appname = captures.get(3).unwrap().as_str();
            let procid = captures.get(4).map(|m| m.as_str());
            let message = captures.get(5).unwrap().as_str();
            
            let mut map = serde_json::Map::new();
            map.insert("ts".to_string(), serde_json::Value::String(timestamp.to_string()));
            map.insert("host".to_string(), serde_json::Value::String(hostname.to_string()));
            map.insert("prog".to_string(), serde_json::Value::String(appname.to_string()));
            
            if let Some(pid_str) = procid {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    map.insert("pid".to_string(), serde_json::Value::Number(pid.into()));
                }
            }
            
            map.insert("msg".to_string(), serde_json::Value::String(message.to_string()));
            
            return Ok(serde_json::Value::Object(map));
        }
        
        Err("Line does not match RFC5424 or RFC3164 syslog format".to_string())
    }
}

pub struct WeblogParser {
    combined_regex: Regex,
    common_regex: Regex,
}

impl WeblogParser {
    pub fn new() -> Self {
        // Combined Log Format: IP - user [timestamp] "request" status size "referer" "user_agent"
        let combined_regex = Regex::new(
            r#"^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d+) (\S+) "([^"]*)" "([^"]*)"$"#
        ).expect("Combined log format regex should compile");

        // Common Log Format: IP - user [timestamp] "request" status size
        let common_regex = Regex::new(
            r#"^(\S+) (\S+) (\S+) \[([^\]]+)\] "([^"]*)" (\d+) (\S+)$"#
        ).expect("Common log format regex should compile");

        Self {
            combined_regex,
            common_regex,
        }
    }

    fn parse_request(request: &str) -> (Option<String>, Option<String>, Option<String>) {
        let parts: Vec<&str> = request.splitn(3, ' ').collect();
        match parts.len() {
            3 => (
                Some(parts[0].to_string()),  // method
                Some(parts[1].to_string()),  // path
                Some(parts[2].to_string()),  // protocol
            ),
            2 => (
                Some(parts[0].to_string()),  // method
                Some(parts[1].to_string()),  // path
                None,                        // protocol
            ),
            1 => (
                Some(parts[0].to_string()),  // method
                None,                        // path
                None,                        // protocol
            ),
            _ => (None, None, None),
        }
    }
}

impl LineParser for WeblogParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let line = line.trim();
        
        // Try Combined Log Format first (with referer and user_agent)
        if let Some(captures) = self.combined_regex.captures(line) {
            let ip = captures.get(1).unwrap().as_str();
            let ident = captures.get(2).unwrap().as_str();
            let user = captures.get(3).unwrap().as_str();
            let timestamp = captures.get(4).unwrap().as_str();
            let request = captures.get(5).unwrap().as_str();
            let status = captures.get(6).unwrap().as_str();
            let size = captures.get(7).unwrap().as_str();
            let referer = captures.get(8).unwrap().as_str();
            let user_agent = captures.get(9).unwrap().as_str();
            
            let (method, path, protocol) = Self::parse_request(request);
            
            let mut map = serde_json::Map::new();
            map.insert("ip".to_string(), serde_json::Value::String(ip.to_string()));
            
            // Only include non-dash values for optional fields
            if ident != "-" {
                map.insert("ident".to_string(), serde_json::Value::String(ident.to_string()));
            }
            if user != "-" {
                map.insert("user".to_string(), serde_json::Value::String(user.to_string()));
            }
            
            map.insert("ts".to_string(), serde_json::Value::String(timestamp.to_string()));
            map.insert("req".to_string(), serde_json::Value::String(request.to_string()));
            
            if let Some(m) = method {
                map.insert("method".to_string(), serde_json::Value::String(m));
            }
            if let Some(p) = path {
                map.insert("path".to_string(), serde_json::Value::String(p));
            }
            if let Some(proto) = protocol {
                map.insert("proto".to_string(), serde_json::Value::String(proto));
            }
            
            if let Ok(status_num) = status.parse::<u32>() {
                map.insert("status".to_string(), serde_json::Value::Number(status_num.into()));
            } else {
                map.insert("status".to_string(), serde_json::Value::String(status.to_string()));
            }
            
            if size != "-" {
                if let Ok(size_num) = size.parse::<u64>() {
                    map.insert("size".to_string(), serde_json::Value::Number(size_num.into()));
                } else {
                    map.insert("size".to_string(), serde_json::Value::String(size.to_string()));
                }
            }
            
            if referer != "-" {
                map.insert("referer".to_string(), serde_json::Value::String(referer.to_string()));
            }
            if user_agent != "-" {
                map.insert("ua".to_string(), serde_json::Value::String(user_agent.to_string()));
            }
            
            return Ok(serde_json::Value::Object(map));
        }
        
        // Try Common Log Format (without referer and user_agent)
        if let Some(captures) = self.common_regex.captures(line) {
            let ip = captures.get(1).unwrap().as_str();
            let ident = captures.get(2).unwrap().as_str();
            let user = captures.get(3).unwrap().as_str();
            let timestamp = captures.get(4).unwrap().as_str();
            let request = captures.get(5).unwrap().as_str();
            let status = captures.get(6).unwrap().as_str();
            let size = captures.get(7).unwrap().as_str();
            
            let (method, path, protocol) = Self::parse_request(request);
            
            let mut map = serde_json::Map::new();
            map.insert("ip".to_string(), serde_json::Value::String(ip.to_string()));
            
            if ident != "-" {
                map.insert("ident".to_string(), serde_json::Value::String(ident.to_string()));
            }
            if user != "-" {
                map.insert("user".to_string(), serde_json::Value::String(user.to_string()));
            }
            
            map.insert("ts".to_string(), serde_json::Value::String(timestamp.to_string()));
            map.insert("req".to_string(), serde_json::Value::String(request.to_string()));
            
            if let Some(m) = method {
                map.insert("method".to_string(), serde_json::Value::String(m));
            }
            if let Some(p) = path {
                map.insert("path".to_string(), serde_json::Value::String(p));
            }
            if let Some(proto) = protocol {
                map.insert("proto".to_string(), serde_json::Value::String(proto));
            }
            
            if let Ok(status_num) = status.parse::<u32>() {
                map.insert("status".to_string(), serde_json::Value::Number(status_num.into()));
            } else {
                map.insert("status".to_string(), serde_json::Value::String(status.to_string()));
            }
            
            if size != "-" {
                if let Ok(size_num) = size.parse::<u64>() {
                    map.insert("size".to_string(), serde_json::Value::Number(size_num.into()));
                } else {
                    map.insert("size".to_string(), serde_json::Value::String(size.to_string()));
                }
            }
            
            return Ok(serde_json::Value::Object(map));
        }
        
        Err("Line does not match Combined or Common Log Format".to_string())
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
    chunk_config: Option<ChunkConfig>,
}

impl<'a> InputFormatWrapper<'a> {
    pub fn new(format: Option<&'a InputFormat>) -> Self {
        Self { 
            format,
            chunk_config: None,
        }
    }
    
    pub fn with_chunking(mut self, chunk_config: ChunkConfig) -> Self {
        self.chunk_config = Some(chunk_config);
        self
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
            Some(InputFormat::Syslog) => {
                self.process_syslog(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Weblog) => {
                self.process_weblog(BufReader::new(reader), pipeline, output, filename)
            }
            None => {
                // Raw text - apply chunking if configured
                if self.chunk_config.is_some() {
                    self.process_text_with_chunking(BufReader::new(reader), pipeline, output, filename)
                } else {
                    // Use existing pipeline unchanged
                    pipeline.process_stream_with_data(BufReader::new(reader), output, filename)
                }
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

    fn process_syslog<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = SyslogParser::new();
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

            // Parse syslog and create structured record
            match parser.parse_line(&line_content) {
                Ok(data) => {
                    records.push(crate::context::RecordData::structured(data));
                }
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match config.error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "syslog parse error on line {}: {}",
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
                format_name: "syslog".to_string(),
                error: parse_error.error,
            });
        }
        
        Ok(result)
    }

    fn process_weblog<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = WeblogParser::new();
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

            // Parse weblog and create structured record
            match parser.parse_line(&line_content) {
                Ok(data) => {
                    records.push(crate::context::RecordData::structured(data));
                }
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match config.error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "weblog parse error on line {}: {}",
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
                format_name: "weblog".to_string(),
                error: parse_error.error,
            });
        }
        
        Ok(result)
    }

    fn process_text_with_chunking<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let chunk_config = self.chunk_config.as_ref().unwrap();
        let chunks = chunk_lines(reader, chunk_config.clone())?;
        
        // Convert chunks to RecordData
        let records: Vec<crate::context::RecordData> = chunks
            .into_iter()
            .map(|chunk| crate::context::RecordData::text(chunk))
            .collect();
        
        // Process chunks through the pipeline
        pipeline.process_records(records, output, filename)
    }
}
