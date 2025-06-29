// src/input_format.rs - Complete integration in a single file

use crate::chunking::{chunk_lines, ChunkConfig};
use regex::Regex;
use serde_json;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum InputFormat {
    #[value(name = "line", help = "Line-based text format (unstructured data)")]
    Line,
    #[value(name = "jsonl", help = "JSON Lines format (one JSON object per line)")]
    Jsonl,
    #[value(name = "csv", help = "Comma-separated values with headers")]
    Csv,
    #[value(name = "tsv", help = "Tab-separated values with headers")]
    Tsv,
    #[value(name = "logfmt", help = "Logfmt format (key=value pairs)")]
    Logfmt,
    #[value(name = "syslog", help = "Syslog format (RFC3164/RFC5424)")]
    Syslog,
    #[value(
        name = "combined",
        help = "Apache/Nginx Combined Log Format (supports standard and extended variants)"
    )]
    Combined,
    #[value(
        name = "fields",
        help = "Whitespace-separated fields (like AWK) with f1, f2, etc. key names"
    )]
    Fields,
}

impl InputFormat {
    /// Detect input format from file extension
    pub fn from_extension(path: &Path) -> Option<InputFormat> {
        if let Some(extension) = path.extension() {
            match extension.to_str()?.to_lowercase().as_str() {
                "jsonl" => Some(InputFormat::Jsonl),
                "csv" => Some(InputFormat::Csv),
                "tsv" => Some(InputFormat::Tsv),
                "logfmt" => Some(InputFormat::Logfmt),
                _ => None,
            }
        } else {
            None
        }
    }
}

pub trait LineParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String>;
}

pub struct JsonlParser;
pub struct CsvParser {
    headers: Option<Vec<String>>,
    separator: char,
}
pub struct FieldsParser;

impl JsonlParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for JsonlParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LineParser for JsonlParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        serde_json::from_str(line.trim()).map_err(|e| format!("Failed to parse JSONL: {}", e))
    }
}

impl CsvParser {
    pub fn new() -> Self {
        Self {
            headers: None,
            separator: ',',
        }
    }

    pub fn new_tsv() -> Self {
        Self {
            headers: None,
            separator: '\t',
        }
    }

    pub fn parse_headers(&mut self, header_line: &str) -> Result<(), String> {
        let format_name = if self.separator == '\t' { "TSV" } else { "CSV" };
        let headers: Vec<String> = self
            .parse_fields(header_line.trim())
            .map_err(|e| format!("Failed to parse {} headers: {}", format_name, e))?
            .into_iter()
            .map(|h| h.trim().trim_matches('"').to_string())
            .filter(|h| !h.is_empty()) // Remove empty headers after trimming
            .collect();

        if headers.is_empty() {
            return Err(format!("{} headers cannot be empty", format_name));
        }

        self.headers = Some(headers);
        Ok(())
    }

    // Fast field parsing using the csv crate
    fn parse_fields(&self, line: &str) -> Result<Vec<String>, String> {
        let format_name = if self.separator == '\t' { "TSV" } else { "CSV" };
        
        // Create a CSV reader for this single line
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.separator as u8)
            .has_headers(false)
            .from_reader(line.as_bytes());
        
        // Read the single record
        let mut record = csv::StringRecord::new();
        match reader.read_record(&mut record) {
            Ok(true) => {
                // Successfully read a record, trim whitespace from fields
                Ok(record.iter().map(|s| s.trim().to_string()).collect())
            }
            Ok(false) => {
                // Empty line
                Ok(vec![])
            }
            Err(e) => {
                Err(format!("{} parsing error: {}", format_name, e))
            }
        }
    }
}

impl Default for CsvParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LineParser for CsvParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let format_name = if self.separator == '\t' { "TSV" } else { "CSV" };
        let headers = self
            .headers
            .as_ref()
            .ok_or(format!("{} headers not initialized", format_name))?;

        let values = self.parse_fields(line)?;

        if values.len() != headers.len() {
            return Err(format!(
                "{} line has {} fields but expected {} headers",
                format_name,
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
}

impl Default for LogfmtParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LogfmtParser {
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

impl FieldsParser {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FieldsParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LineParser for FieldsParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let fields: Vec<&str> = line.split_whitespace().collect();

        let mut map = serde_json::Map::new();
        for (index, field) in fields.iter().enumerate() {
            let key = format!("f{}", index + 1);
            map.insert(key, serde_json::Value::String(field.to_string()));
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
            r"^<(\d{1,3})>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)(?:\s+(.*))?$",
        )
        .expect("RFC5424 regex should compile");

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

impl Default for SyslogParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LineParser for SyslogParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let line = line.trim();

        // Try RFC5424 format first
        if let Some(captures) = self.rfc5424_regex.captures(line) {
            let priority_str = captures.get(1).unwrap().as_str();
            let priority = priority_str
                .parse::<u32>()
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
            map.insert(
                "pri".to_string(),
                serde_json::Value::Number(priority.into()),
            );
            map.insert(
                "facility".to_string(),
                serde_json::Value::Number(facility.into()),
            );
            map.insert(
                "severity".to_string(),
                serde_json::Value::Number(severity.into()),
            );
            map.insert(
                "ts".to_string(),
                serde_json::Value::String(timestamp.to_string()),
            );
            map.insert(
                "host".to_string(),
                serde_json::Value::String(hostname.to_string()),
            );

            // Handle optional fields
            if appname != "-" {
                map.insert(
                    "prog".to_string(),
                    serde_json::Value::String(appname.to_string()),
                );
            }
            if procid != "-" {
                if let Ok(pid) = procid.parse::<u32>() {
                    map.insert("pid".to_string(), serde_json::Value::Number(pid.into()));
                }
            }
            if msgid != "-" {
                map.insert(
                    "msgid".to_string(),
                    serde_json::Value::String(msgid.to_string()),
                );
            }

            map.insert(
                "msg".to_string(),
                serde_json::Value::String(message.to_string()),
            );

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
            map.insert(
                "ts".to_string(),
                serde_json::Value::String(timestamp.to_string()),
            );
            map.insert(
                "host".to_string(),
                serde_json::Value::String(hostname.to_string()),
            );
            map.insert(
                "prog".to_string(),
                serde_json::Value::String(appname.to_string()),
            );

            if let Some(pid_str) = procid {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    map.insert("pid".to_string(), serde_json::Value::Number(pid.into()));
                }
            }

            map.insert(
                "msg".to_string(),
                serde_json::Value::String(message.to_string()),
            );

            return Ok(serde_json::Value::Object(map));
        }

        Err("Line does not match RFC5424 or RFC3164 syslog format".to_string())
    }
}

pub struct CombinedParser {
    extended_regex: Regex,
    standard_combined_regex: Regex,
    common_regex: Regex,
}

impl CombinedParser {
    pub fn new() -> Self {
        // Extended Apache format: IP hostname - user port [timestamp] "request" "query" status size "referer" "user_agent" timing...
        let extended_regex = Regex::new(
            r#"^(\S+)\s+(\S+)\s+-\s+(\S+)\s+(\d+)\s+\[([^\]]+)\]\s+"([^"]*)"\s+"([^"]*)"\s+(\d+)\s+(\S+)\s+"([^"]*)"\s+"([^"]*)"(?:\s+(.*))?$"#
        ).expect("Extended combined format regex should compile");

        // Standard Combined Log Format: IP - user [timestamp] "request" status size "referer" "user_agent"
        let standard_combined_regex = Regex::new(
            r#"^(\S+)\s+-\s+(\S+)\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d+)\s+(\S+)\s+"([^"]*)"\s+"([^"]*)"$"#
        ).expect("Standard combined format regex should compile");

        // Common Log Format: IP - user [timestamp] "request" status size
        let common_regex =
            Regex::new(r#"^(\S+)\s+-\s+(\S+)\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d+)\s+(\S+)$"#)
                .expect("Common log format regex should compile");

        Self {
            extended_regex,
            standard_combined_regex,
            common_regex,
        }
    }

    fn parse_request(request: &str) -> (Option<String>, Option<String>, Option<String>) {
        let parts: Vec<&str> = request.splitn(3, ' ').collect();
        match parts.len() {
            3 => (
                Some(parts[0].to_string()), // method
                Some(parts[1].to_string()), // path
                Some(parts[2].to_string()), // protocol
            ),
            2 => (
                Some(parts[0].to_string()), // method
                Some(parts[1].to_string()), // path
                None,                       // protocol
            ),
            1 => (
                Some(parts[0].to_string()), // method
                None,                       // path
                None,                       // protocol
            ),
            _ => (None, None, None),
        }
    }
}

impl Default for CombinedParser {
    fn default() -> Self {
        Self::new()
    }
}

impl LineParser for CombinedParser {
    fn parse_line(&self, line: &str) -> Result<serde_json::Value, String> {
        let line = line.trim();

        // Try Extended Apache format first (IP hostname - user port [timestamp] "request" "query" status size "referer" "user_agent" timing...)
        if let Some(captures) = self.extended_regex.captures(line) {
            let ip = captures.get(1).unwrap().as_str();
            let hostname = captures.get(2).unwrap().as_str();
            let user = captures.get(3).unwrap().as_str();
            let port = captures.get(4).unwrap().as_str();
            let timestamp = captures.get(5).unwrap().as_str();
            let request = captures.get(6).unwrap().as_str();
            let query = captures.get(7).unwrap().as_str();
            let status = captures.get(8).unwrap().as_str();
            let size = captures.get(9).unwrap().as_str();
            let referer = captures.get(10).unwrap().as_str();
            let user_agent = captures.get(11).unwrap().as_str();
            let timing = captures.get(12).map(|m| m.as_str());

            let (method, path, protocol) = Self::parse_request(request);

            let mut map = serde_json::Map::new();
            map.insert("ip".to_string(), serde_json::Value::String(ip.to_string()));

            // Extended format fields
            if hostname != "-" {
                map.insert(
                    "host".to_string(),
                    serde_json::Value::String(hostname.to_string()),
                );
            }
            if user != "-" {
                map.insert(
                    "user".to_string(),
                    serde_json::Value::String(user.to_string()),
                );
            }
            if let Ok(port_num) = port.parse::<u32>() {
                map.insert(
                    "port".to_string(),
                    serde_json::Value::Number(port_num.into()),
                );
            }

            map.insert(
                "ts".to_string(),
                serde_json::Value::String(timestamp.to_string()),
            );
            map.insert(
                "req".to_string(),
                serde_json::Value::String(request.to_string()),
            );

            if !query.is_empty() && query != "-" {
                map.insert(
                    "query".to_string(),
                    serde_json::Value::String(query.to_string()),
                );
            }

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
                map.insert(
                    "status".to_string(),
                    serde_json::Value::Number(status_num.into()),
                );
            } else {
                map.insert(
                    "status".to_string(),
                    serde_json::Value::String(status.to_string()),
                );
            }

            if size != "-" {
                if let Ok(size_num) = size.parse::<u64>() {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::Number(size_num.into()),
                    );
                } else {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::String(size.to_string()),
                    );
                }
            }

            if referer != "-" {
                map.insert(
                    "referer".to_string(),
                    serde_json::Value::String(referer.to_string()),
                );
            }
            if user_agent != "-" {
                map.insert(
                    "ua".to_string(),
                    serde_json::Value::String(user_agent.to_string()),
                );
            }

            if let Some(timing_data) = timing {
                if !timing_data.trim().is_empty() {
                    map.insert(
                        "timing".to_string(),
                        serde_json::Value::String(timing_data.to_string()),
                    );
                }
            }

            return Ok(serde_json::Value::Object(map));
        }

        // Try Standard Combined Log Format (IP - user [timestamp] "request" status size "referer" "user_agent")
        if let Some(captures) = self.standard_combined_regex.captures(line) {
            let ip = captures.get(1).unwrap().as_str();
            let user = captures.get(2).unwrap().as_str();
            let timestamp = captures.get(3).unwrap().as_str();
            let request = captures.get(4).unwrap().as_str();
            let status = captures.get(5).unwrap().as_str();
            let size = captures.get(6).unwrap().as_str();
            let referer = captures.get(7).unwrap().as_str();
            let user_agent = captures.get(8).unwrap().as_str();

            let (method, path, protocol) = Self::parse_request(request);

            let mut map = serde_json::Map::new();
            map.insert("ip".to_string(), serde_json::Value::String(ip.to_string()));

            if user != "-" {
                map.insert(
                    "user".to_string(),
                    serde_json::Value::String(user.to_string()),
                );
            }

            map.insert(
                "ts".to_string(),
                serde_json::Value::String(timestamp.to_string()),
            );
            map.insert(
                "req".to_string(),
                serde_json::Value::String(request.to_string()),
            );

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
                map.insert(
                    "status".to_string(),
                    serde_json::Value::Number(status_num.into()),
                );
            } else {
                map.insert(
                    "status".to_string(),
                    serde_json::Value::String(status.to_string()),
                );
            }

            if size != "-" {
                if let Ok(size_num) = size.parse::<u64>() {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::Number(size_num.into()),
                    );
                } else {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::String(size.to_string()),
                    );
                }
            }

            if referer != "-" {
                map.insert(
                    "referer".to_string(),
                    serde_json::Value::String(referer.to_string()),
                );
            }
            if user_agent != "-" {
                map.insert(
                    "ua".to_string(),
                    serde_json::Value::String(user_agent.to_string()),
                );
            }

            return Ok(serde_json::Value::Object(map));
        }

        // Try Common Log Format (IP - user [timestamp] "request" status size)
        if let Some(captures) = self.common_regex.captures(line) {
            let ip = captures.get(1).unwrap().as_str();
            let user = captures.get(2).unwrap().as_str();
            let timestamp = captures.get(3).unwrap().as_str();
            let request = captures.get(4).unwrap().as_str();
            let status = captures.get(5).unwrap().as_str();
            let size = captures.get(6).unwrap().as_str();

            let (method, path, protocol) = Self::parse_request(request);

            let mut map = serde_json::Map::new();
            map.insert("ip".to_string(), serde_json::Value::String(ip.to_string()));

            if user != "-" {
                map.insert(
                    "user".to_string(),
                    serde_json::Value::String(user.to_string()),
                );
            }

            map.insert(
                "ts".to_string(),
                serde_json::Value::String(timestamp.to_string()),
            );
            map.insert(
                "req".to_string(),
                serde_json::Value::String(request.to_string()),
            );

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
                map.insert(
                    "status".to_string(),
                    serde_json::Value::Number(status_num.into()),
                );
            } else {
                map.insert(
                    "status".to_string(),
                    serde_json::Value::String(status.to_string()),
                );
            }

            if size != "-" {
                if let Ok(size_num) = size.parse::<u64>() {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::Number(size_num.into()),
                    );
                } else {
                    map.insert(
                        "size".to_string(),
                        serde_json::Value::String(size.to_string()),
                    );
                }
            }

            return Ok(serde_json::Value::Object(map));
        }

        Err("Line does not match any supported Combined Log Format variant".to_string())
    }
}

/// Simple parse error info for summary reporting
#[derive(Debug)]

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
            Some(InputFormat::Line) => {
                // Line format is the same as raw text processing
                if self.chunk_config.is_some() {
                    self.process_text_with_chunking(
                        BufReader::new(reader),
                        pipeline,
                        output,
                        filename,
                    )
                } else {
                    pipeline.process_stream_with_data(BufReader::new(reader), output, filename)
                }
            }
            Some(InputFormat::Jsonl) => {
                self.process_jsonl(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Csv) => {
                self.process_csv(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Tsv) => {
                self.process_tsv(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Logfmt) => {
                self.process_logfmt(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Syslog) => {
                self.process_syslog(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Combined) => {
                self.process_combined(BufReader::new(reader), pipeline, output, filename)
            }
            Some(InputFormat::Fields) => {
                self.process_fields(BufReader::new(reader), pipeline, output, filename)
            }
            None => {
                // Raw text - apply chunking if configured
                if self.chunk_config.is_some() {
                    self.process_text_with_chunking(
                        BufReader::new(reader),
                        pipeline,
                        output,
                        filename,
                    )
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
        self.process_line_based_format_streaming(
            reader, pipeline, output, filename,
            parser, "JSONL", false // No headers for JSONL
        )
    }

    /// Generic streaming processor for all line-based structured formats
    fn process_line_based_format_streaming<R: BufRead, W: Write, P: LineParser>(
        &self,
        mut reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
        parser: P,
        format_name: &str,
        has_headers: bool,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        use std::time::Instant;
        let start_time = Instant::now();

        // Initialize streaming context
        pipeline.init_streaming_context(filename);

        // Initialize local stats
        let mut file_stats = crate::context::ProcessingStats::default();

        // Execute BEGIN processor if present (before reading any data)
        match pipeline.execute_begin_streaming(output) {
            Ok(begin_output_count) => {
                file_stats.records_output += begin_output_count;
            }
            Err(e) => {
                if e.to_string() == "Early exit from BEGIN" {
                    file_stats.processing_time = start_time.elapsed();
                    return Ok(file_stats); // Early exit from BEGIN
                } else {
                    return Err(e);
                }
            }
        }

        let error_strategy = pipeline.get_config().error_strategy.clone();
        let mut line_number = 0;

        // Handle headers if format requires them (CSV/TSV)
        if has_headers {
            let mut header_line = String::new();
            reader.read_line(&mut header_line)?;
            line_number = 1; // Start at 1 since we already read the header line

            if header_line.trim().is_empty() {
                return Err(format!("{} file is empty", format_name).into());
            }

            // Note: Header parsing is format-specific and handled by the caller
            // This function only handles the data lines
        }

        // STREAMING: Process each line immediately instead of collecting
        for line_result in reader.lines() {
            let line = line_result?;
            line_number += 1;
            file_stats.lines_seen += 1; // Track all lines seen (including unparseable)
            let line_content = line.trim();

            if line_content.is_empty() {
                continue;
            }

            // Parse line using the provided parser and create structured record
            let record = match parser.parse_line(line_content) {
                Ok(data) => crate::context::RecordData::structured(data),
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "{} parse error on line {}: {}",
                                format_name, line_number, parse_error
                            )
                            .into());
                        }
                        crate::config::ErrorStrategy::Skip => {
                            // Add to parse errors and skip
                            // TODO: Memory optimization - this Vec grows linearly with parsing errors
                            // For true constant memory usage, consider:
                            // - Limiting to last N errors only
                            // - Disabling error collection during streaming
                            // - Using a bounded circular buffer
                            file_stats.errors += 1;
                            file_stats.parse_errors.push(crate::context::ParseErrorInfo {
                                line_number,
                                format_name: format_name.to_string(),
                                error: parse_error,
                            });
                            continue;
                        }
                    }
                }
            };

            // STREAMING: Process this single record immediately
            let should_continue = pipeline.process_single_record_streaming(record, output)?;
            if !should_continue {
                break; // Exit or broken pipe
            }
        }

        // Execute END processor if present (after processing all data)
        match pipeline.execute_end_streaming(output) {
            Ok(end_output_count) => {
                file_stats.records_output += end_output_count;
            }
            Err(e) => {
                return Err(e);
            }
        }

        // Copy final stats from pipeline
        let pipeline_stats = pipeline.get_stats();
        file_stats.records_processed = pipeline_stats.records_processed;
        file_stats.records_output = pipeline_stats.records_output;
        file_stats.records_skipped = pipeline_stats.records_skipped;
        file_stats.errors += pipeline_stats.errors; // Add to existing parse errors
        file_stats.processing_time = start_time.elapsed();
        // Copy enhanced stats from pipeline
        file_stats.earliest_timestamp = pipeline_stats.earliest_timestamp;
        file_stats.latest_timestamp = pipeline_stats.latest_timestamp;
        file_stats.keys_seen = pipeline_stats.keys_seen.clone();
        file_stats.levels_seen = pipeline_stats.levels_seen.clone();

        Ok(file_stats)
    }

    fn process_csv_or_tsv<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
        is_tsv: bool,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        use std::time::Instant;
        let start_time = Instant::now();
        let format_name = if is_tsv { "TSV" } else { "CSV" };
        
        // Initialize streaming context
        pipeline.init_streaming_context(filename);
        
        // Initialize local stats
        let mut file_stats = crate::context::ProcessingStats::default();
        
        // Execute BEGIN processor if present (before reading any data)
        match pipeline.execute_begin_streaming(output) {
            Ok(begin_output_count) => {
                file_stats.records_output += begin_output_count;
            }
            Err(e) => {
                if e.to_string() == "Early exit from BEGIN" {
                    file_stats.processing_time = start_time.elapsed();
                    return Ok(file_stats); // Early exit from BEGIN
                } else {
                    return Err(e);
                }
            }
        }
        
        let error_strategy = pipeline.get_config().error_strategy.clone();
        
        // Create CSV reader with proper configuration
        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(if is_tsv { b'\t' } else { b',' })
            .has_headers(true)
            .from_reader(reader);
        
        // Get headers and convert to owned strings
        let headers: Vec<String> = csv_reader.headers()?.iter().map(|h| h.to_string()).collect();
        
        let mut line_number = 1; // Starting after header
        
        // STREAMING: Process each record immediately using csv crate's iterator
        for record_result in csv_reader.records() {
            line_number += 1;
            file_stats.lines_seen += 1; // Track all lines seen (including unparseable)
            
            let record = match record_result {
                Ok(record) => record,
                Err(parse_error) => {
                    // Handle parsing error according to error strategy
                    match error_strategy {
                        crate::config::ErrorStrategy::FailFast => {
                            return Err(format!(
                                "{} parse error on line {}: {}",
                                format_name, line_number, parse_error
                            ).into());
                        }
                        crate::config::ErrorStrategy::Skip => {
                            file_stats.errors += 1;
                            file_stats.parse_errors.push(crate::context::ParseErrorInfo {
                                line_number,
                                format_name: format_name.to_string(),
                                error: parse_error.to_string(),
                            });
                            continue;
                        }
                    }
                }
            };
            
            // Convert CSV record to JSON
            let mut map = serde_json::Map::new();
            for (header, value) in headers.iter().zip(record.iter()) {
                map.insert(header.clone(), serde_json::Value::String(value.to_string()));
            }
            let json_value = serde_json::Value::Object(map);
            
            // Create structured record
            let structured_record = crate::context::RecordData::structured(json_value);
            
            // STREAMING: Process this single record immediately
            let should_continue = pipeline.process_single_record_streaming(structured_record, output)?;
            if !should_continue {
                break; // Exit or broken pipe
            }
        }
        
        // Execute END processor if present (after processing all data)
        match pipeline.execute_end_streaming(output) {
            Ok(end_output_count) => {
                file_stats.records_output += end_output_count;
            }
            Err(e) => {
                return Err(e);
            }
        }
        
        // Copy final stats from pipeline
        let pipeline_stats = pipeline.get_stats();
        file_stats.records_processed = pipeline_stats.records_processed;
        file_stats.records_output = pipeline_stats.records_output;
        file_stats.records_skipped = pipeline_stats.records_skipped;
        file_stats.errors += pipeline_stats.errors; // Add to existing parse errors
        file_stats.processing_time = start_time.elapsed();
        // Copy enhanced stats from pipeline
        file_stats.earliest_timestamp = pipeline_stats.earliest_timestamp;
        file_stats.latest_timestamp = pipeline_stats.latest_timestamp;
        file_stats.keys_seen = pipeline_stats.keys_seen.clone();
        file_stats.levels_seen = pipeline_stats.levels_seen.clone();
        
        Ok(file_stats)
    }

    fn process_csv<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        self.process_csv_or_tsv(reader, pipeline, output, filename, false)
    }

    fn process_tsv<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        self.process_csv_or_tsv(reader, pipeline, output, filename, true)
    }

    fn process_logfmt<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = LogfmtParser::new();
        self.process_line_based_format_streaming(
            reader, pipeline, output, filename,
            parser, "logfmt", false // No headers for logfmt
        )
    }

    fn process_syslog<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = SyslogParser::new();
        self.process_line_based_format_streaming(
            reader, pipeline, output, filename,
            parser, "syslog", false // No headers for syslog
        )
    }

    fn process_combined<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = CombinedParser::new();
        self.process_line_based_format_streaming(
            reader, pipeline, output, filename,
            parser, "combined", false // No headers for combined log format
        )
    }

    fn process_fields<R: BufRead, W: Write>(
        &self,
        reader: R,
        pipeline: &mut crate::StreamPipeline,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<crate::context::ProcessingStats, Box<dyn std::error::Error>> {
        let parser = FieldsParser::new();
        self.process_line_based_format_streaming(
            reader, pipeline, output, filename,
            parser, "fields", false // No headers for fields format
        )
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
            .map(crate::context::RecordData::text)
            .collect();

        // Process chunks through the pipeline
        pipeline.process_records(records, output, filename)
    }
}
