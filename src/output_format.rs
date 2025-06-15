use crate::error::ProcessingError;
use crate::pipeline::context::RecordData;
use serde_json::Value;
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum OutputFormat {
    #[value(name = "line", help = "Line-based text output (unstructured data)")]
    Line,
    #[value(name = "jsonl", help = "JSON Lines format (one JSON object per line)")]
    Jsonl,
    #[value(name = "csv", help = "Comma-separated values")]
    Csv,
    #[value(name = "tsv", help = "Tab-separated values")]
    Tsv,
    #[value(name = "logfmt", help = "Logfmt format (key=value pairs)")]
    Logfmt,
    #[value(name = "fields", help = "Whitespace-separated fields (like AWK output)")]
    Fields,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "line" => Ok(OutputFormat::Line),
            "jsonl" => Ok(OutputFormat::Jsonl),
            "csv" => Ok(OutputFormat::Csv),
            "tsv" => Ok(OutputFormat::Tsv),
            "logfmt" => Ok(OutputFormat::Logfmt),
            "fields" => Ok(OutputFormat::Fields),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Line
    }
}

pub struct OutputFormatter {
    format: OutputFormat,
    keys: Option<Vec<String>>,
    csv_headers_written: bool,
    csv_schema_keys: Option<Vec<String>>, // Keys from first record (for warning)
    missing_keys_warned: std::collections::HashSet<String>, // Track warned keys
}

impl OutputFormatter {
    pub fn new(format: OutputFormat, keys: Option<Vec<String>>) -> Self {
        OutputFormatter {
            format,
            csv_headers_written: false,
            keys,
            csv_schema_keys: None,
            missing_keys_warned: std::collections::HashSet::new(),
        }
    }

    fn filter_keys(&self, data: &serde_json::Value) -> serde_json::Value {
        if let Some(ref key_list) = self.keys {
            if let serde_json::Value::Object(obj) = data {
                // Use IndexMap to preserve insertion order for JSON serialization
                let mut filtered = serde_json::Map::new();
                for key in key_list {
                    if let Some(value) = obj.get(key) {
                        filtered.insert(key.clone(), value.clone());
                    }
                }
                return serde_json::Value::Object(filtered);
            }
        }
        data.clone()
    }

    
    fn get_key_order(&self, obj: &serde_json::Map<String, serde_json::Value>) -> Vec<String> {
        if let Some(ref key_list) = self.keys {
            // Use the order specified in --keys, including ALL keys (missing ones become empty cells)
            key_list.clone()
        } else {
            // Use natural iteration order when no --keys specified
            obj.keys().cloned().collect()
        }
    }

    pub fn write_record<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match self.format {
            OutputFormat::Line => self.write_line(output, record),
            OutputFormat::Jsonl => self.write_jsonl(output, record),
            OutputFormat::Csv => self.write_csv(output, record),
            OutputFormat::Tsv => self.write_tsv(output, record),
            OutputFormat::Logfmt => self.write_logfmt(output, record),
            OutputFormat::Fields => self.write_fields(output, record),
        }
    }

    fn write_line<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match record {
            RecordData::Text(text) => {
                writeln!(output, "{}", text)?;
            }
            RecordData::Structured(_) => {
                return Err(ProcessingError::OutputError(
                    "Line format cannot output structured data - use jsonl, csv, or logfmt format instead".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn write_jsonl<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match record {
            RecordData::Text(text) => {
                writeln!(output, "{}", text)?;
            }
            RecordData::Structured(data) => {
                if let Some(ref key_list) = self.keys {
                    if let serde_json::Value::Object(obj) = data {
                        // Manually construct JSON to preserve key order
                        let mut json_parts = Vec::new();
                        for key in key_list {
                            if let Some(value) = obj.get(key) {
                                let key_json = serde_json::to_string(key).map_err(|e| {
                                    ProcessingError::OutputError(format!("JSON key encoding error: {}", e))
                                })?;
                                let value_json = serde_json::to_string(value).map_err(|e| {
                                    ProcessingError::OutputError(format!("JSON value encoding error: {}", e))
                                })?;
                                json_parts.push(format!("{}:{}", key_json, value_json));
                            }
                        }
                        writeln!(output, "{{{}}}", json_parts.join(","))?;
                    } else {
                        // Not an object, just serialize normally
                        let json_line = serde_json::to_string(data).map_err(|e| {
                            ProcessingError::OutputError(format!("JSON encoding error: {}", e))
                        })?;
                        writeln!(output, "{}", json_line)?;
                    }
                } else {
                    // Normal JSON serialization when no key ordering needed
                    let json_line = serde_json::to_string(data).map_err(|e| {
                        ProcessingError::OutputError(format!("JSON encoding error: {}", e))
                    })?;
                    writeln!(output, "{}", json_line)?;
                }
            }
        }
        Ok(())
    }

    fn write_csv<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        self.write_separated_values(output, record, ',')
    }

    fn write_tsv<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        self.write_separated_values(output, record, '\t')
    }

    fn write_separated_values<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
        separator: char,
    ) -> Result<(), ProcessingError> {
        let separator_str = separator.to_string();
        
        match record {
            RecordData::Text(text) => {
                if !self.csv_headers_written {
                    writeln!(output, "text")?;
                    self.csv_headers_written = true;
                }
                writeln!(output, "{}", self.field_escape(text, separator))?;
            }
            RecordData::Structured(data) => {
                let data = self.filter_keys(data);
                if let serde_json::Value::Object(obj) = data {
                    let key_order = self.get_key_order(&obj);
                    
                    // Write headers if not written yet
                    if !self.csv_headers_written {
                        writeln!(output, "{}", key_order.join(&separator_str))?;
                        self.csv_headers_written = true;
                        // Store schema keys for warning purposes (only when --keys not specified)
                        if self.keys.is_none() {
                            self.csv_schema_keys = Some(key_order.clone());
                        }
                    }
                    
                    // Check for missing keys and warn (only when --keys not specified)
                    if self.keys.is_none() {
                        if let Some(ref schema_keys) = self.csv_schema_keys {
                            let current_keys: std::collections::HashSet<String> = obj.keys().map(|s| s.clone()).collect();
                            let schema_keys_set: std::collections::HashSet<String> = schema_keys.iter().cloned().collect();
                            
                            let missing_keys: Vec<String> = current_keys.difference(&schema_keys_set)
                                .filter(|key| !self.missing_keys_warned.contains(*key))
                                .cloned()
                                .collect();
                            
                            if !missing_keys.is_empty() {
                                for key in &missing_keys {
                                    self.missing_keys_warned.insert(key.clone());
                                }
                            }
                        }
                    }

                    // Write values in the specified key order
                    let mut values = Vec::new();
                    for key in &key_order {
                        let value_str = match obj.get(key) {
                            Some(serde_json::Value::String(s)) => s.clone(),
                            Some(serde_json::Value::Number(n)) => n.to_string(),
                            Some(serde_json::Value::Bool(b)) => b.to_string(),
                            Some(serde_json::Value::Null) => String::new(),
                            Some(other) => {
                                serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                            }
                            None => String::new(), // Missing key - use empty value
                        };
                        values.push(self.field_escape(&value_str, separator));
                    }
                    writeln!(output, "{}", values.join(&separator_str))?;
                } else {
                    let format_name = if separator == '\t' { "TSV" } else { "CSV" };
                    return Err(ProcessingError::OutputError(
                        format!("{} format requires object records", format_name),
                    ));
                }
            }
        }
        Ok(())
    }

    fn write_logfmt<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match record {
            RecordData::Text(text) => {
                writeln!(output, "text={}", self.logfmt_escape(text))?;
            }
            RecordData::Structured(data) => {
                let data = self.filter_keys(data);
                if let Value::Object(obj) = data {
                    let key_order = self.get_key_order(&obj);
                    let mut pairs = Vec::new();
                    
                    for key in &key_order {
                        let value = &obj[key];
                        let value_str = match value {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Null => String::new(),
                            other => {
                                serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                            }
                        };
                        let key_clean = self.logfmt_escape_key(key);
                        let value_clean = self.logfmt_escape(&value_str);
                        pairs.push(format!("{}={}", key_clean, value_clean));
                    }
                    writeln!(output, "{}", pairs.join(" "))?;
                } else {
                    return Err(ProcessingError::OutputError(
                        "Logfmt format requires object records".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn write_fields<W: Write>(
        &mut self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match record {
            RecordData::Text(text) => {
                writeln!(output, "{}", text)?;
            }
            RecordData::Structured(data) => {
                let data = self.filter_keys(data);
                if let Value::Object(obj) = data {
                    let key_order = self.get_key_order(&obj);
                    let mut values = Vec::new();
                    
                    for key in &key_order {
                        if let Some(value) = obj.get(key) {
                            let value_str = match value {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                serde_json::Value::Null => String::new(),
                                other => {
                                    serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                                }
                            };
                            values.push(value_str);
                        }
                    }
                    writeln!(output, "{}", values.join(" "))?;
                } else {
                    return Err(ProcessingError::OutputError(
                        "Fields format requires object records".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn field_escape(&self, value: &str, separator: char) -> String {
        if value.contains(separator) || value.contains('"') || value.contains('\n') {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_string()
        }
    }

    fn logfmt_escape(&self, value: &str) -> String {
        if value.contains(' ') || value.contains('=') || value.contains('"') {
            format!("\"{}\"", value.replace('"', "\\\""))
        } else {
            value.to_string()
        }
    }

    fn logfmt_escape_key(&self, key: &str) -> String {
        key.replace(' ', "_").replace('=', "_")
    }

    pub fn reset(&mut self) {
        self.csv_headers_written = false;
        self.csv_schema_keys = None;
        self.missing_keys_warned.clear();
    }
    
    /// Report final CSV/TSV warnings about missing keys (call at end of processing)
    pub fn report_csv_warnings(&self) {
        if (self.format == OutputFormat::Csv || self.format == OutputFormat::Tsv) && self.keys.is_none() && !self.missing_keys_warned.is_empty() {
            if let Some(ref schema_keys) = self.csv_schema_keys {
                let mut all_keys = schema_keys.clone();
                let mut missing_keys: Vec<_> = self.missing_keys_warned.iter().cloned().collect();
                missing_keys.sort();
                all_keys.extend(missing_keys.iter().cloned());
                all_keys.sort();
                
                eprintln!("stelp: warning: keys '{}' found but not in CSV schema (based on first record)", 
                         missing_keys.join("', '"));
                eprintln!("stelp: suggestion: use --keys {} to include all data", 
                         all_keys.join(","));
            }
        }
    }
}