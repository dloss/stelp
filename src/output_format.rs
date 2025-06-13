use crate::error::ProcessingError;
use crate::pipeline::context::RecordData;
use serde_json::Value;
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum OutputFormat {
    Jsonl,
    Csv,
    Logfmt,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "jsonl" => Ok(OutputFormat::Jsonl),
            "csv" => Ok(OutputFormat::Csv),
            "logfmt" => Ok(OutputFormat::Logfmt),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

impl Default for OutputFormat {
    fn default() -> Self {
        OutputFormat::Jsonl
    }
}

pub struct OutputFormatter {
    format: OutputFormat,
    keys: Option<Vec<String>>,
    csv_headers_written: bool,
}

impl OutputFormatter {
    pub fn new(format: OutputFormat, keys: Option<Vec<String>>) -> Self {
        OutputFormatter {
            format,
            csv_headers_written: false,
            keys,
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
            // Use the order specified in --keys, but only include keys that exist in the object
            key_list.iter()
                .filter(|key| obj.contains_key(*key))
                .cloned()
                .collect()
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
            OutputFormat::Jsonl => self.write_jsonl(output, record),
            OutputFormat::Csv => self.write_csv(output, record),
            OutputFormat::Logfmt => self.write_logfmt(output, record),
        }
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
        match record {
            RecordData::Text(text) => {
                if !self.csv_headers_written {
                    writeln!(output, "text")?;
                    self.csv_headers_written = true;
                }
                writeln!(output, "{}", self.csv_escape(text))?;
            }
            RecordData::Structured(data) => {
                let data = self.filter_keys(data);
                if let serde_json::Value::Object(obj) = data {
                    let key_order = self.get_key_order(&obj);
                    
                    // Write headers if not written yet
                    if !self.csv_headers_written {
                        writeln!(output, "{}", key_order.join(","))?;
                        self.csv_headers_written = true;
                    }

                    // Write values in the specified key order
                    let mut values = Vec::new();
                    for key in &key_order {
                        let value_str = match &obj[key] {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Null => String::new(),
                            other => {
                                serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                            }
                        };
                        values.push(self.csv_escape(&value_str));
                    }
                    writeln!(output, "{}", values.join(","))?;
                } else {
                    return Err(ProcessingError::OutputError(
                        "CSV format requires object records".to_string(),
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

    fn csv_escape(&self, value: &str) -> String {
        if value.contains(',') || value.contains('"') || value.contains('\n') {
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
    }
}