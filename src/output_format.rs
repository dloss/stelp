// src/output_format.rs
use crate::pipeline::context::RecordData;
use crate::error::ProcessingError;
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
    csv_headers_written: bool,
}

impl OutputFormatter {
    pub fn new(format: OutputFormat) -> Self {
        OutputFormatter {
            format,
            csv_headers_written: false,
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
                // For text records in jsonl, output as plain text (not JSON-encoded)
                // This maintains backward compatibility with existing behavior
                writeln!(output, "{}", text)?;
            }
            RecordData::Structured(data) => {
                // For structured records, output as JSON
                let json_line = serde_json::to_string(data)
                    .map_err(|e| ProcessingError::OutputError(format!("JSON encoding error: {}", e)))?;
                writeln!(output, "{}", json_line)?;
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
                // For text records, treat as single column
                if !self.csv_headers_written {
                    writeln!(output, "text")?;
                    self.csv_headers_written = true;
                }
                writeln!(output, "{}", self.csv_escape(text))?;
            }
            RecordData::Structured(data) => {
                if let Value::Object(obj) = data {
                    // Write headers if not written yet
                    if !self.csv_headers_written {
                        let headers: Vec<String> = obj.keys().cloned().collect();
                        writeln!(output, "{}", headers.join(","))?;
                        self.csv_headers_written = true;
                    }

                    // Write values in key order (consistent with headers)
                    let mut values = Vec::new();
                    for key in obj.keys() {
                        let value_str = match &obj[key] {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => String::new(),
                            other => serde_json::to_string(other)
                                .unwrap_or_else(|_| "null".to_string()),
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
                // For text records, use 'text' as the key
                writeln!(output, "text={}", self.logfmt_escape(text))?;
            }
            RecordData::Structured(data) => {
                if let Value::Object(obj) = data {
                    let mut pairs = Vec::new();
                    for (key, value) in obj {
                        let value_str = match value {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => String::new(),
                            other => serde_json::to_string(other)
                                .unwrap_or_else(|_| "null".to_string()),
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
        // Keys in logfmt should not contain spaces or special chars
        key.replace(' ', "_").replace('=', "_")
    }

    pub fn reset(&mut self) {
        // Reset state for new stream (e.g., CSV headers)
        self.csv_headers_written = false;
    }
}