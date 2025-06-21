use crate::colors::ColorScheme;
use crate::formatters::RecordFormatter;
use crate::pipeline::context::RecordData;
use serde_json::Value;
use std::collections::HashMap;

/// Standard logfmt formatter with colored output
pub struct LogfmtFormatter {
    colors: ColorScheme,
    timestamp_keys: Vec<&'static str>,
    level_keys: Vec<&'static str>,
    message_keys: Vec<&'static str>,
}

impl LogfmtFormatter {
    pub fn new(use_colors: bool) -> Self {
        Self {
            colors: ColorScheme::new(use_colors),
            timestamp_keys: crate::pipeline::config::TIMESTAMP_KEYS.to_vec(),
            level_keys: vec![
                "level",
                "loglevel",
                "log_level",
                "lvl",
                "severity",
                "levelname",
                "@l",
            ],
            message_keys: vec!["message", "msg", "@m", "@message", "text", "content"],
        }
    }

    /// Convert JSON Value to HashMap of strings for easier processing
    fn value_to_string_map(&self, value: &Value) -> HashMap<String, String> {
        let mut map = HashMap::new();

        if let Value::Object(obj) = value {
            for (key, val) in obj {
                let value_str = match val {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => String::new(),
                    other => serde_json::to_string(other).unwrap_or_else(|_| "null".to_string()),
                };
                map.insert(key.clone(), value_str);
            }
        }

        map
    }

    /// Format structured fields as standard logfmt (space-separated key=value)
    pub fn format_fields_standard(&self, fields: &HashMap<String, String>) -> String {
        if fields.is_empty() {
            return String::new();
        }

        let sorted_fields = self.sort_fields_by_priority(fields);
        let formatted_pairs: Vec<String> = sorted_fields
            .iter()
            .map(|(key, value)| self.format_key_value_pair(key, value))
            .collect();

        formatted_pairs.join(" ")
    }

    /// Sort fields by priority: timestamp, level, message, then alphabetical
    pub fn sort_fields_by_priority<'a>(
        &self,
        fields: &'a HashMap<String, String>,
    ) -> Vec<(&'a String, &'a String)> {
        let mut sorted_fields: Vec<(&String, &String)> = fields.iter().collect();
        sorted_fields.sort_by_key(|(key, _)| {
            if self.is_timestamp_field(key) {
                (0, key.as_str())
            } else if self.is_level_field(key) {
                (1, key.as_str())
            } else if self.is_message_field(key) {
                (2, key.as_str())
            } else {
                (3, key.as_str())
            }
        });
        sorted_fields
    }

    /// Format a single key=value pair with appropriate colors
    pub fn format_key_value_pair(&self, key: &str, value: &str) -> String {
        let colored_key = if self.colors.key.is_empty() {
            key.to_string()
        } else {
            format!("{}{}{}", self.colors.key, key, self.colors.reset)
        };

        let equals = if self.colors.equals.is_empty() {
            "=".to_string()
        } else {
            format!("{}{}{}", self.colors.equals, "=", self.colors.reset)
        };

        let colored_value = self.format_value(key, value);

        format!("{}{}{}", colored_key, equals, colored_value)
    }

    /// Format a value with color and quoting based on content and field type
    fn format_value(&self, key: &str, value: &str) -> String {
        // Choose color based on field type and value content
        let color = if self.is_level_field(key) {
            self.level_color(value)
        } else {
            // Most values are uncolored in klp default scheme
            self.colors.string
        };

        // Quote value if it contains spaces or special characters
        let quoted_value = if self.needs_quoting(value) {
            format!("\"{}\"", self.escape_quotes(value))
        } else {
            value.to_string()
        };

        if color.is_empty() {
            quoted_value
        } else {
            format!("{}{}{}", color, quoted_value, self.colors.reset)
        }
    }

    /// Format a value for plain mode (only value with color, minimal quoting)
    fn format_value_plain(&self, key: &str, value: &str) -> String {
        // Choose color based on field type and value content
        let color = if self.is_level_field(key) {
            self.level_color(value)
        } else {
            // Most values are uncolored in klp default scheme
            self.colors.string
        };

        // In plain mode, don't quote values - just output them as-is
        // Users can handle spacing/parsing themselves since there are no keys
        let display_value = value.to_string();

        if color.is_empty() {
            display_value
        } else {
            format!("{}{}{}", color, display_value, self.colors.reset)
        }
    }

    /// Format structured fields in plain mode (space-separated values only, no keys)
    pub fn format_fields_plain(&self, fields: &HashMap<String, String>) -> String {
        if fields.is_empty() {
            return String::new();
        }

        let sorted_fields = self.sort_fields_by_priority(fields);
        let formatted_values: Vec<String> = sorted_fields
            .iter()
            .map(|(key, value)| self.format_value_plain(key, value))
            .collect();

        formatted_values.join(" ")
    }

    /// Get appropriate color for log level values
    fn level_color(&self, level: &str) -> &str {
        match level.to_lowercase().as_str() {
            // Bright red for error levels
            "error" | "err" | "fatal" | "panic" | "alert" | "crit" | "critical" | "emerg"
            | "emergency" | "severe" => self.colors.level_error,
            // Bright yellow for warning levels
            "warn" | "warning" => self.colors.level_warn,
            // Bright green for info levels
            "info" | "informational" | "notice" => self.colors.level_info,
            // Bright cyan for debug levels
            "debug" | "finer" | "config" => self.colors.level_debug,
            // Cyan for trace levels
            "trace" | "finest" => self.colors.level_trace,
            // Default to no color for unknown levels
            _ => "",
        }
    }

    /// Check if key is likely a timestamp field
    fn is_timestamp_field(&self, key: &str) -> bool {
        self.timestamp_keys.iter().any(|&tk| tk == key)
    }

    /// Check if key is likely a log level field
    fn is_level_field(&self, key: &str) -> bool {
        self.level_keys.iter().any(|&lk| lk == key)
    }

    /// Check if key is likely a message field
    fn is_message_field(&self, key: &str) -> bool {
        self.message_keys.iter().any(|&mk| mk == key)
    }

    /// Check if value needs to be quoted per logfmt rules
    fn needs_quoting(&self, value: &str) -> bool {
        // Quote values that contain spaces, tabs, newlines, quotes, or equals
        value.is_empty()
            || value.contains(' ')
            || value.contains('\t')
            || value.contains('\n')
            || value.contains('"')
            || value.contains('=')
    }

    /// Escape quotes in values per logfmt rules
    fn escape_quotes(&self, value: &str) -> String {
        // Escape quotes with backslashes
        value.replace('"', "\\\"")
    }
}

impl RecordFormatter for LogfmtFormatter {
    fn format_record(&self, record: &RecordData) -> String {
        match record {
            RecordData::Text(text) => text.clone(),
            RecordData::Structured(data) => {
                let fields = self.value_to_string_map(data);
                self.format_fields_standard(&fields)
            }
        }
    }
}

impl LogfmtFormatter {
    /// Format record in plain mode (values only, no keys)
    pub fn format_record_plain(&self, record: &RecordData) -> String {
        match record {
            RecordData::Text(text) => text.clone(),
            RecordData::Structured(data) => {
                let fields = self.value_to_string_map(data);
                self.format_fields_plain(&fields)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_format_simple_fields() {
        let formatter = LogfmtFormatter::new(false); // No colors for testing
        let mut fields = HashMap::new();
        fields.insert("level".to_string(), "info".to_string());
        fields.insert("message".to_string(), "test message".to_string());
        fields.insert("count".to_string(), "42".to_string());

        let result = formatter.format_fields_standard(&fields);
        // Should prioritize level and message, quote message due to space
        assert!(result.contains("level=info"));
        assert!(result.contains("message=\"test message\""));
        assert!(result.contains("count=42"));
    }

    #[test]
    fn test_field_priority_order() {
        let formatter = LogfmtFormatter::new(false);
        let mut fields = HashMap::new();
        fields.insert("zebra".to_string(), "last".to_string());
        fields.insert("timestamp".to_string(), "2024-01-01T10:00:00Z".to_string());
        fields.insert("level".to_string(), "error".to_string());
        fields.insert("message".to_string(), "test".to_string());
        fields.insert("alpha".to_string(), "middle".to_string());

        let result = formatter.format_fields_standard(&fields);
        let parts: Vec<&str> = result.split_whitespace().collect();

        // timestamp should come first, then level, then message
        assert!(parts[0].starts_with("timestamp="));
        assert!(parts[1].starts_with("level="));
        assert!(parts[2].starts_with("message="));
        // alpha should come before zebra (alphabetical)
        assert!(result.find("alpha=").unwrap() < result.find("zebra=").unwrap());
    }

    #[test]
    fn test_colored_vs_plain_output() {
        let colored = LogfmtFormatter::new(true);
        let plain = LogfmtFormatter::new(false);

        let mut fields = HashMap::new();
        fields.insert("level".to_string(), "error".to_string());

        let colored_result = colored.format_fields_standard(&fields);
        let plain_result = plain.format_fields_standard(&fields);

        // Colored should contain ANSI codes
        assert!(colored_result.contains("\x1b["));
        // Plain should not
        assert!(!plain_result.contains("\x1b["));
        // Both should contain the key and value (though possibly separated by color codes)
        assert!(colored_result.contains("level"));
        assert!(colored_result.contains("error"));
        assert!(plain_result.contains("level=error"));
    }

    #[test]
    fn test_quoting_behavior() {
        let formatter = LogfmtFormatter::new(false);
        let mut fields = HashMap::new();

        fields.insert("simple".to_string(), "value".to_string());
        fields.insert("spaced".to_string(), "has spaces".to_string());
        fields.insert("empty".to_string(), "".to_string());
        fields.insert("quoted".to_string(), "has\"quotes".to_string());

        let result = formatter.format_fields_standard(&fields);

        assert!(result.contains("simple=value")); // No quotes needed
        assert!(result.contains("spaced=\"has spaces\"")); // Quotes due to space
        assert!(result.contains("empty=\"\"")); // Quotes due to empty
        assert!(result.contains("quoted=\"has\\\"quotes\"")); // Escaped quotes
    }

    #[test]
    fn test_record_formatter_trait() {
        let formatter = LogfmtFormatter::new(false);

        // Test text record
        let text_record = RecordData::Text("hello world".to_string());
        assert_eq!(formatter.format_record(&text_record), "hello world");

        // Test structured record
        let structured_data = json!({
            "level": "info",
            "message": "test"
        });
        let structured_record = RecordData::Structured(structured_data);
        let result = formatter.format_record(&structured_record);
        assert!(result.contains("level=info"));
        assert!(result.contains("message=test"));
    }
}
