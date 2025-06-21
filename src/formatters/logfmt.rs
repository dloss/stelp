use crate::colors::ColorScheme;
use crate::formatters::RecordFormatter;
use crate::pipeline::context::RecordData;
use serde_json::Value;
use indexmap::IndexMap;

/// Standard logfmt formatter with colored output
pub struct LogfmtFormatter {
    colors: ColorScheme,
    level_keys: Vec<&'static str>,
}

impl LogfmtFormatter {
    pub fn new(use_colors: bool) -> Self {
        Self {
            colors: ColorScheme::new(use_colors),
            level_keys: vec![
                "level",
                "loglevel",
                "log_level",
                "lvl",
                "severity",
                "levelname",
                "@l",
            ],
        }
    }

    /// Convert JSON Value to IndexMap of strings for easier processing
    fn value_to_string_map(&self, value: &Value) -> IndexMap<String, String> {
        let mut map = IndexMap::new();

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
    pub fn format_fields_standard(&self, fields: &IndexMap<String, String>) -> String {
        self.format_fields_with_order(fields, None)
    }

    /// Format fields with explicit key ordering
    pub fn format_fields_with_order(&self, fields: &IndexMap<String, String>, key_order: Option<&[String]>) -> String {
        if fields.is_empty() {
            return String::new();
        }

        // Pre-allocate buffer with estimated capacity
        let estimated_capacity = fields.len() * 32; // Rough estimate per field
        let mut output = String::with_capacity(estimated_capacity);
        
        let ordered_fields = if let Some(order) = key_order {
            // Use provided order, then remaining fields in original order
            let mut result = Vec::new();
            let mut used_keys = std::collections::HashSet::new();
            
            // First, add fields in the specified order
            for key in order {
                if let Some(value) = fields.get(key) {
                    result.push((key, value));
                    used_keys.insert(key);
                }
            }
            
            // Then add remaining fields in original order
            for (key, value) in fields {
                if !used_keys.contains(key) {
                    result.push((key, value));
                }
            }
            
            result
        } else {
            // Use original field order (no sorting)
            fields.iter().collect()
        };
        
        let mut first = true;
        for (key, value) in ordered_fields {
            if !first {
                output.push(' ');
            }
            first = false;
            
            self.format_key_value_pair_into(key, value, &mut output);
        }

        output
    }


    /// Format a single key=value pair with appropriate colors
    pub fn format_key_value_pair(&self, key: &str, value: &str) -> String {
        let mut output = String::with_capacity(key.len() + value.len() + 10); // +10 for colors/equals
        self.format_key_value_pair_into(key, value, &mut output);
        output
    }

    /// Format a single key=value pair directly into a buffer (zero-allocation)
    fn format_key_value_pair_into(&self, key: &str, value: &str, output: &mut String) {
        // Write colored key
        if !self.colors.key.is_empty() {
            output.push_str(&self.colors.key);
        }
        output.push_str(key);
        if !self.colors.key.is_empty() {
            output.push_str(&self.colors.reset);
        }

        // Write colored equals
        if !self.colors.equals.is_empty() {
            output.push_str(&self.colors.equals);
        }
        output.push('=');
        if !self.colors.equals.is_empty() {
            output.push_str(&self.colors.reset);
        }

        // Write colored value
        self.format_value_into(key, value, output);
    }


    /// Format a value directly into a buffer (zero-allocation)
    fn format_value_into(&self, key: &str, value: &str, output: &mut String) {
        // Choose color based on field type and value content
        let color = if self.is_level_field(key) {
            self.level_color(value)
        } else {
            // Most values are uncolored in klp default scheme
            self.colors.string
        };

        // Apply color
        if !color.is_empty() {
            output.push_str(color);
        }

        // Quote and write value if it contains spaces or special characters
        if self.needs_quoting(value) {
            output.push('"');
            // Inline escape quotes to avoid allocation
            for ch in value.chars() {
                if ch == '"' {
                    output.push_str("\\\"");
                } else {
                    output.push(ch);
                }
            }
            output.push('"');
        } else {
            output.push_str(value);
        }

        // Reset color
        if !color.is_empty() {
            output.push_str(&self.colors.reset);
        }
    }


    /// Format a value for plain mode directly into a buffer (zero-allocation)
    fn format_value_plain_into(&self, key: &str, value: &str, output: &mut String) {
        // Choose color based on field type and value content
        let color = if self.is_level_field(key) {
            self.level_color(value)
        } else {
            // Most values are uncolored in klp default scheme
            self.colors.string
        };

        // Apply color
        if !color.is_empty() {
            output.push_str(color);
        }

        // In plain mode, don't quote values - just output them as-is
        output.push_str(value);

        // Reset color
        if !color.is_empty() {
            output.push_str(&self.colors.reset);
        }
    }

    /// Format structured fields in plain mode (space-separated values only, no keys)
    pub fn format_fields_plain(&self, fields: &IndexMap<String, String>) -> String {
        if fields.is_empty() {
            return String::new();
        }

        // Pre-allocate buffer with estimated capacity
        let estimated_capacity = fields.len() * 16; // Rough estimate per value
        let mut output = String::with_capacity(estimated_capacity);
        
        // Use original field order - no sorting needed for performance
        let mut first = true;
        
        for (key, value) in fields {
            if !first {
                output.push(' ');
            }
            first = false;
            
            self.format_value_plain_into(key, value, &mut output);
        }

        output
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

    /// Check if key is likely a log level field
    fn is_level_field(&self, key: &str) -> bool {
        self.level_keys.iter().any(|&lk| lk == key)
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

    /// Fast JSON formatting that preserves order without IndexMap conversion
    fn format_json_as_logfmt_fast(&self, data: &Value) -> String {
        if let Value::Object(obj) = data {
            if obj.is_empty() {
                return String::new();
            }

            // Pre-allocate buffer with estimated capacity
            let estimated_capacity = obj.len() * 32;
            let mut output = String::with_capacity(estimated_capacity);
            let mut first = true;

            for (key, value) in obj {
                if !first {
                    output.push(' ');
                }
                first = false;

                // Format key=value directly
                self.format_json_key_value_into(key, value, &mut output);
            }

            output
        } else {
            // Fallback for non-object JSON
            let fields = self.value_to_string_map(data);
            self.format_fields_with_order(&fields, None)
        }
    }

    /// Format a JSON key=value pair directly into buffer
    fn format_json_key_value_into(&self, key: &str, value: &Value, output: &mut String) {
        // Write colored key
        if !self.colors.key.is_empty() {
            output.push_str(&self.colors.key);
        }
        output.push_str(key);
        if !self.colors.key.is_empty() {
            output.push_str(&self.colors.reset);
        }

        // Write colored equals
        if !self.colors.equals.is_empty() {
            output.push_str(&self.colors.equals);
        }
        output.push('=');
        if !self.colors.equals.is_empty() {
            output.push_str(&self.colors.reset);
        }

        // Format value directly from JSON
        self.format_json_value_into(key, value, output);
    }

    /// Format a JSON value directly into buffer  
    fn format_json_value_into(&self, key: &str, value: &Value, output: &mut String) {
        // Choose color based on field type and value content
        let color = if self.is_level_field(key) {
            match value {
                Value::String(s) => self.level_color(s),
                _ => "",
            }
        } else {
            self.colors.string
        };

        // Apply color
        if !color.is_empty() {
            output.push_str(color);
        }

        // Convert JSON value to string and write with quoting
        match value {
            Value::String(s) => {
                // Quote and escape if needed
                if self.needs_quoting(s) {
                    output.push('"');
                    if s.contains('"') {
                        output.push_str(&s.replace('"', "\\\""));
                    } else {
                        output.push_str(s);
                    }
                    output.push('"');
                } else {
                    output.push_str(s);
                }
            }
            Value::Number(n) => {
                output.push_str(&n.to_string());
            }
            Value::Bool(b) => {
                output.push_str(&b.to_string());
            }
            Value::Null => {
                // Empty for null
            }
            other => {
                // Complex types - serialize and potentially quote
                let serialized = serde_json::to_string(other).unwrap_or_else(|_| "null".to_string());
                if self.needs_quoting(&serialized) {
                    output.push('"');
                    if serialized.contains('"') {
                        output.push_str(&serialized.replace('"', "\\\""));
                    } else {
                        output.push_str(&serialized);
                    }
                    output.push('"');
                } else {
                    output.push_str(&serialized);
                }
            }
        }

        // Reset color
        if !color.is_empty() {
            output.push_str(&self.colors.reset);
        }
    }

}

impl RecordFormatter for LogfmtFormatter {
    fn format_record(&self, record: &RecordData) -> String {
        match record {
            RecordData::Text(text) => text.clone(),
            RecordData::Structured(data) => {
                // Fast path: use direct JSON formatting when no ordering needed
                self.format_json_as_logfmt_fast(data)
            }
        }
    }
    
    fn format_record_with_key_order(&self, record: &RecordData, key_order: Option<&[String]>) -> String {
        match record {
            RecordData::Text(text) => text.clone(),
            RecordData::Structured(data) => {
                let fields = self.value_to_string_map(data);
                self.format_fields_with_order(&fields, key_order)
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
    use indexmap::IndexMap;

    #[test]
    fn test_format_simple_fields() {
        let formatter = LogfmtFormatter::new(false); // No colors for testing
        let mut fields = IndexMap::new();
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
    fn test_colored_vs_plain_output() {
        let colored = LogfmtFormatter::new(true);
        let plain = LogfmtFormatter::new(false);

        let mut fields = IndexMap::new();
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
        let mut fields = IndexMap::new();

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
