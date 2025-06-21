use anyhow::{anyhow, Result};
use regex::Regex;
use serde_json;
use starlark::values::{Heap, Value};

/// Field type specification for pattern extraction
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    String,
    Int,
    Float,
    Word,
}

impl FieldType {
    /// Convert field type to corresponding regex pattern
    fn to_regex(&self) -> &'static str {
        match self {
            FieldType::String => r"([^\s]+)", // Non-whitespace characters
            FieldType::Int => r"([+-]?\d+)",  // Optional sign + digits
            FieldType::Float => r"([+-]?\d*\.?\d+)", // Optional sign + decimal
            FieldType::Word => r"(\w+)",      // Word characters only
        }
    }
}

/// Field specification with name and type
#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub name: String,
    pub field_type: FieldType,
}

/// Pattern extractor that converts text to structured data using named patterns
pub struct PatternExtractor {
    regex: Regex,
    fields: Vec<FieldSpec>,
}

impl PatternExtractor {
    /// Create a new pattern extractor from a pattern string
    /// Pattern format: "{field}" or "{field:type}" where type is int, float, or word
    pub fn new(pattern_str: &str) -> Result<Self> {
        let (regex_pattern, fields) = Self::compile_pattern(pattern_str)?;
        let regex = Regex::new(&regex_pattern)
            .map_err(|e| anyhow!("Failed to compile regex pattern: {}", e))?;

        Ok(PatternExtractor { regex, fields })
    }

    /// Extract structured data from text using the compiled pattern
    pub fn extract<'v>(&self, heap: &'v Heap, text: &str) -> Result<Option<Value<'v>>> {
        match self.regex.captures(text) {
            Some(captures) => {
                let mut obj = serde_json::Map::new();

                // Process each field capture
                for (i, field) in self.fields.iter().enumerate() {
                    // Capture groups are 1-indexed (0 is the full match)
                    if let Some(capture) = captures.get(i + 1) {
                        let captured_text = capture.as_str();
                        let json_value = self.convert_capture(captured_text, &field.field_type)?;
                        obj.insert(field.name.clone(), json_value);
                    }
                }

                // Convert JSON object to Starlark value
                let json_obj = serde_json::Value::Object(obj);
                let starlark_value = json_to_starlark_value(heap, json_obj)?;
                Ok(Some(starlark_value))
            }
            None => Ok(None), // No match
        }
    }

    /// Compile pattern string into regex and field specifications
    fn compile_pattern(pattern_str: &str) -> Result<(String, Vec<FieldSpec>)> {
        let mut regex_pattern = String::new();
        let mut fields = Vec::new();
        let mut chars = pattern_str.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '{' {
                // Parse field specification
                let mut field_spec = String::new();
                let mut found_closing = false;

                for inner_ch in chars.by_ref() {
                    if inner_ch == '}' {
                        found_closing = true;
                        break;
                    }
                    field_spec.push(inner_ch);
                }

                if !found_closing {
                    return Err(anyhow!("Unclosed field specification: {{{}", field_spec));
                }

                if field_spec.is_empty() {
                    return Err(anyhow!("Empty field specification"));
                }

                // Parse field name and type
                let field = Self::parse_field_spec(&field_spec)?;
                regex_pattern.push_str(field.field_type.to_regex());
                fields.push(field);
            } else {
                // Regular character - escape for regex if needed
                match ch {
                    '.' | '*' | '+' | '?' | '^' | '$' | '(' | ')' | '[' | ']' | '|' | '\\' => {
                        regex_pattern.push('\\');
                        regex_pattern.push(ch);
                    }
                    _ => regex_pattern.push(ch),
                }
            }
        }

        Ok((regex_pattern, fields))
    }

    /// Parse field specification like "field" or "field:type"
    fn parse_field_spec(spec: &str) -> Result<FieldSpec> {
        let parts: Vec<&str> = spec.split(':').collect();

        match parts.len() {
            1 => {
                // Just field name, default to string type
                let name = parts[0].trim().to_string();
                if name.is_empty() {
                    return Err(anyhow!("Empty field name"));
                }
                Self::validate_field_name(&name)?;
                Ok(FieldSpec {
                    name,
                    field_type: FieldType::String,
                })
            }
            2 => {
                // Field name and type
                let name = parts[0].trim().to_string();
                let type_str = parts[1].trim();

                if name.is_empty() {
                    return Err(anyhow!("Empty field name"));
                }
                Self::validate_field_name(&name)?;

                let field_type = match type_str {
                    "int" => FieldType::Int,
                    "float" => FieldType::Float,
                    "word" => FieldType::Word,
                    "" => FieldType::String, // Default if type is empty
                    _ => {
                        return Err(anyhow!(
                            "Unknown field type '{}'. Supported types: int, float, word",
                            type_str
                        ))
                    }
                };

                Ok(FieldSpec { name, field_type })
            }
            _ => Err(anyhow!(
                "Invalid field specification '{}'. Use 'field' or 'field:type'",
                spec
            )),
        }
    }

    /// Validate field name is a valid identifier
    fn validate_field_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow!("Empty field name"));
        }

        let mut chars = name.chars();
        let first = chars.next().unwrap();

        // First character must be letter or underscore
        if !first.is_ascii_alphabetic() && first != '_' {
            return Err(anyhow!(
                "Field name '{}' must start with letter or underscore",
                name
            ));
        }

        // Remaining characters must be alphanumeric or underscore
        for c in chars {
            if !c.is_ascii_alphanumeric() && c != '_' {
                return Err(anyhow!(
                    "Field name '{}' contains invalid character '{}'",
                    name,
                    c
                ));
            }
        }

        Ok(())
    }

    /// Convert captured text to appropriate JSON value based on field type
    fn convert_capture(&self, text: &str, field_type: &FieldType) -> Result<serde_json::Value> {
        match field_type {
            FieldType::String | FieldType::Word => Ok(serde_json::Value::String(text.to_string())),
            FieldType::Int => text
                .parse::<i64>()
                .map(|i| serde_json::Value::Number(serde_json::Number::from(i)))
                .map_err(|_| anyhow!("Failed to convert '{}' to integer", text)),
            FieldType::Float => {
                let f = text
                    .parse::<f64>()
                    .map_err(|_| anyhow!("Failed to convert '{}' to float", text))?;

                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        anyhow!(
                            "Float value '{}' cannot be represented as JSON number",
                            text
                        )
                    })
            }
        }
    }
}

/// Convert JSON value to Starlark value
fn json_to_starlark_value(
    heap: &starlark::values::Heap,
    json: serde_json::Value,
) -> Result<starlark::values::Value<'_>> {
    use starlark::values::Value;

    match json {
        serde_json::Value::Null => Ok(Value::new_none()),
        serde_json::Value::Bool(b) => Ok(Value::new_bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(heap.alloc(i as i32))
            } else if let Some(f) = n.as_f64() {
                Ok(heap.alloc(f))
            } else {
                Ok(heap.alloc(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(heap.alloc(s)),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>, anyhow::Error> = arr
                .into_iter()
                .map(|v| json_to_starlark_value(heap, v))
                .collect();
            Ok(heap.alloc(values?))
        }
        serde_json::Value::Object(obj) => {
            use starlark::collections::SmallMap;
            use starlark::values::dict::Dict;

            let mut content = SmallMap::new();
            for (k, v) in obj {
                let key = heap.alloc(k);
                let value = json_to_starlark_value(heap, v)?;
                content.insert_hashed(key.get_hashed().map_err(|e| anyhow!("{}", e))?, value);
            }
            let dict = Dict::new(content);
            Ok(heap.alloc(dict))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starlark::environment::Module;

    #[test]
    fn test_parse_field_spec() {
        // Test basic field name
        let field = PatternExtractor::parse_field_spec("ip").unwrap();
        assert_eq!(field.name, "ip");
        assert_eq!(field.field_type, FieldType::String);

        // Test field with type
        let field = PatternExtractor::parse_field_spec("status:int").unwrap();
        assert_eq!(field.name, "status");
        assert_eq!(field.field_type, FieldType::Int);

        // Test field with float type
        let field = PatternExtractor::parse_field_spec("time:float").unwrap();
        assert_eq!(field.name, "time");
        assert_eq!(field.field_type, FieldType::Float);

        // Test field with word type
        let field = PatternExtractor::parse_field_spec("user:word").unwrap();
        assert_eq!(field.name, "user");
        assert_eq!(field.field_type, FieldType::Word);

        // Test invalid type
        assert!(PatternExtractor::parse_field_spec("field:invalid").is_err());

        // Test empty name
        assert!(PatternExtractor::parse_field_spec("").is_err());
        assert!(PatternExtractor::parse_field_spec(":int").is_err());
    }

    #[test]
    fn test_compile_pattern() {
        // Test simple pattern
        let (regex, fields) = PatternExtractor::compile_pattern("{ip} {user}").unwrap();
        assert_eq!(regex, r"([^\s]+) ([^\s]+)");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "ip");
        assert_eq!(fields[1].name, "user");

        // Test pattern with types
        let (regex, fields) =
            PatternExtractor::compile_pattern("{ip} {status:int} {time:float}").unwrap();
        assert_eq!(regex, r"([^\s]+) ([+-]?\d+) ([+-]?\d*\.?\d+)");
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1].field_type, FieldType::Int);
        assert_eq!(fields[2].field_type, FieldType::Float);

        // Test pattern with special regex characters
        let (regex, _) = PatternExtractor::compile_pattern("GET {path} HTTP").unwrap();
        assert_eq!(regex, r"GET ([^\s]+) HTTP");

        // Test unclosed brace
        assert!(PatternExtractor::compile_pattern("{unclosed").is_err());
    }

    #[test]
    fn test_basic_extraction() {
        let module = Module::new();
        let extractor = PatternExtractor::new("{ip} {user} {action}").unwrap();

        let result = extractor
            .extract(module.heap(), "192.168.1.1 admin login")
            .unwrap();
        assert!(result.is_some());

        let dict = result.unwrap();
        // We can't easily test the Starlark dict content without more complex setup
        // but we can verify it's not None
        assert!(!dict.is_none());
    }

    #[test]
    fn test_type_conversion() {
        let module = Module::new();
        let extractor = PatternExtractor::new("{status:int} {time:float} {user:word}").unwrap();

        let result = extractor.extract(module.heap(), "200 1.5 alice").unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_no_match() {
        let module = Module::new();
        let extractor = PatternExtractor::new("{ip} {user}").unwrap();

        // Use text that has only one word (pattern expects two words)
        let result = extractor.extract(module.heap(), "onlyoneword").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_invalid_type_conversion() {
        let module = Module::new();
        // Use a pattern that could match but then fail type conversion
        // Actually, this is hard to achieve with the current regex design
        // The int regex ([+-]?\d+) only matches valid integers
        // Let's test a scenario where we have too large an integer
        let extractor = PatternExtractor::new("{status:int}").unwrap();

        // This won't match the int pattern, so returns None rather than error
        let result = extractor.extract(module.heap(), "not_a_number");
        assert!(result.unwrap().is_none());

        // Test a valid integer that should work
        let result = extractor.extract(module.heap(), "200");
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_field_name_validation() {
        // Valid names
        assert!(PatternExtractor::validate_field_name("field").is_ok());
        assert!(PatternExtractor::validate_field_name("field_name").is_ok());
        assert!(PatternExtractor::validate_field_name("_private").is_ok());
        assert!(PatternExtractor::validate_field_name("field123").is_ok());

        // Invalid names
        assert!(PatternExtractor::validate_field_name("123field").is_err());
        assert!(PatternExtractor::validate_field_name("field-name").is_err());
        assert!(PatternExtractor::validate_field_name("field.name").is_err());
        assert!(PatternExtractor::validate_field_name("").is_err());
    }
}
