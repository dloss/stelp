// src/pipeline/global_functions.rs
use crate::variables::GlobalVariables;
use starlark::starlark_module;
use starlark::values::{Heap, Value};
use std::cell::{Cell, RefCell};

thread_local! {
    pub(crate) static EMIT_BUFFER: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    pub(crate) static SKIP_FLAG: Cell<bool> = const { Cell::new(false) };
    pub(crate) static EXIT_FLAG: Cell<bool> = const { Cell::new(false) };
    pub(crate) static EXIT_MESSAGE: RefCell<Option<String>> = const { RefCell::new(None) };
    pub(crate) static CURRENT_CONTEXT: RefCell<Option<(*const GlobalVariables, usize, Option<String>)>> = const { RefCell::new(None) };
}

#[starlark_module]
pub(crate) fn global_functions(builder: &mut starlark::environment::GlobalsBuilder) {
    // Control flow functions
    fn emit(text: String) -> anyhow::Result<starlark::values::none::NoneType> {
        EMIT_BUFFER.with(|buffer| {
            buffer.borrow_mut().push(text);
        });
        Ok(starlark::values::none::NoneType)
    }

    fn skip() -> anyhow::Result<starlark::values::none::NoneType> {
        SKIP_FLAG.with(|flag| flag.set(true));
        Ok(starlark::values::none::NoneType)
    }

    fn exit(message: Option<String>) -> anyhow::Result<starlark::values::none::NoneType> {
        EXIT_FLAG.with(|flag| flag.set(true));
        EXIT_MESSAGE.with(|msg| {
            *msg.borrow_mut() = message;
        });
        Ok(starlark::values::none::NoneType)
    }

    fn print(message: String) -> anyhow::Result<starlark::values::none::NoneType> {
        eprintln!("{}", message);
        Ok(starlark::values::none::NoneType)
    }

    // Text processing functions
    fn regex_match(pattern: String, text: String) -> anyhow::Result<bool> {
        match regex::Regex::new(&pattern) {
            Ok(regex) => Ok(regex.is_match(&text)),
            Err(_) => Ok(false),
        }
    }

    fn regex_replace(pattern: String, replacement: String, text: String) -> anyhow::Result<String> {
        let regex = regex::Regex::new(&pattern)?;
        Ok(regex.replace_all(&text, replacement.as_str()).into_owned())
    }

    fn regex_find_all<'v>(
        heap: &'v Heap,
        pattern: String,
        text: String,
    ) -> anyhow::Result<Value<'v>> {
        let regex = regex::Regex::new(&pattern)?;
        let matches: Vec<Value> = regex
            .find_iter(&text)
            .map(|m| heap.alloc(m.as_str().to_string()))
            .collect();
        Ok(heap.alloc(matches))
    }

    // JSON functions
    fn parse_json<'v>(heap: &'v Heap, text: String) -> anyhow::Result<Value<'v>> {
        let json_value: serde_json::Value = serde_json::from_str(&text)?;
        json_to_starlark_value(heap, json_value)
    }

    fn dump_json(value: Value) -> anyhow::Result<String> {
        let json_value = starlark_to_json_value(value)?;
        Ok(serde_json::to_string(&json_value)?)
    }

    // CSV functions
    fn parse_csv<'v>(
        heap: &'v Heap,
        line: String,
        headers: Option<Value>,
        sep: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        use starlark::collections::SmallMap;
        use starlark::values::{dict::Dict, list::ListRef};

        let separator = sep.unwrap_or_else(|| ",".to_string());
        let sep_char = separator.chars().next().unwrap_or(',');

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(sep_char as u8)
            .has_headers(false)
            .from_reader(line.as_bytes());

        if let Some(record) = reader.records().next() {
            let record = record?;
            let fields: Vec<String> = record.iter().map(|field| field.to_string()).collect();

            // Handle headers
            let column_names = if let Some(headers_val) = headers {
                // Extract headers from list value
                if let Some(list) = ListRef::from_value(headers_val) {
                    list.iter()
                        .map(|v| {
                            // Extract the actual string content, not the debug representation
                            if let Some(s) = v.unpack_str() {
                                s.to_string()
                            } else {
                                v.to_string() // Fallback to debug representation
                            }
                        })
                        .collect::<Vec<String>>()
                } else {
                    return Err(anyhow::anyhow!("headers must be a list"));
                }
            } else {
                // Auto-generate col1, col2, col3...
                (1..=fields.len()).map(|i| format!("col{}", i)).collect()
            };

            // Create dict from headers and fields
            let mut dict_map = SmallMap::new();

            for (i, header) in column_names.iter().enumerate() {
                let value = if i < fields.len() {
                    fields[i].clone()
                } else {
                    // More headers than fields - fill with empty string
                    String::new()
                };

                // Allocate key and value as strings directly
                let key = heap.alloc(header.as_str()); // Use as_str() to avoid extra quotes
                let val = heap.alloc(value);
                dict_map.insert_hashed(key.get_hashed().unwrap(), val);
            }

            // Note: if there are more fields than headers, we ignore the extra fields

            let dict = Dict::new(dict_map);
            Ok(heap.alloc(dict))
        } else {
            // Empty line - return empty dict
            let dict = Dict::new(SmallMap::new());
            Ok(heap.alloc(dict))
        }
    }

    fn dump_csv(values: Value, delimiter: Option<String>) -> anyhow::Result<String> {
        use starlark::values::list::ListRef;

        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');

        let list = ListRef::from_value(values)
            .ok_or_else(|| anyhow::anyhow!("Expected list for dump_csv"))?;

        let mut writer = csv::WriterBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(Vec::new());

        let fields: Vec<String> = list
            .iter()
            .map(|v| {
                let s = v.to_string();
                if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                    s[1..s.len() - 1].to_string()
                } else {
                    s
                }
            })
            .collect();

        writer.write_record(&fields)?;
        let data = writer.into_inner()?;
        let result = String::from_utf8(data)?;
        Ok(result.trim_end().to_string())
    }

    // Key-value parsing
    fn parse_kv<'v>(
        heap: &'v Heap,
        line: String,
        sep: Option<String>,
        delim: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let separator = sep.unwrap_or_else(|| "=".to_string());
        let delimiter = delim.unwrap_or_else(|| " ".to_string());

        let mut items = Vec::new();
        for pair in line.split(&delimiter) {
            if let Some((key, value)) = pair.split_once(&separator) {
                let k = key.trim();
                let v = value.trim();
                items.push(format!("{}: {}", k, v));
            }
        }

        let dict_str = format!("{{{}}}", items.join(", "));
        Ok(heap.alloc(dict_str))
    }

    // Standard functions
    fn str<'v>(heap: &'v Heap, value: Value<'v>) -> anyhow::Result<Value<'v>> {
        Ok(heap.alloc(value.to_string()))
    }

    fn len<'v>(value: Value<'v>) -> anyhow::Result<i32> {
        use starlark::values::{dict::DictRef, list::ListRef};

        if let Some(s) = value.unpack_str() {
            Ok(s.len() as i32)
        } else if let Some(list) = ListRef::from_value(value) {
            Ok(list.len() as i32)
        } else if let Some(dict) = DictRef::from_value(value) {
            Ok(dict.len() as i32)
        } else {
            Err(anyhow::anyhow!(
                "object of type '{}' has no len()",
                value.get_type()
            ))
        }
    }
}

// Helper functions for JSON conversion
fn json_to_starlark_value(heap: &Heap, json: serde_json::Value) -> anyhow::Result<Value<'_>> {
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
            let values: Result<Vec<Value>, _> = arr
                .into_iter()
                .map(|v| json_to_starlark_value(heap, v))
                .collect();
            Ok(heap.alloc(values?))
        }
        serde_json::Value::Object(obj) => {
            let mut items = Vec::new();
            for (k, v) in obj {
                let value_str = match json_to_starlark_value(heap, v) {
                    Ok(val) => val.to_string(),
                    Err(_) => "None".to_string(),
                };
                items.push(format!("{}: {}", k, value_str));
            }
            let dict_str = format!("{{{}}}", items.join(", "));
            Ok(heap.alloc(dict_str))
        }
    }
}

fn starlark_to_json_value(value: Value) -> anyhow::Result<serde_json::Value> {
    use starlark::values::{dict::DictRef, list::ListRef};

    if value.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Some(b) = value.unpack_bool() {
        Ok(serde_json::Value::Bool(b))
    } else if let Some(i) = value.unpack_i32() {
        Ok(serde_json::Value::Number(serde_json::Number::from(i)))
    } else if let Some(s) = value.unpack_str() {
        Ok(serde_json::Value::String(s.to_string()))
    } else if let Some(list) = ListRef::from_value(value) {
        let arr: Result<Vec<serde_json::Value>, _> =
            list.iter().map(starlark_to_json_value).collect();
        Ok(serde_json::Value::Array(arr?))
    } else if let Some(dict) = DictRef::from_value(value) {
        let mut obj = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = k.to_string();
            obj.insert(key, starlark_to_json_value(v)?);
        }
        Ok(serde_json::Value::Object(obj))
    } else {
        Ok(serde_json::Value::String(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starlark::environment::{GlobalsBuilder, Module};
    use starlark::eval::Evaluator;
    use starlark::syntax::{AstModule, Dialect};
    use starlark::values::dict::DictRef;

    // Helper to run a test and extract values immediately
    fn test_parse_csv_script(script: &str) -> Result<(usize, Vec<(String, String)>), String> {
        // Create globals with our functions
        let globals = GlobalsBuilder::new().with(global_functions).build();

        // Create module and evaluator
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Parse and execute the script
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended)
            .map_err(|e| format!("Parse error: {}", e))?;
        let result = eval
            .eval_module(ast, &globals)
            .map_err(|e| format!("Eval error: {}", e))?;

        // Extract the dict data immediately while the module is still alive
        let dict = DictRef::from_value(result).ok_or_else(|| "Result is not a dict".to_string())?;

        let len = dict.len();
        let mut entries = Vec::new();

        for (k, v) in dict.iter() {
            let key = k
                .unpack_str()
                .ok_or_else(|| "Key is not a string".to_string())?
                .to_string();
            let value = v
                .unpack_str()
                .ok_or_else(|| "Value is not a string".to_string())?
                .to_string();
            entries.push((key, value));
        }

        entries.sort(); // Sort for consistent testing
        Ok((len, entries))
    }

    #[test]
    fn test_parse_csv_auto_headers() {
        let script = r#"parse_csv("alice,25,engineer")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("col1".to_string(), "alice".to_string()),
                ("col2".to_string(), "25".to_string()),
                ("col3".to_string(), "engineer".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_with_headers() {
        let script = r#"parse_csv("alice,25,engineer", headers=["name", "age", "job"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("job".to_string(), "engineer".to_string()),
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_more_headers_than_fields() {
        let script = r#"parse_csv("alice,25", headers=["name", "age", "job"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("job".to_string(), "".to_string()), // Empty string
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_more_fields_than_headers() {
        let script = r#"parse_csv("alice,25,engineer,bonus", headers=["name", "age"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 2); // Only 2 headers, extra fields ignored
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_custom_delimiter() {
        let script = r#"parse_csv("alice|25|engineer", sep="|")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("col1".to_string(), "alice".to_string()),
                ("col2".to_string(), "25".to_string()),
                ("col3".to_string(), "engineer".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_empty_line() {
        let script = r#"parse_csv("")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 0); // Empty dict for empty line
        assert_eq!(entries, vec![]);
    }

    #[test]
    fn test_parse_csv_single_field() {
        let script = r#"parse_csv("alice")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 1);
        assert_eq!(entries, vec![("col1".to_string(), "alice".to_string()),]);
    }
}
