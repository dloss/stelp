use crate::variables::GlobalVariables;
use starlark::starlark_module;
use starlark::values::{Heap, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

thread_local! {
    pub(crate) static SIMPLE_GLOBALS: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    pub(crate) static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    pub(crate) static SKIP_FLAG: Cell<bool> = Cell::new(false);
    pub(crate) static TERMINATE_FLAG: Cell<bool> = Cell::new(false);
    pub(crate) static TERMINATE_MESSAGE: RefCell<Option<String>> = RefCell::new(None);
    pub(crate) static CURRENT_CONTEXT: RefCell<Option<(*const GlobalVariables, usize, Option<String>)>> = RefCell::new(None);
}

/// Transform st.function_name() calls to st_function_name() calls
pub fn preprocess_st_namespace(script: &str) -> String {
    // Simple regex replacement to transform st.function_name to st_function_name
    let re = regex::Regex::new(r"\bst\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    re.replace_all(script, "st_$1").to_string()
}

#[starlark_module]
pub(crate) fn simple_globals(builder: &mut starlark::environment::GlobalsBuilder) {
    // Top-level control flow functions (unchanged)
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
        TERMINATE_FLAG.with(|flag| flag.set(true));
        TERMINATE_MESSAGE.with(|msg| {
            *msg.borrow_mut() = message;
        });
        Ok(starlark::values::none::NoneType)
    }

    // All st.* functions now prefixed with st_
    fn st_get_global<'v>(
        heap: &'v Heap,
        name: String,
        default: Option<Value<'v>>,
    ) -> anyhow::Result<Value<'v>> {
        // Try to get from actual GlobalVariables if available
        let result = CURRENT_CONTEXT.with(|ctx| {
            if let Some((globals_ptr, _, _)) = *ctx.borrow() {
                let globals = unsafe { &*globals_ptr };
                Some(globals.get(heap, &name, default))
            } else {
                None
            }
        });

        if let Some(value) = result {
            Ok(value)
        } else {
            // Fallback to simple globals
            let result = SIMPLE_GLOBALS.with(|globals| globals.borrow().get(&name).cloned());

            if let Some(value_str) = result {
                // Try to parse as different types
                if let Ok(i) = value_str.parse::<i32>() {
                    Ok(heap.alloc(i))
                } else if value_str == "true" {
                    Ok(Value::new_bool(true))
                } else if value_str == "false" {
                    Ok(Value::new_bool(false))
                } else {
                    Ok(heap.alloc(value_str))
                }
            } else {
                Ok(default.unwrap_or_else(|| Value::new_none()))
            }
        }
    }

    fn st_set_global<'v>(name: String, value: Value<'v>) -> anyhow::Result<Value<'v>> {
        // Try to set in actual GlobalVariables if available
        let set_in_real_globals = CURRENT_CONTEXT.with(|ctx| {
            if let Some((globals_ptr, _, _)) = *ctx.borrow() {
                let globals = unsafe { &*globals_ptr };
                globals.set(name.clone(), value);
                true
            } else {
                false
            }
        });

        if !set_in_real_globals {
            // Fallback to simple globals
            let value_str = if value.is_none() {
                "None".to_string()
            } else {
                // Convert the value to string, removing quotes if it's a string
                let s = value.to_string();
                if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                    s[1..s.len() - 1].to_string()
                } else {
                    s
                }
            };
            SIMPLE_GLOBALS.with(|globals| {
                globals.borrow_mut().insert(name, value_str);
            });
        }

        Ok(value)
    }

    fn st_line_number() -> anyhow::Result<i32> {
        let line_num = CURRENT_CONTEXT.with(|ctx| {
            if let Some((_, line_number, _)) = *ctx.borrow() {
                line_number as i32
            } else {
                0
            }
        });
        Ok(line_num)
    }

    fn st_file_name<'v>(heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        let filename = CURRENT_CONTEXT.with(|ctx| {
            if let Some((_, _, ref filename)) = *ctx.borrow() {
                filename.clone()
            } else {
                None
            }
        });

        if let Some(name) = filename {
            Ok(heap.alloc(name))
        } else {
            Ok(Value::new_none())
        }
    }

    fn st_regex_match(pattern: String, text: String) -> anyhow::Result<bool> {
        match regex::Regex::new(&pattern) {
            Ok(regex) => Ok(regex.is_match(&text)),
            Err(_) => Ok(false), // Return false on regex error instead of propagating
        }
    }

    fn st_regex_replace(
        pattern: String,
        replacement: String,
        text: String,
    ) -> anyhow::Result<String> {
        let regex = regex::Regex::new(&pattern)?;
        Ok(regex.replace_all(&text, replacement.as_str()).into_owned())
    }

    fn st_regex_find_all<'v>(
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

    fn st_parse_json<'v>(heap: &'v Heap, text: String) -> anyhow::Result<Value<'v>> {
        let json_value: serde_json::Value = serde_json::from_str(&text)?;
        json_to_starlark_value(heap, json_value)
    }

    fn st_to_json(value: Value) -> anyhow::Result<String> {
        let json_value = starlark_to_json_value(value)?;
        Ok(serde_json::to_string(&json_value)?)
    }

    fn st_parse_csv<'v>(
        heap: &'v Heap,
        line: String,
        delimiter: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .from_reader(line.as_bytes());

        if let Some(record) = reader.records().next() {
            let record = record?;
            let fields: Vec<Value> = record
                .iter()
                .map(|field| heap.alloc(field.to_string()))
                .collect();
            Ok(heap.alloc(fields))
        } else {
            Ok(heap.alloc(Vec::<Value>::new()))
        }
    }

    fn st_to_csv(values: Value, delimiter: Option<String>) -> anyhow::Result<String> {
        use starlark::values::list::ListRef;

        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');

        let list = ListRef::from_value(values)
            .ok_or_else(|| anyhow::anyhow!("Expected list for to_csv"))?;

        let mut writer = csv::WriterBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Never) // Don't quote fields automatically
            .from_writer(Vec::new());

        let fields: Vec<String> = list
            .iter()
            .map(|v| {
                let s = v.to_string();
                // Remove quotes if they exist
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
        Ok(result.trim_end().to_string()) // Remove trailing newline
    }

    fn st_parse_kv<'v>(
        heap: &'v Heap,
        line: String,
        sep: Option<String>,
        delim: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let separator = sep.unwrap_or_else(|| "=".to_string());
        let delimiter = delim.unwrap_or_else(|| " ".to_string());

        // Create a simple dict representation as a string for now
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

    // Keep the standard Starlark built-ins
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

// Helper functions for JSON conversion (unchanged from original)
fn json_to_starlark_value<'v>(
    heap: &'v Heap,
    json: serde_json::Value,
) -> anyhow::Result<Value<'v>> {
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
            // Create a simple dict representation as a string for now
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
