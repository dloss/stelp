use anyhow::Result;
use regex::Regex;
use starlark::starlark_module;
use starlark::values::{dict::DictRef, list::ListRef, Heap, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::OnceLock;
use crate::variables::GlobalVariables;

// Thread-local storage for emit/skip/exit functions
thread_local! {
    pub static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    pub static SKIP_FLAG: Cell<bool> = Cell::new(false);
    pub static EXIT_FLAG: Cell<bool> = Cell::new(false);
    pub static GLOBAL_VARS_REF: RefCell<Option<*const GlobalVariables>> = RefCell::new(None);
    pub static LINE_CONTEXT: RefCell<Option<(usize, Option<String>)>> = RefCell::new(None);
}

// Regex cache
static REGEX_CACHE: OnceLock<std::sync::Mutex<HashMap<String, Regex>>> = OnceLock::new();

fn get_regex(pattern: &str) -> Result<Regex> {
    let cache = REGEX_CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
    let mut cache_guard = cache.lock().unwrap();

    if let Some(regex) = cache_guard.get(pattern) {
        Ok(regex.clone())
    } else {
        let regex = Regex::new(pattern)?;
        cache_guard.insert(pattern.to_string(), regex.clone());
        Ok(regex)
    }
}

#[starlark_module]
pub fn global_functions(builder: &mut starlark::environment::GlobalsBuilder) {
    /// Emit an additional output line
    fn emit(text: String) -> anyhow::Result<starlark::values::none::NoneType> {
        EMIT_BUFFER.with(|buffer| {
            buffer.borrow_mut().push(text);
        });
        Ok(starlark::values::none::NoneType)
    }

    /// Skip outputting the current line
    fn skip() -> anyhow::Result<starlark::values::none::NoneType> {
        SKIP_FLAG.with(|flag| flag.set(true));
        Ok(starlark::values::none::NoneType)
    }

    /// Stop processing entirely
    fn exit<'v>(heap: &'v Heap, message: Option<String>) -> anyhow::Result<Value<'v>> {
        EXIT_FLAG.with(|flag| flag.set(true));
        if let Some(msg) = message {
            Ok(heap.alloc(msg))
        } else {
            Ok(Value::new_none())
        }
    }

    /// Get current line number
    fn line_number() -> anyhow::Result<i32> {
        LINE_CONTEXT.with(|ctx| {
            if let Some((line_num, _)) = *ctx.borrow() {
                Ok(line_num as i32)
            } else {
                Ok(0)
            }
        })
    }

    /// Get current file name
    fn file_name<'v>(heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        LINE_CONTEXT.with(|ctx| {
            if let Some((_, ref file)) = *ctx.borrow() {
                if let Some(ref filename) = file {
                    Ok(heap.alloc(filename.clone()))
                } else {
                    Ok(Value::new_none())
                }
            } else {
                Ok(Value::new_none())
            }
        })
    }

    /// Check if text matches a regex pattern
    fn regex_match(pattern: String, text: String) -> anyhow::Result<bool> {
        let regex = get_regex(&pattern)?;
        Ok(regex.is_match(&text))
    }

    /// Replace text using regex
    fn regex_replace(pattern: String, replacement: String, text: String) -> anyhow::Result<String> {
        let regex = get_regex(&pattern)?;
        Ok(regex.replace_all(&text, replacement.as_str()).into_owned())
    }

    /// Find all regex matches
    fn regex_find_all<'v>(
        heap: &'v Heap,
        pattern: String,
        text: String,
    ) -> anyhow::Result<Value<'v>> {
        let regex = get_regex(&pattern)?;
        let matches: Vec<Value> = regex
            .find_iter(&text)
            .map(|m| heap.alloc(m.as_str().to_string()))
            .collect();
        Ok(heap.alloc(matches))
    }

    /// Parse JSON string
    fn parse_json<'v>(heap: &'v Heap, text: String) -> anyhow::Result<Value<'v>> {
        let json_value: serde_json::Value = serde_json::from_str(&text)?;
        json_to_starlark_value(heap, json_value)
    }

    /// Convert value to JSON string
    fn dump_json(value: Value) -> anyhow::Result<String> {
        let json_value = starlark_to_json_value(value)?;
        Ok(serde_json::to_string(&json_value)?)
    }

    /// Parse CSV line
    fn parse_csv<'v>(
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

    /// Convert array to CSV line
    fn dump_csv(values: Value, delimiter: Option<String>) -> anyhow::Result<String> {
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');

        let list = ListRef::from_value(values)
            .ok_or_else(|| anyhow::anyhow!("Expected list for dump_csv"))?;

        let mut writer = csv::WriterBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .from_writer(Vec::new());

        let fields: Vec<String> = list.iter().map(|v| v.to_string()).collect();
        writer.write_record(&fields)?;

        let data = writer.into_inner()?;
        let result = String::from_utf8(data)?;
        Ok(result.trim_end().to_string()) // Remove trailing newline
    }

    /// Parse key-value pairs
    fn parse_kv<'v>(
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
}

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