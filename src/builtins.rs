use std::cell::{Cell, RefCell};
use starlark::values::{Value, none::NoneType, list::ListRef, dict::DictRef};
use starlark::starlark_module;
use anyhow::Result;
use regex::Regex;
use std::collections::HashMap;
use std::sync::OnceLock;

// Thread-local storage for emit/skip/terminate functions
thread_local! {
    pub static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    pub static SKIP_FLAG: Cell<bool> = Cell::new(false);
    pub static TERMINATE_FLAG: Cell<bool> = Cell::new(false);
    pub static GLOBAL_VARS_REF: RefCell<Option<*const crate::GlobalVariables>> = RefCell::new(None);
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
pub fn global_functions() -> Result<()> {
    /// Emit an additional output line
    fn emit(text: String) -> anyhow::Result<NoneType> {
        EMIT_BUFFER.with(|buffer| {
            buffer.borrow_mut().push(text);
        });
        Ok(NoneType)
    }

    /// Skip outputting the current line
    fn skip() -> anyhow::Result<NoneType> {
        SKIP_FLAG.with(|flag| flag.set(true));
        Ok(NoneType)
    }

    /// Stop processing entirely
    fn terminate() -> anyhow::Result<NoneType> {
        TERMINATE_FLAG.with(|flag| flag.set(true));
        Ok(NoneType)
    }

    /// Get a global variable
    fn get_global(name: String, default: Option<Value>) -> anyhow::Result<Value> {
        GLOBAL_VARS_REF.with(|global_ref| {
            if let Some(globals_ptr) = *global_ref.borrow() {
                let globals = unsafe { &*globals_ptr };
                Ok(globals.get(&name, default))
            } else {
                Ok(default.unwrap_or(Value::new_none()))
            }
        })
    }

    /// Set a global variable
    fn set_global(name: String, value: Value) -> anyhow::Result<Value> {
        GLOBAL_VARS_REF.with(|global_ref| {
            if let Some(globals_ptr) = *global_ref.borrow() {
                let globals = unsafe { &*globals_ptr };
                globals.set(name, value.clone());
            }
            Ok(value)
        })
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
    fn file_name() -> anyhow::Result<Value> {
        LINE_CONTEXT.with(|ctx| {
            if let Some((_, ref file)) = *ctx.borrow() {
                if let Some(ref filename) = file {
                    Ok(Value::new(filename.clone()))
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
    fn regex_find_all(pattern: String, text: String) -> anyhow::Result<Value> {
        let regex = get_regex(&pattern)?;
        let matches: Vec<Value> = regex.find_iter(&text)
            .map(|m| Value::new(m.as_str().to_string()))
            .collect();
        Ok(Value::new(matches))
    }

    /// Parse JSON string
    fn parse_json(text: String) -> anyhow::Result<Value> {
        let json_value: serde_json::Value = serde_json::from_str(&text)?;
        json_to_starlark_value(json_value)
    }

    /// Convert value to JSON string
    fn to_json(value: Value) -> anyhow::Result<String> {
        let json_value = starlark_to_json_value(value)?;
        Ok(serde_json::to_string(&json_value)?)
    }

    /// Parse CSV line
    fn parse_csv(line: String, delimiter: Option<String>) -> anyhow::Result<Value> {
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');
        
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .from_reader(line.as_bytes());
        
        if let Some(record) = reader.records().next() {
            let record = record?;
            let fields: Vec<Value> = record.iter()
                .map(|field| Value::new(field.to_string()))
                .collect();
            Ok(Value::new(fields))
        } else {
            Ok(Value::new(Vec::<Value>::new()))
        }
    }

    /// Convert array to CSV line
    fn to_csv(values: Value, delimiter: Option<String>) -> anyhow::Result<String> {
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');
        
        let list = ListRef::from_value(&values)
            .ok_or_else(|| anyhow::anyhow!("Expected list for to_csv"))?;
        
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
    fn parse_kv(line: String, sep: Option<String>, delim: Option<String>) -> anyhow::Result<Value> {
        let separator = sep.unwrap_or_else(|| "=".to_string());
        let delimiter = delim.unwrap_or_else(|| " ".to_string());
        
        let mut map = std::collections::HashMap::new();
        
        for pair in line.split(&delimiter) {
            if let Some((key, value)) = pair.split_once(&separator) {
                map.insert(
                    Value::new(key.trim().to_string()),
                    Value::new(value.trim().to_string())
                );
            }
        }
        
        Ok(Value::new(map))
    }

    Ok(())
}

fn json_to_starlark_value(json: serde_json::Value) -> anyhow::Result<Value> {
    match json {
        serde_json::Value::Null => Ok(Value::new_none()),
        serde_json::Value::Bool(b) => Ok(Value::new(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::new(i as i32))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::new(f))
            } else {
                Ok(Value::new(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(Value::new(s)),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>, _> = arr.into_iter()
                .map(json_to_starlark_value)
                .collect();
            Ok(Value::new(values?))
        }
        serde_json::Value::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(Value::new(k), json_to_starlark_value(v)?);
            }
            Ok(Value::new(map))
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
    } else if let Some(f) = value.unpack_f64() {
        Ok(serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0))))
    } else if let Some(s) = value.unpack_str() {
        Ok(serde_json::Value::String(s.to_string()))
    } else if let Some(list) = ListRef::from_value(&value) {
        let arr: Result<Vec<serde_json::Value>, _> = list.iter()
            .map(starlark_to_json_value)
            .collect();
        Ok(serde_json::Value::Array(arr?))
    } else if let Some(dict) = DictRef::from_value(&value) {
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