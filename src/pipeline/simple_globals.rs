use crate::variables::GlobalVariables;
use starlark::starlark_module;
use starlark::values::{Heap, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

thread_local! {
    pub(crate) static SIMPLE_GLOBALS: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    pub(crate) static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    pub(crate) static SKIP_FLAG: Cell<bool> = Cell::new(false);
    pub(crate) static EXIT_FLAG: Cell<bool> = Cell::new(false);
    pub(crate) static EXIT_MESSAGE: RefCell<Option<String>> = RefCell::new(None);
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
        EXIT_FLAG.with(|flag| flag.set(true));
        EXIT_MESSAGE.with(|msg| {
            *msg.borrow_mut() = message;
        });
        Ok(starlark::values::none::NoneType)
    }

    fn print(message: String) -> anyhow::Result<starlark::values::none::NoneType> {
        // Print to stderr to avoid cluttering stdout
        eprintln!("{}", message);
        Ok(starlark::values::none::NoneType)
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