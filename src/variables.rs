// src/variables.rs
use starlark::values::{Heap, Value};
use std::cell::RefCell;
use std::collections::HashMap;

/// Global variables that persist across lines
pub struct GlobalVariables {
    store: RefCell<HashMap<String, String>>, // Store as JSON strings to avoid lifetime issues
}

impl GlobalVariables {
    pub fn new() -> Self {
        GlobalVariables {
            store: RefCell::new(HashMap::new()),
        }
    }

    pub fn get<'v>(&self, heap: &'v Heap, name: &str, default: Option<Value<'v>>) -> Value<'v> {
        if let Some(json_str) = self.store.borrow().get(name) {
            // Try to deserialize from JSON
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let Ok(starlark_value) = json_to_starlark_value(heap, json_value) {
                    return starlark_value;
                }
            }
        }
        default.unwrap_or(Value::new_none())
    }

    pub fn set(&self, name: String, value: Value<'_>) {
        // Convert to JSON for storage
        if let Ok(json_value) = starlark_to_json_value(value) {
            if let Ok(json_str) = serde_json::to_string(&json_value) {
                self.store.borrow_mut().insert(name, json_str);
            }
        }
    }

    pub fn clear(&self) {
        self.store.borrow_mut().clear();
    }

    // Additional methods needed for glob dictionary
    pub fn contains(&self, name: &str) -> bool {
        self.store.borrow().contains_key(name)
    }

    pub fn len(&self) -> usize {
        self.store.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.borrow().is_empty()
    }

    pub fn keys(&self) -> Vec<String> {
        self.store.borrow().keys().cloned().collect()
    }

    pub fn remove(&self, name: &str) -> bool {
        self.store.borrow_mut().remove(name).is_some()
    }

    pub fn get_raw(&self, name: &str) -> Option<String> {
        self.store.borrow().get(name).cloned()
    }

    pub fn increment_counter(&self, name: &str) -> i32 {
        let current = self
            .get_raw(name)
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        let new_value = current + 1;
        let json_str = serde_json::to_string(&serde_json::Value::Number(serde_json::Number::from(
            new_value,
        )))
        .unwrap_or_else(|_| "1".to_string());
        self.store.borrow_mut().insert(name.to_string(), json_str);
        new_value
    }
}

impl Default for GlobalVariables {
    fn default() -> Self {
        Self::new()
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
            // For now, just create a simple dict representation as a string
            // This avoids the complex Starlark dict API issues
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
