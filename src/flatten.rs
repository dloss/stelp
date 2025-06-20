//! Shared data flattening utilities for output formats that require flat structures

use serde_json::{Map, Value};

/// Flatten nested JSON data into a flat structure with dot-notation keys
/// 
/// Examples:
/// - `{"user": {"name": "Alice"}}` → `{"user.name": "Alice"}`
/// - `{"items": ["a", "b"]}` → `{"items.0": "a", "items.1": "b"}`
/// - `{"users": [{"name": "Alice"}]}` → `{"users.0.name": "Alice"}`
pub fn flatten_data(data: &Value) -> Value {
    let mut result = Map::new();
    flatten_recursive(data, String::new(), &mut result);
    Value::Object(result)
}

/// Recursively flatten a JSON value into the result map
fn flatten_recursive(value: &Value, prefix: String, result: &mut Map<String, Value>) {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                flatten_recursive(val, new_key, result);
            }
        }
        Value::Array(arr) => {
            for (index, val) in arr.iter().enumerate() {
                let new_key = if prefix.is_empty() {
                    index.to_string()
                } else {
                    format!("{}.{}", prefix, index)
                };
                flatten_recursive(val, new_key, result);
            }
        }
        _ => {
            // Primitive value - insert directly
            result.insert(prefix, value.clone());
        }
    }
}

/// Check if a Value contains nested structures that need flattening
pub fn has_nested_data(value: &Value) -> bool {
    match value {
        Value::Object(obj) => {
            obj.values().any(|v| matches!(v, Value::Object(_) | Value::Array(_)))
        }
        Value::Array(arr) => {
            arr.iter().any(|v| matches!(v, Value::Object(_) | Value::Array(_)))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flatten_simple_object() {
        let data = json!({"name": "Alice", "age": 30});
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["name"], "Alice");
        assert_eq!(flattened["age"], 30);
    }

    #[test]
    fn test_flatten_nested_object() {
        let data = json!({
            "user": {
                "name": "Alice",
                "profile": {
                    "age": 30,
                    "city": "NYC"
                }
            }
        });
        
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["user.name"], "Alice");
        assert_eq!(flattened["user.profile.age"], 30);
        assert_eq!(flattened["user.profile.city"], "NYC");
    }

    #[test]
    fn test_flatten_array() {
        let data = json!({"items": ["apple", "banana", "cherry"]});
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["items.0"], "apple");
        assert_eq!(flattened["items.1"], "banana");
        assert_eq!(flattened["items.2"], "cherry");
    }

    #[test]
    fn test_flatten_array_of_objects() {
        let data = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ]
        });
        
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["users.0.name"], "Alice");
        assert_eq!(flattened["users.0.age"], 30);
        assert_eq!(flattened["users.1.name"], "Bob");
        assert_eq!(flattened["users.1.age"], 25);
    }

    #[test]
    fn test_flatten_mixed_nesting() {
        let data = json!({
            "id": 123,
            "user": {"name": "Alice"},
            "tags": ["admin", "user"],
            "settings": {
                "notifications": {
                    "email": true,
                    "sms": false
                }
            }
        });
        
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["id"], 123);
        assert_eq!(flattened["user.name"], "Alice");
        assert_eq!(flattened["tags.0"], "admin");
        assert_eq!(flattened["tags.1"], "user");
        assert_eq!(flattened["settings.notifications.email"], true);
        assert_eq!(flattened["settings.notifications.sms"], false);
    }

    #[test]
    fn test_has_nested_data() {
        assert!(!has_nested_data(&json!({"name": "Alice", "age": 30})));
        assert!(has_nested_data(&json!({"user": {"name": "Alice"}})));
        assert!(has_nested_data(&json!({"items": ["a", "b"]})));
        assert!(!has_nested_data(&json!("simple string")));
        assert!(!has_nested_data(&json!(42)));
    }

    #[test]
    fn test_flatten_empty_structures() {
        let empty_obj = json!({});
        let flattened = flatten_data(&empty_obj);
        assert!(flattened.as_object().unwrap().is_empty());

        let empty_array = json!([]);
        let flattened = flatten_data(&empty_array);
        assert!(flattened.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_flatten_null_values() {
        let data = json!({
            "user": {
                "name": "Alice",
                "email": null
            }
        });
        
        let flattened = flatten_data(&data);
        
        assert_eq!(flattened["user.name"], "Alice");
        assert_eq!(flattened["user.email"], Value::Null);
    }
}