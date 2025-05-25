use std::cell::RefCell;
use std::collections::HashMap;
use starlark::values::Value;

/// Global variables that persist across lines
pub struct GlobalVariables {
    store: RefCell<HashMap<String, Value>>,
}

impl GlobalVariables {
    pub fn new() -> Self {
        GlobalVariables {
            store: RefCell::new(HashMap::new()),
        }
    }
    
    pub fn get(&self, name: &str, default: Option<Value>) -> Value {
        self.store.borrow().get(name).cloned()
            .unwrap_or(default.unwrap_or(Value::new_none()))
    }
    
    pub fn set(&self, name: String, value: Value) {
        self.store.borrow_mut().insert(name, value);
    }
    
    pub fn clear(&self) {
        self.store.borrow_mut().clear();
    }
}

impl Default for GlobalVariables {
    fn default() -> Self {
        Self::new()
    }
}