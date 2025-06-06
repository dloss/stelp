// src/pipeline/glob_dict.rs
use crate::variables::GlobalVariables;
use starlark::collections::SmallMap;
use starlark::values::{dict::Dict, Heap, Value};

/// Create a glob dictionary that syncs with GlobalVariables
pub fn create_glob_dict<'v>(heap: &'v Heap, globals: &GlobalVariables) -> Value<'v> {
    let mut map = SmallMap::new();

    // Pre-populate with existing global variables
    for key in globals.keys() {
        let value = globals.get(heap, &key, None);
        let key_val = heap.alloc(key);
        map.insert_hashed(key_val.get_hashed().unwrap(), value);
    }

    let dict = Dict::new(map);
    heap.alloc(dict)
}

/// Update GlobalVariables from a glob dictionary after script execution
pub fn sync_glob_dict_to_globals<'v>(glob_dict: Value<'v>, globals: &GlobalVariables) {
    use starlark::values::dict::DictRef;

    if let Some(dict_ref) = DictRef::from_value(glob_dict) {
        // Clear existing globals first
        globals.clear();

        // Copy all dict entries to GlobalVariables
        for (k, v) in dict_ref.iter() {
            if let Some(key_str) = k.unpack_str() {
                globals.set(key_str.to_string(), v);
            }
        }
    }
}
