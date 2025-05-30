use crate::variables::GlobalVariables;
use starlark::starlark_module;
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

#[starlark_module]
pub(crate) fn simple_globals(builder: &mut starlark::environment::GlobalsBuilder) {
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

    fn terminate(message: Option<String>) -> anyhow::Result<starlark::values::none::NoneType> {
        TERMINATE_FLAG.with(|flag| flag.set(true));
        TERMINATE_MESSAGE.with(|msg| {
            *msg.borrow_mut() = message;
        });
        Ok(starlark::values::none::NoneType)
    }

    fn get_global<'v>(
        heap: &'v starlark::values::Heap,
        name: String,
        default: Option<starlark::values::Value<'v>>,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
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
                    Ok(starlark::values::Value::new_bool(true))
                } else if value_str == "false" {
                    Ok(starlark::values::Value::new_bool(false))
                } else {
                    Ok(heap.alloc(value_str))
                }
            } else {
                Ok(default.unwrap_or_else(|| starlark::values::Value::new_none()))
            }
        }
    }

    fn set_global<'v>(
        name: String,
        value: starlark::values::Value<'v>,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
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

    fn line_number() -> anyhow::Result<i32> {
        let line_num = CURRENT_CONTEXT.with(|ctx| {
            if let Some((_, line_number, _)) = *ctx.borrow() {
                line_number as i32
            } else {
                0
            }
        });
        Ok(line_num)
    }

    fn file_name<'v>(
        heap: &'v starlark::values::Heap,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
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
            Ok(starlark::values::Value::new_none())
        }
    }

    fn regex_match(pattern: String, text: String) -> anyhow::Result<bool> {
        match regex::Regex::new(&pattern) {
            Ok(regex) => Ok(regex.is_match(&text)),
            Err(_) => Ok(false), // Return false on regex error instead of propagating
        }
    }

    fn regex_replace(pattern: String, replacement: String, text: String) -> anyhow::Result<String> {
        let regex = regex::Regex::new(&pattern)?;
        Ok(regex.replace_all(&text, replacement.as_str()).into_owned())
    }

    fn str<'v>(
        heap: &'v starlark::values::Heap,
        value: starlark::values::Value<'v>,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
        Ok(heap.alloc(value.to_string()))
    }

    fn len<'v>(value: starlark::values::Value<'v>) -> anyhow::Result<i32> {
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
