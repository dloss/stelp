// src/pipeline/processors.rs - Fixed version
use crate::context::{ProcessResult, RecordContext, RecordData};
use crate::pipeline::glob_dict::{create_glob_dict, sync_glob_dict_to_globals};
use crate::pipeline::global_functions::{
    global_functions, CURRENT_CONTEXT, EMIT_BUFFER, EXIT_FLAG, EXIT_MESSAGE, SKIP_FLAG,
};
use crate::pipeline::stream::RecordProcessor;
use crate::variables::GlobalVariables;
use crate::{CompilationError, ProcessingError};
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};

// Prelude code that provides helper functions like inc()
const PRELUDE_CODE: &str = include_str!("../prelude.star");

/// Starlark-based record processor with global namespace
pub struct StarlarkProcessor {
    globals: Globals,
    script_source: String,
    name: String,
}

// Define our own result type that doesn't have lifetime issues
#[derive(Debug)]
enum StarlarkResult {
    None,
    Text(String),
    List(Vec<String>),
    Structured(serde_json::Value), // NEW: Add structured data support
}

impl StarlarkProcessor {
    /// Create from script source
    pub fn from_script(name: &str, script: &str) -> Result<Self, CompilationError> {
        // Create globals with built-in functions
        let globals = GlobalsBuilder::standard().with(global_functions).build();

        // Validate syntax by parsing with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let _ast = AstModule::parse("script", script.to_string(), &dialect)?;

        Ok(StarlarkProcessor {
            globals,
            script_source: script.to_string(),
            name: name.to_string(),
        })
    }

    fn execute_with_context(
        &self,
        record: &RecordData,
        ctx: &RecordContext,
    ) -> Result<StarlarkResult, anyhow::Error> {
        // Set up context for global functions
        CURRENT_CONTEXT.with(|ctx_cell| {
            *ctx_cell.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Create fresh module for each record
        let module = Module::new();

        // Create glob dictionary using the existing function
        let glob_dict = create_glob_dict(module.heap(), ctx.global_vars);
        module.set("glob", glob_dict);

        // Inject meta variables directly as ALLUPPERCASE globals
        module.set("LINENUM", module.heap().alloc(ctx.line_number as i32));
        module.set("RECNUM", module.heap().alloc(ctx.record_count as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILENAME", module.heap().alloc(filename));
        } else {
            module.set("FILENAME", starlark::values::Value::new_none());
        }

        // Set record-specific variables based on type
        match record {
            RecordData::Text(text) => {
                module.set("line", module.heap().alloc(text.clone()));
                module.set("data", starlark::values::Value::new_none());
            }
            RecordData::Structured(data) => {
                module.set("line", starlark::values::Value::new_none());
                let starlark_data = json_to_starlark_value(module.heap(), data.clone())?;
                module.set("data", starlark_data);
            }
        }

        // Add constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));
        module.set("None", starlark::values::Value::new_none());

        // Parse and execute script with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };

        // Load and execute prelude to provide helper functions like inc()
        let prelude_ast = AstModule::parse("prelude", PRELUDE_CODE.to_string(), &dialect)
            .map_err(|e| anyhow::anyhow!("Prelude parse error: {}", e))?;
        let mut prelude_eval = Evaluator::new(&module);
        prelude_eval
            .eval_module(prelude_ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Prelude execution error: {}", e))?;
        let ast = AstModule::parse("script", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Script parse error: {}", e))?;

        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Script execution error: {}", e))?;

        // Sync glob dictionary back to global variables after execution
        if let Some(glob_value) = module.get("glob") {
            sync_glob_dict_to_globals(glob_value, ctx.global_vars);
        }

        // Convert the result to our owned type before returning
        let starlark_result = if result.is_none() {
            StarlarkResult::None
        } else {
            // NEW: Check if it's a dictionary first
            use starlark::values::dict::DictRef;
            if let Some(_dict) = DictRef::from_value(result) {
                // Convert Starlark dict to JSON
                match starlark_to_json_value(result) {
                    Ok(json_value) => StarlarkResult::Structured(json_value),
                    Err(_) => {
                        // Fallback to string representation
                        StarlarkResult::Text(result.to_string())
                    }
                }
            } else if let Ok(mut iterator) = result.iterate(module.heap()) {
                // Handle any iterable (including ranges)
                let mut strings = Vec::new();
                loop {
                    match iterator.next() {
                        Some(item) => {
                            let item_str = if item.is_none() {
                                String::new()
                            } else if let Some(s) = item.unpack_str() {
                                s.to_string()
                            } else {
                                let s = item.to_string();
                                if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                                    s[1..s.len() - 1].to_string()
                                } else {
                                    s
                                }
                            };
                            strings.push(item_str);
                        }
                        None => break,
                    }
                }
                StarlarkResult::List(strings)
            } else {
                // Single value
                let text = if let Some(s) = result.unpack_str() {
                    s.to_string()
                } else {
                    let s = result.to_string();
                    if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    }
                };
                StarlarkResult::Text(text)
            }
        };

        Ok(starlark_result)
    }

    pub fn process_standalone(&self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        // Clear thread-local state
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        EXIT_FLAG.with(|flag| flag.set(false));
        EXIT_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Execute script
        let result = match self.execute_with_context(record, ctx) {
            Ok(starlark_result) => {
                // Collect emitted lines
                let emissions: Vec<RecordData> = EMIT_BUFFER.with(|buffer| {
                    buffer
                        .borrow()
                        .iter()
                        .map(|s| RecordData::text(s.clone()))
                        .collect()
                });

                // Check control flow flags
                let skip_flag = SKIP_FLAG.with(|flag| flag.get());
                let exit_flag = EXIT_FLAG.with(|flag| flag.get());

                if skip_flag {
                    if emissions.is_empty() {
                        ProcessResult::Skip
                    } else {
                        ProcessResult::FanOut(emissions)
                    }
                } else if exit_flag {
                    let final_output = EXIT_MESSAGE
                        .with(|msg| msg.borrow().as_ref().map(|s| RecordData::text(s.clone())));
                    ProcessResult::Exit(final_output)
                } else {
                    // Handle different result types
                    match starlark_result {
                        StarlarkResult::List(strings) => {
                            // Convert list to string representation (no implicit fan-out)
                            let list_str = format!("[{}]", strings.join(", "));
                            let clean_result = RecordData::text(list_str);
                            match emissions.is_empty() {
                                true => ProcessResult::Transform(clean_result),
                                false => ProcessResult::TransformWithEmissions {
                                    primary: Some(clean_result),
                                    emissions,
                                },
                            }
                        }
                        StarlarkResult::None => {
                            // None means no output
                            if emissions.is_empty() {
                                ProcessResult::Skip
                            } else {
                                ProcessResult::FanOut(emissions)
                            }
                        }
                        StarlarkResult::Text(text) => {
                            let clean_result = RecordData::text(text);
                            match emissions.is_empty() {
                                true => ProcessResult::Transform(clean_result),
                                false => ProcessResult::TransformWithEmissions {
                                    primary: Some(clean_result),
                                    emissions,
                                },
                            }
                        }
                        // NEW: Handle structured data
                        StarlarkResult::Structured(json_value) => {
                            let clean_result = RecordData::structured(json_value);
                            match emissions.is_empty() {
                                true => ProcessResult::Transform(clean_result),
                                false => ProcessResult::TransformWithEmissions {
                                    primary: Some(clean_result),
                                    emissions,
                                },
                            }
                        }
                    }
                }
            }
            Err(starlark_error) => ProcessResult::Error(ProcessingError::ScriptError {
                step: self.name.clone(),
                line: ctx.line_number,
                source: starlark_error,
            }),
        };

        // Clear context
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = None;
        });

        result
    }
}

impl RecordProcessor for StarlarkProcessor {
    fn process(&mut self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        self.process_standalone(record, ctx)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Filter processor that uses Starlark expressions
pub struct FilterProcessor {
    globals: Globals,
    script_source: String,
    name: String,
}

impl FilterProcessor {
    pub fn from_script(name: &str, script: &str) -> Result<Self, CompilationError> {
        // Create globals with built-in functions
        let globals = GlobalsBuilder::standard().with(global_functions).build();

        // Validate syntax by parsing with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let _ast = AstModule::parse("filter", script.to_string(), &dialect)?;

        Ok(FilterProcessor {
            globals,
            script_source: script.to_string(),
            name: name.to_string(),
        })
    }

    // Keep the old from_expression method for backward compatibility
    pub fn from_expression(name: &str, expression: &str) -> Result<Self, CompilationError> {
        Self::from_script(name, expression)
    }

    fn filter_matches(&self, record: &RecordData, ctx: &RecordContext) -> Result<bool, anyhow::Error> {
        // Set up context for global functions
        CURRENT_CONTEXT.with(|ctx_cell| {
            *ctx_cell.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Create fresh module for each record
        let module = Module::new();

        // Create glob dictionary using the existing function
        let glob_dict = create_glob_dict(module.heap(), ctx.global_vars);
        module.set("glob", glob_dict);

        // Inject meta variables
        module.set("LINENUM", module.heap().alloc(ctx.line_number as i32));
        module.set("RECNUM", module.heap().alloc(ctx.record_count as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILENAME", module.heap().alloc(filename));
        } else {
            module.set("FILENAME", starlark::values::Value::new_none());
        }

        // Set record-specific variables
        match record {
            RecordData::Text(text) => {
                module.set("line", module.heap().alloc(text.clone()));
                module.set("data", starlark::values::Value::new_none());
            }
            RecordData::Structured(data) => {
                module.set("line", starlark::values::Value::new_none());
                let starlark_data = json_to_starlark_value(module.heap(), data.clone())?;
                module.set("data", starlark_data);
            }
        }

        // Add constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));
        module.set("None", starlark::values::Value::new_none());

        // Execute filter with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };

        // Load and execute prelude to provide helper functions like inc()
        let prelude_ast = AstModule::parse("prelude", PRELUDE_CODE.to_string(), &dialect)
            .map_err(|e| anyhow::anyhow!("Prelude parse error: {}", e))?;
        let mut prelude_eval = Evaluator::new(&module);
        prelude_eval
            .eval_module(prelude_ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Prelude execution error: {}", e))?;
        let ast = AstModule::parse("filter", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Filter parse error: {}", e))?;

        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Filter execution error: {}", e))?;

        // Sync glob dictionary back to global variables after execution
        if let Some(glob_value) = module.get("glob") {
            sync_glob_dict_to_globals(glob_value, ctx.global_vars);
        }

        // Check for control flow
        let should_exit = EXIT_FLAG.with(|flag| flag.get());
        if should_exit {
            let msg = EXIT_MESSAGE.with(|msg| msg.borrow().clone());
            return Err(anyhow::anyhow!("Filter exit: {}", msg.unwrap_or_default()));
        }

        // Convert result to boolean
        if result.is_none() {
            Ok(false)
        } else if let Some(b) = result.unpack_bool() {
            Ok(b)
        } else {
            // Truthy evaluation for non-boolean values
            Ok(!result.is_none()
                && result != starlark::values::Value::new_bool(false)
                && !(result.unpack_str().is_some_and(|s| s.is_empty()))
                && (result.unpack_i32() != Some(0)))
        }
    }
}

impl RecordProcessor for FilterProcessor {
    fn process(&mut self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        let result = match self.filter_matches(record, ctx) {
            Ok(filter_matches) => {
                if filter_matches {
                    ProcessResult::Transform(record.clone())
                } else {
                    ProcessResult::Skip
                }
            }
            Err(error) => ProcessResult::Error(ProcessingError::ScriptError {
                step: self.name.clone(),
                line: ctx.line_number,
                source: error,
            }),
        };

        // Clear context
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = None;
        });

        result
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Helper function for JSON conversion
fn json_to_starlark_value(
    heap: &starlark::values::Heap,
    json: serde_json::Value,
) -> anyhow::Result<starlark::values::Value<'_>> {
    use starlark::values::Value;

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
            let values: Result<Vec<Value>, anyhow::Error> = arr
                .into_iter()
                .map(|v| json_to_starlark_value(heap, v))
                .collect();
            Ok(heap.alloc(values?))
        }
        serde_json::Value::Object(obj) => {
            use starlark::collections::SmallMap;
            use starlark::values::dict::Dict;

            let mut content = SmallMap::new();
            for (k, v) in obj {
                let key = heap.alloc(k);
                let value = json_to_starlark_value(heap, v)?;
                content.insert_hashed(
                    key.get_hashed().map_err(|e| anyhow::anyhow!("{}", e))?,
                    value,
                );
            }
            let dict = Dict::new(content);
            Ok(heap.alloc(dict))
        }
    }
}

fn starlark_to_json_value(value: starlark::values::Value) -> anyhow::Result<serde_json::Value> {
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
            // FIX: Use unpack_str() to get the actual string value without quotes
            let key = if let Some(s) = k.unpack_str() {
                s.to_string()
            } else {
                k.to_string()
            };
            obj.insert(key, starlark_to_json_value(v)?);
        }
        Ok(serde_json::Value::Object(obj))
    } else {
        Ok(serde_json::Value::String(value.to_string()))
    }
}