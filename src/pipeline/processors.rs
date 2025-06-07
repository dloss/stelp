// src/pipeline/processors.rs - Updated to include glob dictionary

use crate::error::{CompilationError, ProcessingError};
use crate::pipeline::context::{ProcessResult, RecordContext, RecordData};
use crate::pipeline::glob_dict::{create_glob_dict, sync_glob_dict_to_globals};
use crate::pipeline::global_functions::{
    global_functions, CURRENT_CONTEXT, EMIT_BUFFER, EXIT_FLAG, EXIT_MESSAGE, SKIP_FLAG,
};
use crate::pipeline::stream::RecordProcessor;
use crate::variables::GlobalVariables;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};

/// Starlark-based record processor with global namespace
pub struct StarlarkProcessor {
    globals: Globals,
    script_source: String,
    name: String,
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

    /// Execute script with fresh module per record
    fn execute_with_context(
        &self,
        record: &RecordData,
        ctx: &RecordContext,
    ) -> Result<String, anyhow::Error> {
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

        // Parse and execute script
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let ast = AstModule::parse("script", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Script parse error: {}", e))?;

        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Script execution error: {}", e))?;

        // Sync the glob dictionary back to global variables using existing function
        if let Some(updated_glob) = module.get("glob") {
            sync_glob_dict_to_globals(updated_glob, ctx.global_vars);
        }

        Ok(result.to_string())
    }

    pub fn process_standalone(&self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        // Clear thread-local state
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        EXIT_FLAG.with(|flag| flag.set(false));
        EXIT_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Execute script
        let result = match self.execute_with_context(record, ctx) {
            Ok(result_str) => {
                // Collect emitted lines
                let emissions: Vec<RecordData> = EMIT_BUFFER.with(|buffer| {
                    buffer
                        .borrow()
                        .iter()
                        .map(|s| RecordData::text(s.clone()))
                        .collect()
                });

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
                    // Normal processing
                    let clean_result = if result_str == "None" {
                        record.clone()
                    } else {
                        let processed_str = if result_str.starts_with('"')
                            && result_str.ends_with('"')
                            && result_str.len() > 1
                        {
                            result_str[1..result_str.len() - 1].to_string()
                        } else {
                            result_str
                        };
                        RecordData::text(processed_str)
                    };

                    match emissions.is_empty() {
                        true => ProcessResult::Transform(clean_result),
                        false => ProcessResult::TransformWithEmissions {
                            primary: Some(clean_result),
                            emissions,
                        },
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

/// Simple filter processor
pub struct FilterProcessor {
    globals: Globals,
    script_source: String,
    name: String,
}

impl FilterProcessor {
    /// Create from filter expression
    pub fn from_expression(name: &str, expression: &str) -> Result<Self, CompilationError> {
        let globals = GlobalsBuilder::standard().with(global_functions).build();

        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let _ast = AstModule::parse("filter", expression.to_string(), &dialect)?;

        Ok(FilterProcessor {
            globals,
            script_source: expression.to_string(),
            name: name.to_string(),
        })
    }

    fn filter_matches(
        &self,
        record: &RecordData,
        ctx: &RecordContext,
    ) -> Result<bool, anyhow::Error> {
        // Set up context
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Clear thread-local state
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        EXIT_FLAG.with(|flag| flag.set(false));
        EXIT_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Create fresh module
        let module = Module::new();

        // ADD: Create glob dictionary - this was missing!
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

        // Execute filter
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let ast = AstModule::parse("filter", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Filter parse error: {}", e))?;

        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Filter execution error: {}", e))?;

        // ADD: Sync glob dictionary back to global variables after execution
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
            let mut items = Vec::new();
            for (k, v) in obj {
                let value_repr = match json_to_starlark_value(heap, v) {
                    Ok(val) => {
                        if val.is_none() {
                            "None".to_string()
                        } else if let Some(s) = val.unpack_str() {
                            format!("\"{}\"", s)
                        } else {
                            val.to_string()
                        }
                    }
                    Err(_) => "None".to_string(),
                };
                items.push(format!("\"{}\": {}", k, value_repr));
            }
            let dict_str = format!("{{{}}}", items.join(", "));
            Ok(heap.alloc(dict_str))
        }
    }
}
