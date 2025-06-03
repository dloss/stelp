// src/pipeline/processors.rs
use crate::error::{CompilationError, ProcessingError};
use crate::pipeline::context::{ProcessResult, RecordContext, RecordData};
use crate::pipeline::simple_globals::{
    preprocess_st_namespace, simple_globals, CURRENT_CONTEXT, EMIT_BUFFER, SKIP_FLAG,
    EXIT_FLAG, EXIT_MESSAGE,
};
use crate::pipeline::stream::RecordProcessor;
use crate::variables::GlobalVariables;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};

/// Starlark-based record processor (adapted from LineProcessor)
pub struct StarlarkProcessor {
    globals: Globals,
    script_source: String,
    name: String,
}

impl StarlarkProcessor {
    /// Create from script source
    pub fn from_script(name: &str, script: &str) -> Result<Self, CompilationError> {
        // Create globals with built-ins
        let globals = GlobalsBuilder::new().with(simple_globals).build();

        // Preprocess st.* calls to st_* function names
        let script_source = preprocess_st_namespace(script);

        // Validate syntax by parsing with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let _ast = AstModule::parse("script", script_source.clone(), &dialect)?;

        Ok(StarlarkProcessor {
            globals,
            script_source,
            name: name.to_string(),
        })
    }

    /// Execute script with fresh module per record
    fn execute_with_context(
        &self,
        text: &str,
        ctx: &RecordContext,
    ) -> Result<String, anyhow::Error> {
        // Set up context for global functions (keep same signature as original)
        CURRENT_CONTEXT.with(|ctx_cell| {
            *ctx_cell.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Create fresh module for each record (local variables)
        let module = Module::new();

        // Set built-in variables (same as before)
        module.set("line", module.heap().alloc(text.to_string()));
        module.set("LINE_NUMBER", module.heap().alloc(ctx.line_number as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILE_NAME", module.heap().alloc(filename.to_string()));
        }

        // Add True/False/None constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));
        module.set("None", starlark::values::Value::new_none());

        // Parse and execute script with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let ast = AstModule::parse("script", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Script parse error: {}", e))?;

        // Execute AST with globals available
        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Script execution error: {}", e))?;

        // Convert result to string immediately to avoid lifetime issues
        Ok(result.to_string())
    }

    pub fn process_standalone(&self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        // For Commit 1: only handle text records, structured records are pass-through
        let text = match record {
            RecordData::Text(text) => text,
            RecordData::Structured(_) => {
                // Pass structured records through unchanged for now
                return ProcessResult::Transform(record.clone());
            }
        };

        // Clear emit buffer and flags
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        EXIT_FLAG.with(|flag| flag.set(false));
        EXIT_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Execute script (same logic as before)
        let result = match self.execute_with_context(text, ctx) {
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

                // Check for special control values
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
                    // Normal processing - FIXED: Handle the script result properly
                    let clean_result = if result_str == "None" {
                        // If script returns None, pass through original line unchanged
                        text.to_string()
                    } else {
                        // Remove surrounding quotes if they exist
                        if result_str.starts_with('"')
                            && result_str.ends_with('"')
                            && result_str.len() > 1
                        {
                            result_str[1..result_str.len() - 1].to_string()
                        } else {
                            result_str
                        }
                    };

                    let output_record = RecordData::text(clean_result);

                    match emissions.is_empty() {
                        true => ProcessResult::Transform(output_record),
                        false => ProcessResult::TransformWithEmissions {
                            primary: Some(output_record),
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

        // Clear context to avoid dangling pointers
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

/// Simple filter processor that only keeps records matching a boolean expression
pub struct FilterProcessor {
    globals: Globals,
    pub script_source: String,
    name: String,
}

impl FilterProcessor {
    /// Create from filter expression
    pub fn from_expression(name: &str, expression: &str) -> Result<Self, CompilationError> {
        // Create globals with built-ins
        let globals = GlobalsBuilder::new().with(simple_globals).build();

        // Preprocess st.* calls to st_* function names
        let script = preprocess_st_namespace(expression);

        // Validate syntax by parsing with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let _ast = AstModule::parse("filter", script.clone(), &dialect)?;

        Ok(FilterProcessor {
            globals,
            script_source: script,
            name: name.to_string(),
        })
    }

    /// Execute filter expression with context
    fn filter_matches(&self, text: &str, ctx: &RecordContext) -> Result<bool, anyhow::Error> {
        // Set up context for global functions (same signature as StarlarkProcessor)
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

        // Create fresh module for each record
        let module = Module::new();

        // Set built-in variables (same as StarlarkProcessor)
        module.set("line", module.heap().alloc(text.to_string()));
        module.set("LINE_NUMBER", module.heap().alloc(ctx.line_number as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILE_NAME", module.heap().alloc(filename.to_string()));
        }

        // Add True/False/None constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));
        module.set("None", starlark::values::Value::new_none());

        // Parse and execute script with f-strings enabled
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

        // Convert result to boolean
        Ok(result.to_bool())
    }
}

impl RecordProcessor for FilterProcessor {
    fn process(&mut self, record: &RecordData, ctx: &RecordContext) -> ProcessResult {
        // For Commit 1: only handle text records, structured records are pass-through
        let text = match record {
            RecordData::Text(text) => text,
            RecordData::Structured(_) => {
                // Pass structured records through unchanged for now
                return ProcessResult::Transform(record.clone());
            }
        };

        let result = match self.filter_matches(text, ctx) {
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

        // Clear context to avoid dangling pointers
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = None;
        });

        result
    }

    fn name(&self) -> &str {
        &self.name
    }
}
