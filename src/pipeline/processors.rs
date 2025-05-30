use crate::error::{CompilationError, ProcessingError};
use crate::pipeline::context::{LineContext, ProcessResult};
use crate::pipeline::simple_globals::{
    simple_globals, CURRENT_CONTEXT, EMIT_BUFFER, SKIP_FLAG, TERMINATE_FLAG, TERMINATE_MESSAGE,
};
use crate::pipeline::stream::LineProcessor;
use crate::variables::GlobalVariables;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use std::borrow::Cow;

/// Starlark-based line processor
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

    /// Execute script with fresh module per line
    fn execute_with_context(&self, line: &str, ctx: &LineContext) -> Result<String, anyhow::Error> {
        // Set up context for global functions
        CURRENT_CONTEXT.with(|ctx_cell| {
            *ctx_cell.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Create fresh module for each line (local variables)
        let module = Module::new();

        // Set built-in variables
        module.set("line", module.heap().alloc(line.to_string()));
        module.set("LINE_NUMBER", module.heap().alloc(ctx.line_number as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILE_NAME", module.heap().alloc(filename.to_string()));
        }

        // Add True/False constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));

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

    pub fn process_standalone(&self, line: &str, ctx: &LineContext) -> ProcessResult {
        // Clear emit buffer and flags
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        TERMINATE_FLAG.with(|flag| flag.set(false));
        TERMINATE_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Execute script
        let result = match self.execute_with_context(line, ctx) {
            Ok(result_str) => {
                // Collect emitted lines
                let emissions: Vec<String> = EMIT_BUFFER.with(|buffer| buffer.borrow().clone());

                // Check for special control values
                if SKIP_FLAG.with(|flag| flag.get()) {
                    if emissions.is_empty() {
                        ProcessResult::Skip
                    } else {
                        ProcessResult::MultipleOutputs(emissions)
                    }
                } else if TERMINATE_FLAG.with(|flag| flag.get()) {
                    let final_output = TERMINATE_MESSAGE.with(|msg| {
                        if let Some(message) = msg.borrow().clone() {
                            Some(Cow::Owned(message))
                        } else {
                            None
                        }
                    });
                    ProcessResult::Terminate(final_output)
                } else {
                    // Normal processing
                    let is_none = result_str == "None" || result_str.is_empty();
                    let clean_result = if is_none {
                        String::new()
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

                    match (is_none || clean_result.is_empty(), emissions.is_empty()) {
                        (true, true) => ProcessResult::Transform(Cow::Owned(line.to_string())), // No change
                        (true, false) => ProcessResult::MultipleOutputs(emissions),
                        (false, true) => ProcessResult::Transform(Cow::Owned(clean_result)),
                        (false, false) => ProcessResult::TransformWithEmissions {
                            primary: Some(Cow::Owned(clean_result)),
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

impl LineProcessor for StarlarkProcessor {
    fn process(&mut self, line: &str, ctx: &LineContext) -> ProcessResult {
        self.process_standalone(line, ctx)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Simple filter processor that skips lines based on a boolean expression
pub struct FilterProcessor {
    globals: Globals,
    pub script_source: String, // Make this public for debugging
    name: String,
}

impl FilterProcessor {
    /// Create from filter expression
    pub fn from_expression(name: &str, expression: &str) -> Result<Self, CompilationError> {
        // Create globals with built-ins - use same globals as StarlarkProcessor
        let globals = GlobalsBuilder::new().with(simple_globals).build();

        // Use the expression directly without wrapping in bool()
        // Starlark will automatically convert the result to boolean when needed
        let script = expression.to_string();

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
    fn should_filter(&self, line: &str, ctx: &LineContext) -> Result<bool, anyhow::Error> {
        // Set up context for global functions
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = Some((
                ctx.global_vars as *const GlobalVariables,
                ctx.line_number,
                ctx.file_name.map(|s| s.to_string()),
            ));
        });

        // Clear thread-local state first (same as StarlarkProcessor)
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        TERMINATE_FLAG.with(|flag| flag.set(false));
        TERMINATE_MESSAGE.with(|msg| *msg.borrow_mut() = None);

        // Create fresh module for each line
        let module = Module::new();

        // Set built-in variables (same as StarlarkProcessor)
        module.set("line", module.heap().alloc(line.to_string()));
        module.set("LINE_NUMBER", module.heap().alloc(ctx.line_number as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILE_NAME", module.heap().alloc(filename.to_string()));
        }

        // Add True/False constants
        module.set("True", starlark::values::Value::new_bool(true));
        module.set("False", starlark::values::Value::new_bool(false));

        // Parse and execute script with f-strings enabled
        let dialect = Dialect {
            enable_f_strings: true,
            ..Dialect::Extended
        };
        let ast = AstModule::parse("filter", self.script_source.clone(), &dialect)
            .map_err(|e| anyhow::anyhow!("Filter parse error: {}", e))?;

        // Execute AST with globals available (same as StarlarkProcessor)
        let mut eval = Evaluator::new(&module);
        let result = eval
            .eval_module(ast, &self.globals)
            .map_err(|e| anyhow::anyhow!("Filter execution error: {}", e))?;

        // Convert result to boolean
        Ok(result.to_bool())
    }
}

impl LineProcessor for FilterProcessor {
    fn process(&mut self, line: &str, ctx: &LineContext) -> ProcessResult {
        let result = match self.should_filter(line, ctx) {
            Ok(should_filter) => {
                if should_filter {
                    ProcessResult::Skip
                } else {
                    ProcessResult::Transform(Cow::Owned(line.to_string()))
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
