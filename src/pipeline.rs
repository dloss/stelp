use crate::error::*;
use crate::variables::GlobalVariables;
use starlark::environment::{Globals, GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use std::borrow::Cow;
use std::io::{BufRead, Write};
use std::time::{Duration, Instant};

// Thread-local storage for testing and simple state management
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static SIMPLE_GLOBALS: RefCell<HashMap<String, String>> = RefCell::new(HashMap::new());
    static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    static SKIP_FLAG: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static TERMINATE_FLAG: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static TERMINATE_MESSAGE: RefCell<Option<String>> = RefCell::new(None);
    static CURRENT_CONTEXT: RefCell<Option<(*const GlobalVariables, usize, Option<String>)>> = RefCell::new(None);
}

/// Context passed to each processor
pub struct LineContext<'a> {
    pub line_number: usize,
    pub file_name: Option<&'a str>,
    pub global_vars: &'a GlobalVariables,
}

/// Result of processing a single line
#[derive(Debug)]
pub enum ProcessResult {
    /// Transform line (use Cow to avoid allocation if unchanged)
    Transform(Cow<'static, str>),
    /// Multiple output lines
    MultipleOutputs(Vec<String>),
    /// Transform with additional emitted lines
    TransformWithEmissions {
        primary: Option<Cow<'static, str>>,
        emissions: Vec<String>,
    },
    /// Skip this line (filter out)
    Skip,
    /// Stop processing entirely, with optional final output
    Terminate(Option<Cow<'static, str>>),
    /// Processing error
    Error(ProcessingError),
}

/// Main trait for line processing steps
pub trait LineProcessor: Send + Sync {
    fn process(&mut self, line: &str, ctx: &LineContext) -> ProcessResult;
    fn name(&self) -> &str;
    fn reset(&mut self) {} // Called between files/streams
}

/// Configuration for pipeline behavior
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub error_strategy: ErrorStrategy,
    pub debug: bool,
    pub buffer_size: usize,
    pub max_line_length: usize,
    pub progress_interval: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            error_strategy: ErrorStrategy::Skip,
            debug: false,
            buffer_size: 65536,       // 64KB
            max_line_length: 1048576, // 1MB
            progress_interval: 0,     // Disabled
        }
    }
}

/// Simple error handling strategy
#[derive(Debug, Clone)]
pub enum ErrorStrategy {
    /// Skip problematic lines and continue processing
    Skip,
    /// Stop processing on first error
    FailFast,
}

/// Runtime statistics
#[derive(Debug, Default, Clone)]
pub struct ProcessingStats {
    pub lines_processed: usize,
    pub lines_output: usize,
    pub lines_skipped: usize,
    pub errors: usize,
    pub processing_time: Duration,
}

/// Shared context across all processors
pub struct PipelineContext {
    pub global_vars: GlobalVariables,
    pub line_number: usize,
    pub total_processed: usize,
    pub file_name: Option<String>,
}

impl PipelineContext {
    pub fn new() -> Self {
        PipelineContext {
            global_vars: GlobalVariables::new(),
            line_number: 0,
            total_processed: 0,
            file_name: None,
        }
    }
}

/// Main pipeline orchestrator
pub struct StreamPipeline {
    processors: Vec<Box<dyn LineProcessor>>,
    context: PipelineContext,
    config: PipelineConfig,
    stats: ProcessingStats,
}

impl StreamPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        StreamPipeline {
            processors: Vec::new(),
            context: PipelineContext::new(),
            config,
            stats: ProcessingStats::default(),
        }
    }

    pub fn add_processor(&mut self, processor: Box<dyn LineProcessor>) {
        self.processors.push(processor);
    }

    pub fn get_global_vars(&self) -> &GlobalVariables {
        &self.context.global_vars
    }

    /// Reset processor state between files (but keep global variables)
    pub fn reset_processors(&mut self) {
        for processor in &mut self.processors {
            processor.reset();
        }
    }

    /// Process a single file/stream
    pub fn process_stream<R: BufRead, W: Write>(
        &mut self,
        input: R,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<ProcessingStats, ProcessingError> {
        let start_time = Instant::now();

        // Update context for new file
        self.context.file_name = filename.map(|s| s.to_string());
        self.context.line_number = 0;

        // Reset processor state (not global variables)
        for processor in &mut self.processors {
            processor.reset();
        }

        // Process the file
        for line_result in input.lines() {
            let line = line_result?;
            self.context.line_number += 1;
            self.stats.lines_processed += 1; // Count every line processed

            // Check line length using hardcoded limit
            if line.len() > self.config.max_line_length {
                let error = ProcessingError::LineTooLong {
                    length: line.len(),
                    max_length: self.config.max_line_length,
                };
                match self.config.error_strategy {
                    ErrorStrategy::FailFast => return Err(error),
                    ErrorStrategy::Skip => {
                        self.stats.errors += 1;
                        continue;
                    }
                }
            }

            match self.process_line(&line)? {
                ProcessResult::Transform(output_line) => {
                    writeln!(output, "{}", output_line)?;
                    self.stats.lines_output += 1;
                }
                ProcessResult::MultipleOutputs(outputs) => {
                    for output_line in outputs {
                        writeln!(output, "{}", output_line)?;
                        self.stats.lines_output += 1;
                    }
                }
                ProcessResult::TransformWithEmissions { primary, emissions } => {
                    if let Some(output_line) = primary {
                        writeln!(output, "{}", output_line)?;
                        self.stats.lines_output += 1;
                    }
                    for emission in emissions {
                        writeln!(output, "{}", emission)?;
                        self.stats.lines_output += 1;
                    }
                }
                ProcessResult::Skip => {
                    self.stats.lines_skipped += 1;
                }
                ProcessResult::Terminate(final_output) => {
                    // Output the final line if provided
                    if let Some(output_line) = final_output {
                        writeln!(output, "{}", output_line)?;
                        self.stats.lines_output += 1;
                    }
                    // Then stop processing - this is the key fix!
                    break;
                }
                ProcessResult::Error(err) => match self.config.error_strategy {
                    ErrorStrategy::FailFast => return Err(err),
                    ErrorStrategy::Skip => {
                        self.stats.errors += 1;
                        if self.config.debug {
                            eprintln!(
                                "Error processing line {}: {}",
                                self.context.line_number, err
                            );
                        }
                        continue;
                    }
                },
            }

            self.context.total_processed += 1;
        }

        self.stats.processing_time = start_time.elapsed();

        if self.config.debug {
            eprintln!(
                "Processing complete: {} lines processed, {} output, {} skipped, {} errors in {:?}",
                self.stats.lines_processed,
                self.stats.lines_output,
                self.stats.lines_skipped,
                self.stats.errors,
                self.stats.processing_time
            );
        }

        Ok(self.stats.clone())
    }

    fn process_line(&mut self, line: &str) -> Result<ProcessResult, ProcessingError> {
        let mut current_line = line.to_string();

        let ctx = LineContext {
            line_number: self.context.line_number,
            file_name: self.context.file_name.as_deref(),
            global_vars: &self.context.global_vars,
        };

        // Process through all processors in sequence
        for processor in &mut self.processors {
            match processor.process(&current_line, &ctx) {
                ProcessResult::Transform(new_line) => {
                    current_line = new_line.into_owned();
                    // Continue to next processor
                }
                ProcessResult::Skip => {
                    // If any processor skips, the whole line is skipped
                    return Ok(ProcessResult::Skip);
                }
                ProcessResult::Error(err) => {
                    // If any processor errors, handle according to error strategy
                    return Ok(ProcessResult::Error(err));
                }
                other_result => {
                    // For terminate, multiple outputs, etc., stop processing and return
                    return Ok(other_result);
                }
            }
        }

        Ok(ProcessResult::Transform(Cow::Owned(current_line)))
    }

    /// Completely reset everything (for reusing pipeline)
    pub fn hard_reset(&mut self) {
        self.context.global_vars.clear();
        self.context.line_number = 0;
        self.context.total_processed = 0;
        self.context.file_name = None;

        for processor in &mut self.processors {
            processor.reset();
        }

        self.stats = ProcessingStats::default();
    }
}

// Simple built-in functions using thread-local storage
use starlark::starlark_module;

#[starlark_module]
fn simple_globals(builder: &mut starlark::environment::GlobalsBuilder) {
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
        CURRENT_CONTEXT.with(|current_ctx| {
            *current_ctx.borrow_mut() = Some((
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
