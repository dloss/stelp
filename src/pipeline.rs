use std::borrow::Cow;
use std::io::{BufRead, Write};
use std::time::{Duration, Instant};
use starlark::syntax::{AstModule, Dialect};
use starlark::environment::{GlobalsBuilder, Module, FrozenModule};
use starlark::eval::Evaluator;
use starlark::values::Value;
use crate::error::*;
use crate::variables::GlobalVariables;
use crate::builtins::{global_functions, EMIT_BUFFER, SKIP_FLAG, TERMINATE_FLAG, GLOBAL_VARS_REF, LINE_CONTEXT};

/// Context passed to each processor
pub struct LineContext<'a> {
    pub line_number: usize,
    pub file_name: Option<&'a str>,
    pub global_vars: &'a GlobalVariables,
}

/// Result of processing a single line
#[derive(Debug, Clone)]
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
            buffer_size: 65536,
            max_line_length: 1048576,
            progress_interval: 10000,
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

    /// Process a single file/stream
    pub fn process_stream<R: BufRead, W: Write>(&mut self, 
                                               input: R, 
                                               output: &mut W,
                                               filename: Option<&str>) -> Result<ProcessingStats, ProcessingError> {
        let start_time = Instant::now();
        
        // Update context for new file
        self.context.file_name = filename.map(|s| s.to_string());
        self.context.line_number = 0;
        // Note: global_vars are NOT reset - they persist across files
        
        // Reset processor state (not global variables)
        for processor in &mut self.processors {
            processor.reset();
        }
        
        // Process the file
        for line_result in input.lines() {
            let line = line_result?;
            self.context.line_number += 1;
            
            // Check line length
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
                    // Then stop processing
                    break;
                }
                ProcessResult::Error(err) => {
                    match self.config.error_strategy {
                        ErrorStrategy::FailFast => return Err(err),
                        ErrorStrategy::Skip => {
                            self.stats.errors += 1;
                            if self.config.debug {
                                eprintln!("Error processing line {}: {}", self.context.line_number, err);
                            }
                            continue;
                        }
                    }
                }
            }
            
            self.stats.lines_processed += 1;
            self.context.total_processed += 1;
            
            // Progress reporting
            if self.config.progress_interval > 0 && 
               self.stats.lines_processed % self.config.progress_interval == 0 {
                eprintln!("Processed {} lines", self.stats.lines_processed);
            }
        }
        
        self.stats.processing_time = start_time.elapsed();
        
        if self.config.debug {
            eprintln!("Processing complete: {} lines processed, {} output, {} skipped, {} errors in {:?}",
                     self.stats.lines_processed,
                     self.stats.lines_output, 
                     self.stats.lines_skipped,
                     self.stats.errors,
                     self.stats.processing_time);
        }
        
        Ok(self.stats.clone())
    }
    
    fn process_line(&mut self, line: &str) -> Result<ProcessResult, ProcessingError> {
        let mut current_line = Cow::Borrowed(line);
        
        let ctx = LineContext {
            line_number: self.context.line_number,
            file_name: self.context.file_name.as_deref(),
            global_vars: &self.context.global_vars,
        };
        
        // Process through all processors in sequence
        for processor in &mut self.processors {
            match processor.process(&current_line, &ctx) {
                ProcessResult::Transform(new_line) => {
                    current_line = new_line;
                }
                other_result => return Ok(other_result),
            }
        }
        
        Ok(ProcessResult::Transform(current_line))
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

/// Starlark-based line processor
pub struct StarlarkProcessor {
    frozen_globals: FrozenModule,
    compiled_ast: AstModule,
    name: String,
}

impl StarlarkProcessor {
    /// Create from script source
    pub fn from_script(name: &str, script: &str) -> Result<Self, CompilationError> {
        // Create frozen globals with built-ins
        let globals = GlobalsBuilder::new()
            .with(starlark::stdlib::LibraryExtension::StructType)
            .with(starlark::stdlib::LibraryExtension::Map)
            .with(global_functions)
            .build();
        
        let frozen_globals = globals.freeze()?;
        
        // Compile the script
        let ast = AstModule::parse("script", script, &Dialect::Extended)?;
        
        Ok(StarlarkProcessor {
            frozen_globals,
            compiled_ast: ast,
            name: name.to_string(),
        })
    }
    
    /// Execute script with fresh module per line
    fn execute_with_context(&self, 
                          line: &str, 
                          ctx: &LineContext) -> Result<Value, starlark::Error> {
        // Set up thread-local context
        GLOBAL_VARS_REF.with(|global_ref| {
            *global_ref.borrow_mut() = Some(ctx.global_vars as *const GlobalVariables);
        });
        
        LINE_CONTEXT.with(|line_ctx| {
            *line_ctx.borrow_mut() = Some((ctx.line_number, ctx.file_name.map(|s| s.to_string())));
        });
        
        // Create fresh module for each line (local variables)
        let module = Module::new();
        module.frozen_heap().add_reference(self.frozen_globals.frozen_heap());
        
        // Set built-in variables
        module.set("line", Value::new(line.to_string()));
        module.set("LINE_NUMBER", Value::new(ctx.line_number as i32));
        if let Some(filename) = ctx.file_name {
            module.set("FILE_NAME", Value::new(filename.to_string()));
        }
        
        // Execute pre-compiled AST with frozen globals available
        let mut eval = Evaluator::new(&module);
        eval.eval_module(&self.compiled_ast, &self.frozen_globals)
    }
}

impl LineProcessor for StarlarkProcessor {
    fn process(&mut self, line: &str, ctx: &LineContext) -> ProcessResult {
        // Clear emit buffer and flags
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        TERMINATE_FLAG.with(|flag| flag.set(false));
        
        // Execute script
        match self.execute_with_context(line, ctx) {
            Ok(value) => {
                // Collect emitted lines
                let emissions: Vec<String> = EMIT_BUFFER.with(|buffer| {
                    buffer.borrow().clone()
                });
                
                // Check for special control values
                if SKIP_FLAG.with(|flag| flag.get()) {
                    if emissions.is_empty() {
                        ProcessResult::Skip
                    } else {
                        ProcessResult::MultipleOutputs(emissions)
                    }
                } else if TERMINATE_FLAG.with(|flag| flag.get()) {
                    let final_output = if value.is_none() {
                        None
                    } else {
                        Some(Cow::Owned(value.to_string()))
                    };
                    ProcessResult::Terminate(final_output)
                } else {
                    // Normal processing
                    match (value.is_none(), emissions.is_empty()) {
                        (true, true) => ProcessResult::Transform(Cow::Borrowed(line)), // No change
                        (true, false) => ProcessResult::MultipleOutputs(emissions),
                        (false, true) => ProcessResult::Transform(Cow::Owned(value.to_string())),
                        (false, false) => ProcessResult::TransformWithEmissions {
                            primary: Some(Cow::Owned(value.to_string())),
                            emissions,
                        },
                    }
                }
            }
            Err(starlark_error) => ProcessResult::Error(
                ProcessingError::ScriptError {
                    step: self.name.clone(),
                    line: ctx.line_number,
                    source: starlark_error,
                }
            ),
        }
    }
    
    fn name(&self) -> &str {
        &self.name
    }
}

// Add a convenience method for executing processors outside the main pipeline
impl StarlarkProcessor {
    pub fn process(&self, line: &str, ctx: &LineContext) -> ProcessResult {
        // Clear emit buffer and flags
        EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
        SKIP_FLAG.with(|flag| flag.set(false));
        TERMINATE_FLAG.with(|flag| flag.set(false));
        
        // Execute script
        match self.execute_with_context(line, ctx) {
            Ok(value) => {
                // Collect emitted lines
                let emissions: Vec<String> = EMIT_BUFFER.with(|buffer| {
                    buffer.borrow().clone()
                });
                
                // Check for special control values
                if SKIP_FLAG.with(|flag| flag.get()) {
                    if emissions.is_empty() {
                        ProcessResult::Skip
                    } else {
                        ProcessResult::MultipleOutputs(emissions)
                    }
                } else if TERMINATE_FLAG.with(|flag| flag.get()) {
                    let final_output = if value.is_none() {
                        None
                    } else {
                        Some(Cow::Owned(value.to_string()))
                    };
                    ProcessResult::Terminate(final_output)
                } else {
                    // Normal processing
                    match (value.is_none(), emissions.is_empty()) {
                        (true, true) => ProcessResult::Transform(Cow::Borrowed(line)), // No change
                        (true, false) => ProcessResult::MultipleOutputs(emissions),
                        (false, true) => ProcessResult::Transform(Cow::Owned(value.to_string())),
                        (false, false) => ProcessResult::TransformWithEmissions {
                            primary: Some(Cow::Owned(value.to_string())),
                            emissions,
                        },
                    }
                }
            }
            Err(starlark_error) => ProcessResult::Error(
                ProcessingError::ScriptError {
                    step: self.name.clone(),
                    line: ctx.line_number,
                    source: starlark_error,
                }
            ),
        }
    }
}