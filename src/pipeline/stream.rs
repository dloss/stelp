use crate::variables::GlobalVariables;
use std::borrow::Cow;
use std::io::{BufRead, Write};
use std::time::Instant;

use crate::error::ProcessingError;
use crate::pipeline::config::{ErrorStrategy, PipelineConfig};
use crate::pipeline::context::{LineContext, PipelineContext, ProcessResult, ProcessingStats};

/// Main trait for line processing steps
pub trait LineProcessor: Send + Sync {
    fn process(&mut self, line: &str, ctx: &LineContext) -> ProcessResult;
    fn name(&self) -> &str;
    fn reset(&mut self) {} // Called between files/streams
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
