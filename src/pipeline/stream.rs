// src/pipeline/stream.rs
use crate::variables::GlobalVariables;
use std::io::{BufRead, Write};
use std::time::Instant;

use crate::error::ProcessingError;
use crate::pipeline::config::{ErrorStrategy, PipelineConfig};
use crate::pipeline::context::{
    PipelineContext, ProcessResult, ProcessingStats, RecordContext, RecordData,
};

/// Main trait for record processing steps
pub trait RecordProcessor: Send + Sync {
    fn process(&mut self, record: &RecordData, ctx: &RecordContext) -> ProcessResult;
    fn name(&self) -> &str;
    fn reset(&mut self) {} // Called between files/streams
}

/// Main pipeline orchestrator
pub struct StreamPipeline {
    processors: Vec<Box<dyn RecordProcessor>>,
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

    pub fn add_processor(&mut self, processor: Box<dyn RecordProcessor>) {
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
        self.context.record_count = 0;

        // Reset local stats for this file
        let mut file_stats = ProcessingStats::default();

        // Reset processor state (not global variables)
        for processor in &mut self.processors {
            processor.reset();
        }

        // Process the file line by line
        for line_result in input.lines() {
            let line = match line_result {
                Ok(line) => line,
                Err(e) => {
                    // Handle broken pipe gracefully
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    return Err(ProcessingError::IoError(e));
                }
            };

            self.context.line_number += 1;
            self.context.record_count += 1;
            file_stats.records_processed += 1;

            // Check line length
            if line.len() > self.config.max_line_length {
                let error = ProcessingError::LineTooLong {
                    length: line.len(),
                    max_length: self.config.max_line_length,
                };
                match self.config.error_strategy {
                    ErrorStrategy::FailFast => return Err(error),
                    ErrorStrategy::Skip => {
                        file_stats.errors += 1;
                        if self.config.debug {
                            eprintln!(
                                "stelp: line {}: line too long, skipping",
                                self.context.line_number
                            );
                        }
                        continue;
                    }
                }
            }

            // Create initial record from line
            let record = RecordData::text(line);

            match self.process_record(&record)? {
                ProcessResult::Transform(output_record) => {
                    if let Err(e) = self.write_record(output, &output_record) {
                        // Handle broken pipe gracefully
                        if e.to_string().contains("Broken pipe") {
                            break;
                        }
                        return Err(e);
                    }
                    file_stats.records_output += 1;
                }
                ProcessResult::FanOut(output_records) => {
                    for output_record in output_records {
                        if let Err(e) = self.write_record(output, &output_record) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e);
                        }
                        file_stats.records_output += 1;
                    }
                }
                ProcessResult::TransformWithEmissions { primary, emissions } => {
                    if let Some(output_record) = primary {
                        if let Err(e) = self.write_record(output, &output_record) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e);
                        }
                        file_stats.records_output += 1;
                    }
                    for emission in emissions {
                        if let Err(e) = self.write_record(output, &emission) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e);
                        }
                        file_stats.records_output += 1;
                    }
                }
                ProcessResult::Skip => {
                    file_stats.records_skipped += 1;
                }
                ProcessResult::Exit(final_output) => {
                    // Output the final record if provided
                    if let Some(output_record) = final_output {
                        if let Err(e) = self.write_record(output, &output_record) {
                            if !e.to_string().contains("Broken pipe") {
                                return Err(e);
                            }
                        } else {
                            file_stats.records_output += 1;
                        }
                    }
                    // Stop processing
                    break;
                }
                ProcessResult::Error(err) => match self.config.error_strategy {
                    ErrorStrategy::FailFast => return Err(err),
                    ErrorStrategy::Skip => {
                        file_stats.errors += 1;
                        if self.config.debug {
                            eprintln!("stelp: line {}: {}", self.context.line_number, err);
                        }
                        continue;
                    }
                },
            }

            self.context.total_processed += 1;
        }

        file_stats.processing_time = start_time.elapsed();

        // Update global stats
        self.stats.records_processed += file_stats.records_processed;
        self.stats.records_output += file_stats.records_output;
        self.stats.records_skipped += file_stats.records_skipped;
        self.stats.errors += file_stats.errors;
        self.stats.processing_time += file_stats.processing_time;

        Ok(file_stats)
    }

    fn process_record(&mut self, record: &RecordData) -> Result<ProcessResult, ProcessingError> {
        let mut current_record = record.clone();

        let ctx = RecordContext {
            line_number: self.context.line_number,
            record_count: self.context.record_count,
            file_name: self.context.file_name.as_deref(),
            global_vars: &self.context.global_vars,
        };

        // Process through all processors in sequence
        for processor in &mut self.processors {
            match processor.process(&current_record, &ctx) {
                ProcessResult::Transform(new_record) => {
                    current_record = new_record;
                    // Continue to next processor
                }
                ProcessResult::Skip => {
                    // If any processor skips, the whole record is skipped
                    return Ok(ProcessResult::Skip);
                }
                ProcessResult::Error(err) => {
                    // If any processor errors, handle according to error strategy
                    return Ok(ProcessResult::Error(err));
                }
                other_result => {
                    // For terminate, fan-out, etc., stop processing and return
                    return Ok(other_result);
                }
            }
        }

        Ok(ProcessResult::Transform(current_record))
    }

    fn write_record<W: Write>(
        &self,
        output: &mut W,
        record: &RecordData,
    ) -> Result<(), ProcessingError> {
        match record {
            RecordData::Text(text) => {
                writeln!(output, "{}", text)?;
            }
            RecordData::Structured(data) => {
                // For now, write structured data as JSON
                writeln!(
                    output,
                    "{}",
                    serde_json::to_string(data).unwrap_or_else(|_| "null".to_string())
                )?;
            }
        }
        Ok(())
    }

    /// Get current accumulated stats
    pub fn get_stats(&self) -> &ProcessingStats {
        &self.stats
    }

    /// Completely reset everything (for reusing pipeline)
    pub fn hard_reset(&mut self) {
        self.context.global_vars.clear();
        self.context.line_number = 0;
        self.context.record_count = 0;
        self.context.total_processed = 0;
        self.context.file_name = None;

        for processor in &mut self.processors {
            processor.reset();
        }

        self.stats = ProcessingStats::default();
    }
}
