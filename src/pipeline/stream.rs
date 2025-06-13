// src/pipeline/stream.rs
use crate::output_format::OutputFormatter;
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
    output_formatter: OutputFormatter,
}

impl StreamPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        let output_formatter = OutputFormatter::new(config.output_format, config.keys.clone());
        StreamPipeline {
            processors: Vec::new(),
            context: PipelineContext::new(),
            output_formatter,
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

    pub fn get_config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Reset processor state between files (but keep global variables)
    pub fn reset_processors(&mut self) {
        for processor in &mut self.processors {
            processor.reset();
        }
        self.output_formatter.reset();
    }

    /// Enhanced process_stream that supports parsed data
    pub fn process_stream_with_data<R: BufRead, W: Write>(
        &mut self,
        reader: R,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<ProcessingStats, Box<dyn std::error::Error>> {
        // Convert your ProcessingError to Box<dyn Error>
        self.process_stream(reader, output, filename)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    /// Process a collection of records directly (used by input format parsers)
    pub fn process_records<W: Write>(
        &mut self,
        records: Vec<RecordData>,
        output: &mut W,
        filename: Option<&str>,
    ) -> Result<ProcessingStats, Box<dyn std::error::Error>> {
        let start_time = Instant::now();

        // Update context for new file
        self.context.file_name = filename.map(|s| s.to_string());
        self.context.line_number = 0;
        self.context.record_count = 0;

        // Reset local stats for this file
        let mut file_stats = ProcessingStats::default();

        for record in records {
            self.context.line_number += 1;
            self.context.record_count += 1;
            file_stats.records_processed += 1;

            // Process the record through the pipeline
            match self.process_record(&record).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)? {
                ProcessResult::Transform(output_record) => {
                    if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                        // Handle broken pipe gracefully
                        if e.to_string().contains("Broken pipe") {
                            break;
                        }
                        return Err(e.into());
                    }
                    file_stats.records_output += 1;
                }
                ProcessResult::FanOut(output_records) => {
                    for output_record in output_records {
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e.into());
                        }
                        file_stats.records_output += 1;
                    }
                }
                ProcessResult::TransformWithEmissions { primary, emissions } => {
                    if let Some(output_record) = primary {
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e.into());
                        }
                        file_stats.records_output += 1;
                    }
                    for emission in emissions {
                        if let Err(e) = self.output_formatter.write_record(output, &emission) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e.into());
                        }
                        file_stats.records_output += 1;
                    }
                }
                ProcessResult::Skip => {
                    file_stats.records_skipped += 1;
                }
                ProcessResult::Exit(final_output) => {
                    if let Some(output_record) = final_output {
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                            if !e.to_string().contains("Broken pipe") {
                                return Err(e.into());
                            }
                        } else {
                            file_stats.records_output += 1;
                        }
                    }
                    break; // Stop processing
                }
                ProcessResult::Error(err) => match self.config.error_strategy {
                    ErrorStrategy::FailFast => return Err(Box::new(err)),
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

        for line_result in input.lines() {
            let line = line_result.map_err(|e| ProcessingError::IoError(e))?;

            self.context.line_number += 1;
            self.context.record_count += 1;
            file_stats.records_processed += 1;

            // Check for empty lines
            let line_content = line.trim();
            if line_content.is_empty() {
                file_stats.records_skipped += 1;
                continue;
            }

            // Create text record from line
            let record = RecordData::text(line);

            // Process the record through the pipeline
            match self.process_record(&record)? {
                ProcessResult::Transform(output_record) => {
                    if let Err(e) = self.output_formatter.write_record(output, &output_record) {
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
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
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
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                            if e.to_string().contains("Broken pipe") {
                                break;
                            }
                            return Err(e);
                        }
                        file_stats.records_output += 1;
                    }
                    for emission in emissions {
                        if let Err(e) = self.output_formatter.write_record(output, &emission) {
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
                    if let Some(output_record) = final_output {
                        if let Err(e) = self.output_formatter.write_record(output, &output_record) {
                            if !e.to_string().contains("Broken pipe") {
                                return Err(e);
                            }
                        } else {
                            file_stats.records_output += 1;
                        }
                    }
                    break; // Stop processing
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

        self.output_formatter.reset();
        self.stats = ProcessingStats::default();
    }
}
