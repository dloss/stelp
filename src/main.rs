use clap::{ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser};
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

use stelp::chunking::{parse_chunk_strategy, ChunkConfig};
use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::context::ProcessingStats;
use stelp::input_format::{InputFormat, InputFormatWrapper};
use stelp::output_format::OutputFormat;
use stelp::StreamPipeline;
use stelp::{
    DeriveProcessor, ExtractProcessor, FilterProcessor, StarlarkProcessor, WindowProcessor,
};

#[derive(Debug, Clone)]
enum PipelineStep {
    Extract(String),
    Eval(String),
    Filter(String),
    Derive(String),
    ScriptFile(PathBuf),
}

#[derive(Parser)]
#[command(name = "stelp")]
#[command(about = "Process text streams with Starlark scripts (Starlark Event and Line Processor)")]
#[command(version)]
struct Args {
    // === CORE PROCESSING ===
    /// Pipeline evaluation expressions (executed in order)
    #[arg(short = 'e', long = "eval", action = ArgAction::Append)]
    evals: Vec<String>,

    /// Script file containing pipeline definition
    #[arg(short = 's', long = "script")]
    script_file: Option<PathBuf>,

    /// Include Starlark files (processed in order)
    #[arg(short = 'I', long = "include", action = ArgAction::Append)]
    includes: Vec<PathBuf>,

    /// BEGIN expression - Run before processing any input lines
    #[arg(long = "begin")]
    begin: Option<String>,

    /// END expression - Run after processing all input lines
    #[arg(long = "end")]
    end: Option<String>,

    // === DATA EXTRACTION & TRANSFORMATION ===
    /// Extract structured data using named patterns like '{field}' or '{field:type}'
    #[arg(long = "extract-vars")]
    extract_pattern: Option<String>,

    /// Filter expressions - Only keep lines where expression is true
    #[arg(long = "filter", action = ArgAction::Append)]
    filters: Vec<String>,

    /// Derive expressions - Transform structured data by injecting field variables
    #[arg(short = 'd', long = "derive", action = ArgAction::Append)]
    derives: Vec<String>,

    // === INPUT/OUTPUT FORMATS ===
    /// Input files to process (default: stdin if none provided)
    #[arg(value_name = "FILE")]
    input_files: Vec<PathBuf>,

    /// Input format for structured parsing
    #[arg(short = 'f', long = "input-format", value_enum)]
    input_format: Option<InputFormat>,

    /// Output file (default: stdout)
    #[arg(short = 'o', long = "output")]
    output_file: Option<PathBuf>,

    /// Output format
    #[arg(short = 'F', long = "output-format", value_enum)]
    output_format: Option<OutputFormat>,

    /// Restrict output to specific keys from structured data (comma-separated)
    #[arg(short = 'k', long = "keys")]
    keys: Option<String>,

    /// Remove keys from structured data output (comma-separated)
    #[arg(short = 'K', long = "remove-keys")]
    remove_keys: Option<String>,

    // === PROCESSING CONTROL ===
    /// Process N lines at a time
    #[arg(long)]
    chunk_lines: Option<usize>,

    /// Start new chunk on pattern match (regex)
    #[arg(long)]
    chunk_start: Option<String>,

    /// Chunks separated by delimiter
    #[arg(long)]
    chunk_delim: Option<String>,

    /// Window size - keep last N records for window functions
    #[arg(long = "window")]
    window_size: Option<usize>,

    /// Fail on first error instead of skipping lines
    #[arg(long)]
    fail_fast: bool,

    // === OUTPUT CONTROL ===
    /// Print only values, not keys (plain output mode)
    #[arg(short = 'p', long = "plain")]
    plain: bool,

    /// Show only records with these log levels (comma-separated)
    #[arg(short = 'l', long = "levels")]
    levels: Option<String>,

    /// Hide records with these log levels (comma-separated)
    #[arg(short = 'L', long = "exclude-levels")]
    exclude_levels: Option<String>,

    /// Force colored output even when not on TTY
    #[arg(long = "color", action = ArgAction::SetTrue)]
    force_color: bool,

    /// Disable colored output even when not TTY
    #[arg(long = "no-color", action = ArgAction::SetTrue)]
    no_color: bool,

    /// Show processing statistics
    #[arg(long)]
    stats: bool,

    /// Debug mode - show processing details
    #[arg(long)]
    debug: bool,

    /// List available built-in regex patterns and exit
    #[arg(long = "list-patterns")]
    list_patterns: bool,
}

impl Args {
    fn validate(&self) -> Result<(), String> {
        let has_script_file = self.script_file.is_some();
        let has_extract = self.extract_pattern.is_some();
        let has_evals = !self.evals.is_empty();
        let has_filters = !self.filters.is_empty();
        let has_derives = !self.derives.is_empty();
        let has_begin_end = self.begin.is_some() || self.end.is_some();
        let has_input_format = self.input_format.is_some();
        let has_output_format = self.output_format.is_some();
        let has_chunking =
            self.chunk_lines.is_some() || self.chunk_start.is_some() || self.chunk_delim.is_some();
        let has_level_filters = self.levels.is_some() || self.exclude_levels.is_some();
        let has_input_files = !self.input_files.is_empty();

        // Check for mutually exclusive chunking options
        let chunk_options_count = [
            self.chunk_lines.is_some(),
            self.chunk_start.is_some(),
            self.chunk_delim.is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count();

        if chunk_options_count > 1 {
            return Err("Cannot specify multiple chunking strategies simultaneously".to_string());
        }

        let has_any_processing =
            has_extract || has_evals || has_filters || has_derives || has_begin_end;
        let has_format_or_utility =
            has_input_format || has_output_format || has_chunking || has_level_filters;

        match (has_script_file, has_any_processing, has_format_or_utility, has_input_files) {
            (true, true, _, _) => Err("Cannot use --script with other processing options".to_string()),
            (true, false, _, _) => Ok(()),     // Script file only
            (false, true, _, _) => Ok(()),     // Processing arguments
            (false, false, true, _) => Ok(()), // Format/utility options only
            (false, false, false, true) => Ok(()), // Input files only - allow smart defaults
            (false, false, false, false) => {
                Err("SHOW_HELP".to_string()) // Special case to trigger help display
            }
        }
    }

    /// Extract pipeline steps in the order they appeared on the command line
    fn get_pipeline_steps(&self, matches: &ArgMatches) -> Result<Vec<PipelineStep>, String> {
        let mut steps_with_indices = Vec::new();

        // Get eval steps with their indices
        if let Some(eval_indices) = matches.indices_of("evals") {
            let eval_values: Vec<&String> = matches.get_many::<String>("evals").unwrap().collect();
            for (pos, index) in eval_indices.enumerate() {
                steps_with_indices.push((index, PipelineStep::Eval(eval_values[pos].clone())));
            }
        }

        // Get filter steps with their indices
        if let Some(filter_indices) = matches.indices_of("filters") {
            let filter_values: Vec<&String> =
                matches.get_many::<String>("filters").unwrap().collect();
            for (pos, index) in filter_indices.enumerate() {
                steps_with_indices.push((index, PipelineStep::Filter(filter_values[pos].clone())));
            }
        }

        // Get derive steps with their indices
        if let Some(derive_indices) = matches.indices_of("derives") {
            let derive_values: Vec<&String> =
                matches.get_many::<String>("derives").unwrap().collect();
            for (pos, index) in derive_indices.enumerate() {
                steps_with_indices.push((index, PipelineStep::Derive(derive_values[pos].clone())));
            }
        }

        // Handle extract pattern - it doesn't have an index, so we place it first
        if let Some(extract_pattern) = &self.extract_pattern {
            steps_with_indices.push((0, PipelineStep::Extract(extract_pattern.clone())));
        }

        // Handle script file - it doesn't have an index, so we place it first
        if let Some(script_file) = &self.script_file {
            steps_with_indices.push((0, PipelineStep::ScriptFile(script_file.clone())));
        }

        // Sort by original command line position
        steps_with_indices.sort_by_key(|(index, _)| *index);

        // Extract just the steps
        Ok(steps_with_indices
            .into_iter()
            .map(|(_, step)| step)
            .collect())
    }

    fn get_chunk_config(&self) -> Result<Option<ChunkConfig>, String> {
        if let Some(lines) = self.chunk_lines {
            Ok(Some(ChunkConfig {
                strategy: parse_chunk_strategy(&format!("lines:{}", lines))?,
                ..Default::default()
            }))
        } else if let Some(pattern) = &self.chunk_start {
            Ok(Some(ChunkConfig {
                strategy: parse_chunk_strategy(&format!("start-pattern:{}", pattern))?,
                ..Default::default()
            }))
        } else if let Some(delimiter) = &self.chunk_delim {
            Ok(Some(ChunkConfig {
                strategy: parse_chunk_strategy(&format!("delimiter:{}", delimiter))?,
                ..Default::default()
            }))
        } else {
            Ok(None)
        }
    }

    /// Determine whether to use colors based on flags and environment
    fn determine_color_usage(&self) -> Option<bool> {
        if self.force_color {
            Some(true)
        } else if self.no_color {
            Some(false)
        } else {
            None // Auto-detect based on TTY
        }
    }
}

/// Build the final script by concatenating includes and user script
fn build_final_script(includes: &[PathBuf], user_script: &str) -> Result<String, String> {
    let mut final_script = String::new();

    // Add includes in order
    for include_path in includes {
        let include_content = std::fs::read_to_string(include_path)
            .map_err(|e| format!("Include file '{}' not found: {}", include_path.display(), e))?;
        final_script.push_str(&include_content);
        final_script.push_str("\n\n");
    }

    // Add user script
    final_script.push_str(user_script);

    Ok(final_script)
}

fn main() {
    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches).unwrap_or_else(|e| {
        eprintln!("stelp: argument parsing failed: {}", e);
        std::process::exit(1);
    });

    // Handle --list-patterns option early
    if args.list_patterns {
        use stelp::pipeline::global_functions::get_pattern_list;

        for (name, description) in get_pattern_list() {
            println!("{:<15} - {}", name, description);
        }
        std::process::exit(0);
    }

    // Validate arguments
    if let Err(e) = args.validate() {
        if e == "SHOW_HELP" {
            let mut cmd = Args::command();
            println!("{}", cmd.render_usage());
            println!("Try 'stelp --help' for more information.");
            std::process::exit(0);
        } else {
            eprintln!("stelp: {}", e);
            std::process::exit(1);
        }
    }

    // Build pipeline steps first (before moving parts of args)
    let steps = args.get_pipeline_steps(&matches).unwrap_or_else(|e| {
        eprintln!("stelp: failed to parse pipeline steps: {}", e);
        std::process::exit(1);
    });

    // Extract or auto-detect input format before creating config
    let input_format = match args.input_format.clone() {
        Some(format) => Some(format), // User explicitly specified format
        None => {
            // Auto-detect from first input file if available
            if let Some(first_file) = args.input_files.first() {
                InputFormat::from_extension(first_file)
            } else {
                // No input files or no detectable format, default to Line
                Some(InputFormat::Line)
            }
        }
    };

    // Extract chunking config before moving args
    let chunk_config = args.get_chunk_config().unwrap_or_else(|e| {
        eprintln!("stelp: {}", e);
        std::process::exit(1);
    });

    // Extract color preference before moving args
    let color_preference = args.determine_color_usage();

    // Build configuration with smart output format defaulting
    let output_format = match args.output_format {
        Some(format) => format, // User explicitly specified output format
        None => {
            // Default based on input format and plain mode
            match input_format {
                Some(InputFormat::Line) => OutputFormat::Line, // Text input defaults to text output
                _ => OutputFormat::Logfmt, // All structured formats default to logfmt (plain mode affects rendering, not format choice)
            }
        }
    };

    let keys = args.keys.as_ref().map(|k| {
        k.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>()
    });

    let remove_keys = args.remove_keys.as_ref().map(|k| {
        k.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>()
    });

    let config = PipelineConfig {
        error_strategy: if args.fail_fast {
            ErrorStrategy::FailFast
        } else {
            ErrorStrategy::Skip
        },
        debug: args.debug,
        input_format: input_format.clone(),
        output_format, // Use the determined format
        keys,
        remove_keys,
        color_preference,
        plain: args.plain,
        ..Default::default()
    };

    // Create pipeline
    let mut pipeline = StreamPipeline::new(config);

    // Create input format wrapper with optional chunking
    let format_wrapper = if let Some(config) = chunk_config {
        InputFormatWrapper::new(input_format.as_ref()).with_chunking(config)
    } else {
        InputFormatWrapper::new(input_format.as_ref())
    };

    // Add level filter processor if specified
    if args.levels.is_some() || args.exclude_levels.is_some() {
        let level_filter = stelp::LevelFilterProcessor::new(
            "level_filter",
            args.levels.as_deref(),
            args.exclude_levels.as_deref(),
        );
        pipeline.add_processor(Box::new(level_filter));
    }

    // Add processors to pipeline in order
    for (i, step) in steps.iter().enumerate() {
        match step {
            PipelineStep::Extract(pattern) => {
                let processor = ExtractProcessor::new(&format!("extract_{}", i + 1), pattern)
                    .unwrap_or_else(|e| {
                        eprintln!("stelp: failed to compile extract-vars pattern: {}", e);
                        std::process::exit(1);
                    });
                let final_processor: Box<dyn stelp::pipeline::stream::RecordProcessor> =
                    if let Some(window_size) = args.window_size {
                        Box::new(WindowProcessor::new(window_size, Box::new(processor)))
                    } else {
                        Box::new(processor)
                    };
                pipeline.add_processor(final_processor);
            }
            PipelineStep::Eval(eval_expr) => {
                let final_script =
                    build_final_script(&args.includes, eval_expr).unwrap_or_else(|e| {
                        eprintln!("stelp: {}", e);
                        std::process::exit(1);
                    });
                let processor =
                    StarlarkProcessor::from_script(&format!("eval_{}", i + 1), &final_script)
                        .unwrap_or_else(|e| {
                            eprintln!("stelp: failed to compile eval expression {}: {}", i + 1, e);
                            std::process::exit(1);
                        });
                let final_processor: Box<dyn stelp::pipeline::stream::RecordProcessor> =
                    if let Some(window_size) = args.window_size {
                        Box::new(WindowProcessor::new(window_size, Box::new(processor)))
                    } else {
                        Box::new(processor)
                    };
                pipeline.add_processor(final_processor);
            }
            PipelineStep::Filter(filter_expr) => {
                let final_script =
                    build_final_script(&args.includes, filter_expr).unwrap_or_else(|e| {
                        eprintln!("stelp: {}", e);
                        std::process::exit(1);
                    });
                let processor =
                    FilterProcessor::from_expression(&format!("filter_{}", i + 1), &final_script)
                        .unwrap_or_else(|e| {
                            eprintln!(
                                "stelp: failed to compile filter expression {}: {}",
                                i + 1,
                                e
                            );
                            std::process::exit(1);
                        });
                let final_processor: Box<dyn stelp::pipeline::stream::RecordProcessor> =
                    if let Some(window_size) = args.window_size {
                        Box::new(WindowProcessor::new(window_size, Box::new(processor)))
                    } else {
                        Box::new(processor)
                    };
                pipeline.add_processor(final_processor);
            }
            PipelineStep::Derive(derive_expr) => {
                let final_script =
                    build_final_script(&args.includes, derive_expr).unwrap_or_else(|e| {
                        eprintln!("stelp: {}", e);
                        std::process::exit(1);
                    });
                let processor =
                    DeriveProcessor::from_script(&format!("derive_{}", i + 1), &final_script)
                        .unwrap_or_else(|e| {
                            eprintln!(
                                "stelp: failed to compile derive expression {}: {}",
                                i + 1,
                                e
                            );
                            std::process::exit(1);
                        });
                let final_processor: Box<dyn stelp::pipeline::stream::RecordProcessor> =
                    if let Some(window_size) = args.window_size {
                        Box::new(WindowProcessor::new(window_size, Box::new(processor)))
                    } else {
                        Box::new(processor)
                    };
                pipeline.add_processor(final_processor);
            }
            PipelineStep::ScriptFile(script_path) => {
                let script_content = std::fs::read_to_string(script_path).unwrap_or_else(|e| {
                    eprintln!(
                        "stelp: failed to read script file '{}': {}",
                        script_path.display(),
                        e
                    );
                    std::process::exit(1);
                });
                let final_script = build_final_script(&args.includes, &script_content)
                    .unwrap_or_else(|e| {
                        eprintln!("stelp: {}", e);
                        std::process::exit(1);
                    });
                let processor = StarlarkProcessor::from_script(
                    &format!("script:{}", script_path.display()),
                    &final_script,
                )
                .unwrap_or_else(|e| {
                    eprintln!("stelp: failed to compile script file: {}", e);
                    std::process::exit(1);
                });
                let final_processor: Box<dyn stelp::pipeline::stream::RecordProcessor> =
                    if let Some(window_size) = args.window_size {
                        Box::new(WindowProcessor::new(window_size, Box::new(processor)))
                    } else {
                        Box::new(processor)
                    };
                pipeline.add_processor(final_processor);
            }
        }
    }

    // Add default identity processor if no processing steps were provided
    if steps.is_empty() && !args.input_files.is_empty() {
        // For structured formats, a simple identity transform that preserves the data
        // For line format, just pass through the line
        let identity_script = match input_format.as_ref().unwrap_or(&InputFormat::Line) {
            InputFormat::Line => "line",  // Pass through the line as-is
            _ => "data",  // Pass through structured data (gets formatted by output formatter)
        };
        
        let processor = StarlarkProcessor::from_script("identity", identity_script)
            .unwrap_or_else(|e| {
                eprintln!("stelp: failed to compile default identity processor: {}", e);
                std::process::exit(1);
            });
        pipeline.add_processor(Box::new(processor));
    }

    // Add BEGIN processor if specified
    if let Some(begin_expr) = &args.begin {
        let final_script = build_final_script(&args.includes, begin_expr).unwrap_or_else(|e| {
            eprintln!("stelp: {}", e);
            std::process::exit(1);
        });
        let processor =
            StarlarkProcessor::from_script("BEGIN", &final_script).unwrap_or_else(|e| {
                eprintln!("stelp: failed to compile BEGIN expression: {}", e);
                std::process::exit(1);
            });
        pipeline.set_begin_processor(Box::new(processor));
    }

    // Add END processor if specified
    if let Some(end_expr) = &args.end {
        let final_script = build_final_script(&args.includes, end_expr).unwrap_or_else(|e| {
            eprintln!("stelp: {}", e);
            std::process::exit(1);
        });
        let processor = StarlarkProcessor::from_script("END", &final_script).unwrap_or_else(|e| {
            eprintln!("stelp: failed to compile END expression: {}", e);
            std::process::exit(1);
        });
        pipeline.set_end_processor(Box::new(processor));
    }

    // Set up output
    let mut output: Box<dyn Write> = if let Some(output_path) = &args.output_file {
        let file = File::create(output_path).unwrap_or_else(|e| {
            eprintln!(
                "stelp: failed to create output file '{}': {}",
                output_path.display(),
                e
            );
            std::process::exit(1);
        });
        Box::new(io::BufWriter::with_capacity(65536, file))
    } else {
        Box::new(io::BufWriter::with_capacity(65536, io::stdout()))
    };

    // Process input files or stdin
    let mut total_stats = ProcessingStats::default();

    if args.input_files.is_empty() {
        // No input files specified, read from stdin
        if args.debug {
            eprintln!("stelp: reading from stdin");
        }
        let input = BufReader::with_capacity(65536, io::stdin());
        let stats = format_wrapper
            .process_with_pipeline(input, &mut pipeline, &mut output, Some("<stdin>"))
            .unwrap_or_else(|e| {
                eprintln!("stelp: processing stdin failed: {}", e);
                std::process::exit(1);
            });
        total_stats = stats;
    } else {
        // Process each input file
        for (file_index, input_path) in args.input_files.iter().enumerate() {
            if args.debug {
                eprintln!("stelp: processing file: {}", input_path.display());
            }

            let file = File::open(input_path).unwrap_or_else(|e| {
                eprintln!(
                    "stelp: failed to open input file '{}': {}",
                    input_path.display(),
                    e
                );
                std::process::exit(1);
            });
            let input = BufReader::with_capacity(65536, file);

            let filename = input_path.to_string_lossy();
            let stats = format_wrapper
                .process_with_pipeline(input, &mut pipeline, &mut output, Some(&filename))
                .unwrap_or_else(|e| {
                    eprintln!(
                        "stelp: processing file '{}' failed: {}",
                        input_path.display(),
                        e
                    );
                    std::process::exit(1);
                });

            // Accumulate statistics
            if file_index == 0 {
                total_stats = stats;
            } else {
                total_stats.records_processed += stats.records_processed;
                total_stats.records_output += stats.records_output;
                total_stats.records_skipped += stats.records_skipped;
                total_stats.errors += stats.errors;
                total_stats.processing_time += stats.processing_time;
                total_stats.parse_errors.extend(stats.parse_errors);
            }

            // Reset pipeline state between files (but keep globals)
            pipeline.reset_processors();
        }
    }

    // Ensure output is flushed
    if let Err(e) = output.flush() {
        if e.kind() == io::ErrorKind::BrokenPipe {
            std::process::exit(0); // Normal termination
        } else {
            eprintln!("stelp: failed to flush output: {}", e);
            std::process::exit(1);
        }
    }

    // Report CSV warnings about missing keys
    pipeline.get_output_formatter().report_csv_warnings();

    // Report parse errors in debug mode
    if args.debug && !total_stats.parse_errors.is_empty() {
        if total_stats.parse_errors.len() <= 5 {
            // Show individual errors for small counts
            for error in &total_stats.parse_errors {
                eprintln!(
                    "stelp: line {}: {} parse error: {}",
                    error.line_number, error.format_name, error.error
                );
            }
        } else {
            // Show summary for large counts
            eprintln!(
                "stelp: {} parse errors encountered",
                total_stats.parse_errors.len()
            );
        }
    }

    // Print performance stats if requested
    if args.stats {
        let processing_ms = total_stats.processing_time.as_secs_f64() * 1000.0;
        let records_per_sec = if processing_ms > 0.0 {
            (total_stats.records_processed as f64) / (processing_ms / 1000.0)
        } else {
            0.0
        };

        eprintln!(
            "stelp: {} records processed, {} output, {} skipped, {} errors in {:.2}ms ({:.0} records/s)",
            total_stats.records_processed,
            total_stats.records_output,
            total_stats.records_skipped,
            total_stats.errors,
            processing_ms,
            records_per_sec
        );
    }

    // Determine exit code based on results
    let exit_code = {
        let pipeline_exit_code = pipeline.get_exit_code();
        if pipeline_exit_code != 0 {
            pipeline_exit_code // Use exit code from exit() function
        } else if total_stats.errors > 0 {
            1 // Processing errors occurred
        } else if total_stats.records_output == 0 {
            2 // No output produced
        } else {
            0 // Success
        }
    };

    std::process::exit(exit_code);
}
