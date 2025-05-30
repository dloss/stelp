use clap::{ArgAction, Parser};
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::context::ProcessingStats;
use stelp::processors::{FilterProcessor, StarlarkProcessor};

#[derive(Parser)]
#[command(name = "stelp")]
#[command(about = "Process text streams with Starlark scripts (Starlark Event and Line Processor)")]
#[command(version = "0.4.0")]
struct Args {
    /// Input files to process (default: stdin if none provided)
    #[arg(value_name = "FILE")]
    input_files: Vec<PathBuf>,

    /// Pipeline evaluation expressions (executed in order)
    #[arg(short = 'e', long = "eval", action = ArgAction::Append)]
    evals: Vec<String>,

    /// Script file containing pipeline definition
    #[arg(short = 'f', long = "file")]
    script_file: Option<PathBuf>,

    /// Filter expressions - remove lines where expression is true (executed before --eval)
    #[arg(long = "filter", action = ArgAction::Append)]
    filters: Vec<String>,

    /// Output file (default: stdout)
    #[arg(short = 'o', long = "output")]
    output_file: Option<PathBuf>,

    /// Debug mode - show processing details
    #[arg(long)]
    debug: bool,

    /// Fail on first error instead of skipping lines
    #[arg(long)]
    fail_fast: bool,
}

impl Args {
    fn validate(&self) -> Result<(), String> {
        let has_script_file = self.script_file.is_some();
        let has_evals = !self.evals.is_empty();
        let has_filters = !self.filters.is_empty();

        match (has_script_file, has_evals || has_filters) {
            (true, true) => Err("Cannot use --file with --eval or --filter arguments".to_string()),
            (true, false) => Ok(()), // Script file only
            (false, true) => Ok(()), // Eval/filter arguments only
            (false, false) => {
                Err("Must provide either --file or --eval/--filter arguments".to_string())
            }
        }
    }
}

fn main() {
    let args = Args::parse();

    if let Err(e) = args.validate() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    if let Err(e) = run(args) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // Create pipeline configuration with hardcoded sensible defaults
    let config = PipelineConfig {
        error_strategy: if args.fail_fast {
            ErrorStrategy::FailFast
        } else {
            ErrorStrategy::Skip
        },
        debug: args.debug,
        buffer_size: 65536,       // 64KB - good default
        max_line_length: 1048576, // 1MB - reasonable limit
        progress_interval: 0,     // Disabled - no progress reporting
    };

    // Create pipeline
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Add processors based on input
    if let Some(script_path) = args.script_file {
        // Load from script file
        let script_content = std::fs::read_to_string(&script_path).map_err(|e| {
            format!(
                "Failed to read script file '{}': {}",
                script_path.display(),
                e
            )
        })?;

        let processor = StarlarkProcessor::from_script(
            &format!("file:{}", script_path.display()),
            &script_content,
        )
        .map_err(|e| format!("Failed to compile script file: {}", e))?;

        pipeline.add_processor(Box::new(processor));
    } else {
        // Add filter processors first (they run before eval processors)
        for (i, filter_expr) in args.filters.iter().enumerate() {
            let processor =
                FilterProcessor::from_expression(&format!("filter_{}", i + 1), filter_expr)
                    .map_err(|e| format!("Failed to compile filter expression {}: {}", i + 1, e))?;

            pipeline.add_processor(Box::new(processor));
        }

        // Add processors from --eval arguments
        for (i, eval_expr) in args.evals.iter().enumerate() {
            let processor =
                StarlarkProcessor::from_script(&format!("eval_{}", i + 1), eval_expr)
                    .map_err(|e| format!("Failed to compile eval expression {}: {}", i + 1, e))?;

            pipeline.add_processor(Box::new(processor));
        }
    }

    // Set up output with hardcoded buffer size
    let mut output: Box<dyn Write> = if let Some(output_path) = &args.output_file {
        let file = File::create(output_path).map_err(|e| {
            format!(
                "Failed to create output file '{}': {}",
                output_path.display(),
                e
            )
        })?;
        Box::new(io::BufWriter::with_capacity(65536, file))
    } else {
        Box::new(io::BufWriter::with_capacity(65536, io::stdout()))
    };

    // Process input files or stdin
    let mut total_stats = ProcessingStats::default();

    if args.input_files.is_empty() {
        // No input files specified, read from stdin
        if args.debug {
            eprintln!("Reading from stdin...");
        }
        let input = BufReader::with_capacity(65536, io::stdin());
        let stats = pipeline
            .process_stream(input, &mut output, Some("<stdin>"))
            .map_err(|e| format!("Processing stdin failed: {}", e))?;
        total_stats = stats;
    } else {
        // Process each input file
        for (file_index, input_path) in args.input_files.iter().enumerate() {
            if args.debug {
                eprintln!("Processing file: {}", input_path.display());
            }

            let file = File::open(input_path).map_err(|e| {
                format!(
                    "Failed to open input file '{}': {}",
                    input_path.display(),
                    e
                )
            })?;
            let input = BufReader::with_capacity(65536, file);

            let filename = input_path.to_string_lossy();
            let stats = pipeline
                .process_stream(input, &mut output, Some(&filename))
                .map_err(|e| format!("Processing file '{}' failed: {}", input_path.display(), e))?;

            // Accumulate statistics
            if file_index == 0 {
                total_stats = stats;
            } else {
                total_stats.lines_processed += stats.lines_processed;
                total_stats.lines_output += stats.lines_output;
                total_stats.lines_skipped += stats.lines_skipped;
                total_stats.errors += stats.errors;
                total_stats.processing_time += stats.processing_time;
            }

            // Reset pipeline state between files (but keep globals)
            pipeline.reset_processors();
        }
    }

    // Ensure output is flushed
    output.flush()?;

    // Print final stats if debug mode
    if args.debug {
        eprintln!("Final statistics:");
        eprintln!(
            "  Files processed: {}",
            if args.input_files.is_empty() {
                1
            } else {
                args.input_files.len()
            }
        );
        eprintln!("  Lines processed: {}", total_stats.lines_processed);
        eprintln!("  Lines output: {}", total_stats.lines_output);
        eprintln!("  Lines skipped: {}", total_stats.lines_skipped);
        eprintln!("  Errors: {}", total_stats.errors);
        eprintln!("  Processing time: {:?}", total_stats.processing_time);

        if total_stats.lines_processed > 0 {
            let rate =
                total_stats.lines_processed as f64 / total_stats.processing_time.as_secs_f64();
            eprintln!("  Processing rate: {:.0} lines/second", rate);
        }
    }

    Ok(())
}
