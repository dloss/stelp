use clap::{ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser};
use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::context::ProcessingStats;
use stelp::processors::{FilterProcessor, StarlarkProcessor};

#[derive(Debug, Clone)]
enum PipelineStep {
    Eval(String),
    Filter(String),
    ScriptFile(PathBuf),
}

#[derive(Parser)]
#[command(name = "stelp")]
#[command(about = "Process text streams with Starlark scripts (Starlark Event and Line Processor)")]
#[command(version = "0.5.0")]
struct Args {
    /// Input files to process (default: stdin if none provided)
    #[arg(value_name = "FILE")]
    input_files: Vec<PathBuf>,

    /// Include Starlark files (processed in order)
    #[arg(long = "include", action = ArgAction::Append)]
    includes: Vec<PathBuf>,

    /// Pipeline evaluation expressions (executed in order)
    #[arg(short = 'e', long = "eval", action = ArgAction::Append)]
    evals: Vec<String>,

    /// Script file containing pipeline definition
    #[arg(short = 'f', long = "file")]
    script_file: Option<PathBuf>,

    /// Filter expressions - Only keep lines expression is true
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

    /// Extract pipeline steps in the order they appeared on the command line
    fn get_pipeline_steps(&self, matches: &ArgMatches) -> Result<Vec<PipelineStep>, String> {
        let mut steps_with_indices = Vec::new();

        // Get eval steps with their indices
        if let Some(eval_indices) = matches.indices_of("evals") {
            let eval_values: Vec<&String> = matches
                .get_many::<String>("evals")
                .unwrap_or_default()
                .collect();

            for (i, index) in eval_indices.enumerate() {
                if i < eval_values.len() {
                    steps_with_indices.push((index, PipelineStep::Eval(eval_values[i].clone())));
                }
            }
        }

        // Get filter steps with their indices
        if let Some(filter_indices) = matches.indices_of("filters") {
            let filter_values: Vec<&String> = matches
                .get_many::<String>("filters")
                .unwrap_or_default()
                .collect();

            for (i, index) in filter_indices.enumerate() {
                if i < filter_values.len() {
                    steps_with_indices
                        .push((index, PipelineStep::Filter(filter_values[i].clone())));
                }
            }
        }

        // Add script file if present (it doesn't have a specific position, so put it first)
        if let Some(ref script_path) = self.script_file {
            steps_with_indices.push((0, PipelineStep::ScriptFile(script_path.clone())));
        }

        // Sort by command line position to get correct order
        steps_with_indices.sort_by_key(|(index, _)| *index);

        Ok(steps_with_indices
            .into_iter()
            .map(|(_, step)| step)
            .collect())
    }
}

/// Build the final script by concatenating includes and user script
fn build_final_script(
    includes: &[PathBuf],
    user_script: &str,
) -> Result<String, Box<dyn std::error::Error>> {
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
    // Parse args using both derive API and manual matching for indices
    let matches = Args::command().get_matches();
    let args = match Args::from_arg_matches(&matches) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error parsing arguments: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = args.validate() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    if let Err(e) = run(args, &matches) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(args: Args, matches: &ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    // Create pipeline configuration
    let config = PipelineConfig {
        error_strategy: if args.fail_fast {
            ErrorStrategy::FailFast
        } else {
            ErrorStrategy::Skip
        },
        debug: args.debug,
        buffer_size: 65536,
        max_line_length: 1048576,
        progress_interval: 0,
    };

    // Create pipeline
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Get pipeline steps in command-line order
    let pipeline_steps = args.get_pipeline_steps(matches)?;

    if args.debug {
        eprintln!("Pipeline steps (in order):");
        for (i, step) in pipeline_steps.iter().enumerate() {
            match step {
                PipelineStep::Eval(expr) => eprintln!("  {}: eval: {}", i + 1, expr),
                PipelineStep::Filter(expr) => eprintln!("  {}: filter: {}", i + 1, expr),
                PipelineStep::ScriptFile(path) => {
                    eprintln!("  {}: script: {}", i + 1, path.display())
                }
            }
        }
    }

    // Add processors based on ordered steps
    for (i, step) in pipeline_steps.iter().enumerate() {
        match step {
            PipelineStep::Eval(eval_expr) => {
                let final_script = build_final_script(&args.includes, eval_expr)?;
                let processor =
                    StarlarkProcessor::from_script(&format!("eval_{}", i + 1), &final_script)
                        .map_err(|e| {
                            format!("Failed to compile eval expression {}: {}", i + 1, e)
                        })?;
                pipeline.add_processor(Box::new(processor));
            }
            PipelineStep::Filter(filter_expr) => {
                let final_script = build_final_script(&args.includes, filter_expr)?;
                let processor = FilterProcessor::from_expression(
                    &format!("filter_{}", i + 1),
                    &final_script,
                )
                .map_err(|e| format!("Failed to compile filter expression {}: {}", i + 1, e))?;
                pipeline.add_processor(Box::new(processor));
            }
            PipelineStep::ScriptFile(script_path) => {
                let script_content = std::fs::read_to_string(script_path).map_err(|e| {
                    format!(
                        "Failed to read script file '{}': {}",
                        script_path.display(),
                        e
                    )
                })?;
                let final_script = build_final_script(&args.includes, &script_content)?;
                let processor = StarlarkProcessor::from_script(
                    &format!("file:{}", script_path.display()),
                    &final_script,
                )
                .map_err(|e| format!("Failed to compile script file: {}", e))?;
                pipeline.add_processor(Box::new(processor));
            }
        }
    }

    // Set up output
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
                total_stats.records_processed += stats.records_processed;
                total_stats.records_output += stats.records_output;
                total_stats.records_skipped += stats.records_skipped;
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
        eprintln!("  Records processed: {}", total_stats.records_processed);
        eprintln!("  Records output: {}", total_stats.records_output);
        eprintln!("  Records skipped: {}", total_stats.records_skipped);
        eprintln!("  Errors: {}", total_stats.errors);
        eprintln!("  Processing time: {:?}", total_stats.processing_time);

        if total_stats.records_processed > 0 {
            let rate =
                total_stats.records_processed as f64 / total_stats.processing_time.as_secs_f64();
            eprintln!("  Processing rate: {:.0} records/second", rate);
        }
    }

    Ok(())
}
