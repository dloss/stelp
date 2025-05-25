use clap::{Parser, ArgAction};
use std::io::{self, BufReader, BufRead, Write};
use std::fs::File;
use std::path::PathBuf;

use starproc::{
    StreamPipeline, StarlarkProcessor, PipelineConfig, ErrorStrategy,
    ProcessingError, CompilationError
};

#[derive(Parser)]
#[command(name = "starproc")]
#[command(about = "Process text streams with Starlark scripts")]
#[command(version = "0.1.0")]
struct Args {
    /// Pipeline steps (executed in order)
    #[arg(value_name = "EXPRESSION")]
    steps: Vec<String>,
    
    /// Additional pipeline steps
    #[arg(short = 's', long = "step", action = ArgAction::Append)]
    extra_steps: Vec<String>,
    
    /// Script file containing pipeline definition
    #[arg(short = 'f', long = "file")]
    pipeline_file: Option<PathBuf>,
    
    /// Debug mode - show processing details
    #[arg(long)]
    debug: bool,
    
    /// Fail on first error instead of skipping lines
    #[arg(long)]
    fail_fast: bool,
    
    /// Show progress every N lines
    #[arg(long, value_name = "N")]
    progress: Option<usize>,
    
    /// Maximum line length
    #[arg(long, default_value = "1048576")] // 1MB
    max_line_length: usize,
    
    /// Buffer size for I/O
    #[arg(long, default_value = "65536")] // 64KB
    buffer_size: usize,
    
    /// Input file (default: stdin)
    #[arg(short = 'i', long = "input")]
    input_file: Option<PathBuf>,
    
    /// Output file (default: stdout)
    #[arg(short = 'o', long = "output")]
    output_file: Option<PathBuf>,
}

impl Args {
    fn validate(&self) -> Result<(), String> {
        let has_file = self.pipeline_file.is_some();
        let has_steps = !self.steps.is_empty() || !self.extra_steps.is_empty();
        
        match (has_file, has_steps) {
            (true, true) => Err("Cannot use both --file and expression arguments".to_string()),
            (true, false) => Ok(()), // File only
            (false, true) => Ok(()),  // CLI expressions only  
            (false, false) => Err("Must provide either --file or expression arguments".to_string()),
        }
    }
    
    fn get_all_steps(&self) -> Vec<String> {
        let mut all_steps = self.steps.clone();
        all_steps.extend(self.extra_steps.clone());
        all_steps
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
    // Create pipeline configuration
    let config = PipelineConfig {
        error_strategy: if args.fail_fast { 
            ErrorStrategy::FailFast 
        } else { 
            ErrorStrategy::Skip 
        },
        debug: args.debug,
        buffer_size: args.buffer_size,
        max_line_length: args.max_line_length,
        progress_interval: args.progress.unwrap_or(0),
    };
    
    // Create pipeline
    let mut pipeline = StreamPipeline::new(config);
    
    // Add processors based on input
    if let Some(file_path) = args.pipeline_file {
        // Load from file
        let script_content = std::fs::read_to_string(&file_path)
            .map_err(|e| format!("Failed to read pipeline file '{}': {}", file_path.display(), e))?;
        
        let processor = StarlarkProcessor::from_script(
            &format!("file:{}", file_path.display()),
            &script_content
        ).map_err(|e| format!("Failed to compile pipeline file: {}", e))?;
        
        pipeline.add_processor(Box::new(processor));
    } else {
        // Add processors from CLI arguments
        let all_steps = args.get_all_steps();
        for (i, step) in all_steps.iter().enumerate() {
            let processor = StarlarkProcessor::from_script(
                &format!("step_{}", i + 1),
                step
            ).map_err(|e| format!("Failed to compile step {}: {}", i + 1, e))?;
            
            pipeline.add_processor(Box::new(processor));
        }
    }
    
    // Set up input
    let input_filename = args.input_file.as_ref().map(|p| p.to_string_lossy().as_ref());
    let input: Box<dyn BufRead> = if let Some(input_path) = &args.input_file {
        let file = File::open(input_path)
            .map_err(|e| format!("Failed to open input file '{}': {}", input_path.display(), e))?;
        Box::new(BufReader::with_capacity(args.buffer_size, file))
    } else {
        Box::new(BufReader::with_capacity(args.buffer_size, io::stdin()))
    };
    
    // Set up output
    let mut output: Box<dyn Write> = if let Some(output_path) = &args.output_file {
        let file = File::create(output_path)
            .map_err(|e| format!("Failed to create output file '{}': {}", output_path.display(), e))?;
        Box::new(io::BufWriter::with_capacity(args.buffer_size, file))
    } else {
        Box::new(io::BufWriter::with_capacity(args.buffer_size, io::stdout()))
    };
    
    // Process the stream
    let stats = pipeline.process_stream(
        input,
        &mut output,
        input_filename
    ).map_err(|e| format!("Processing failed: {}", e))?;
    
    // Ensure output is flushed
    output.flush()?;
    
    // Print final stats if debug mode
    if args.debug {
        eprintln!("Final statistics:");
        eprintln!("  Lines processed: {}", stats.lines_processed);
        eprintln!("  Lines output: {}", stats.lines_output);
        eprintln!("  Lines skipped: {}", stats.lines_skipped);
        eprintln!("  Errors: {}", stats.errors);
        eprintln!("  Processing time: {:?}", stats.processing_time);
        
        if stats.lines_processed > 0 {
            let rate = stats.lines_processed as f64 / stats.processing_time.as_secs_f64();
            eprintln!("  Processing rate: {:.0} lines/second", rate);
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_simple_transform() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", "line.upper()").unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new("hello\nworld\n");
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 2);
        assert_eq!(stats.lines_output, 2);
        assert_eq!(String::from_utf8(output).unwrap(), "HELLO\nWORLD\n");
    }
    
    #[test]
    fn test_emit_and_skip() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", r#"
fields = line.split(",")
for field in fields:
    emit(field.upper())
skip()
        "#).unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new("hello,world\nfoo,bar\n");
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 2);
        assert_eq!(stats.lines_output, 4);
        assert_eq!(stats.lines_skipped, 2);
        assert_eq!(String::from_utf8(output).unwrap(), "HELLO\nWORLD\nFOO\nBAR\n");
    }
    
    #[test]
    fn test_global_variables() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", r#"
count = get_global("count", 0) + 1
set_global("count", count)
f"Line {count}: {line}"
        "#).unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new("hello\nworld\n");
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 2);
        assert_eq!(stats.lines_output, 2);
        assert_eq!(String::from_utf8(output).unwrap(), "Line 1: hello\nLine 2: world\n");
    }
    
    #[test]
    fn test_terminate() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", r#"
if "STOP" in line:
    terminate(f"Stopped at: {line}")
else:
    line.upper()
        "#).unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new("hello\nSTOP here\nworld\n");
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 2); // Only processed until STOP
        assert_eq!(stats.lines_output, 2);
        assert_eq!(String::from_utf8(output).unwrap(), "HELLO\nStopped at: STOP here\n");
    }
    
    #[test]
    fn test_regex_functions() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", r#"
if regex_match(r"\d+", line):
    regex_replace(r"(\d+)", r"NUMBER(\1)", line)
else:
    line
        "#).unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new("hello 123\nworld\ntest 456\n");
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 3);
        assert_eq!(stats.lines_output, 3);
        assert_eq!(String::from_utf8(output).unwrap(), "hello NUMBER(123)\nworld\ntest NUMBER(456)\n");
    }
    
    #[test]
    fn test_json_parsing() {
        let config = PipelineConfig::default();
        let mut pipeline = StreamPipeline::new(config);
        
        let processor = StarlarkProcessor::from_script("test", r#"
try:
    data = parse_json(line)
    data["name"] + ": " + str(data["value"])
except:
    line
        "#).unwrap();
        pipeline.add_processor(Box::new(processor));
        
        let input = Cursor::new(r#"{"name": "test", "value": 42}
invalid json
{"name": "hello", "value": 123}
"#);
        let mut output = Vec::new();
        
        let stats = pipeline.process_stream(input, &mut output, None).unwrap();
        
        assert_eq!(stats.lines_processed, 3);
        assert_eq!(stats.lines_output, 3);
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("test: 42"));
        assert!(output_str.contains("invalid json"));
        assert!(output_str.contains("hello: 123"));
    }
}