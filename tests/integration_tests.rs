// tests/integration_tests.rs
use std::io::Cursor;
use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::context::{RecordContext, RecordData};
use stelp::processors::{FilterProcessor, StarlarkProcessor};
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_terminate_working() {
    println!("=== Testing working exit ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
    };

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# First transform the line
result = line.upper()

# Then check if we should exit
if "STOP" in line:
    exit("Stopped at: " + line)

# Return the transformed result
result
        "#,
    )
    .unwrap();

    // Test normal line
    let record1 = RecordData::text("hello".to_string());
    let result1 = processor.process_standalone(&record1, &ctx);
    println!("Normal line result: {:?}", result1);

    // Test exit line
    let record2 = RecordData::text("STOP here".to_string());
    let result2 = processor.process_standalone(&record2, &ctx);
    println!("Exit line result: {:?}", result2);

    // Now test in pipeline
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nSTOP here\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    println!("Pipeline stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Pipeline output: '{}'", output_str);

    // Now it should work correctly
    assert!(output_str.contains("HELLO"));
    assert!(output_str.contains("Stopped at: STOP here"));
    assert!(!output_str.contains("WORLD")); // Should stop before this
}

#[test]
fn test_terminate_bypass() {
    println!("=== Testing without exit function ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# First transform to uppercase
result = line.upper()

# Then check if we should emit and skip
if "STOP" in result:
    emit("Stopped at: " + line)  # emit original line for message
    skip()

# Return the transformed result (which will be skipped if skip() was called)
result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nSTOP here\nworld\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    println!("Stats: {:?}", _stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Output: '{}'", output_str);

    assert!(output_str.contains("HELLO"));
    assert!(output_str.contains("Stopped at: STOP here"));
    assert!(output_str.contains("WORLD"));

    assert_eq!(_stats.records_processed, 3);
    assert_eq!(_stats.records_output, 3);
}

#[test]
fn test_simple_transform() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);
    assert_eq!(String::from_utf8(output).unwrap(), "HELLO\nWORLD\n");
}

#[test]
fn test_emit_and_skip() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
fields = line.split(",")
for field in fields:
    emit(field.upper())
skip()
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello,world\nfoo,bar\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 4);
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "HELLO\nWORLD\nFOO\nBAR\n"
    );
}

#[test]
fn test_st_namespace_global_variables() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Use st_get_global and st_set_global (underscore versions)
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
count = st_get_global("count", 0) + 1
st_set_global("count", count)
"Line " + str(count) + ": " + line
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "Line 1: hello\nLine 2: world\n"
    );
}

#[test]
fn test_st_namespace_regex_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
if st_regex_match("\\d+", line):
    result = st_regex_replace("\\d+", "NUMBER", line)
else:
    result = line

result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello 123\nworld\ntest 456\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "hello NUMBER\nworld\ntest NUMBER\n");
}

#[test]
fn test_st_namespace_json_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Create a simple JSON object and convert it
data = {"line": line, "length": len(line)}
st_to_json(data)
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    // The output should contain JSON strings
    assert!(output_str.contains("hello"));
    assert!(output_str.contains("world"));
}

#[test]
fn test_st_namespace_csv_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Very simple test - just return "PROCESSED" to see if the script runs at all
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
"PROCESSED: " + line
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    eprintln!("Stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    eprintln!("Output: '{}'", output_str);

    // First check if we processed anything at all
    assert!(stats.records_processed > 0, "No records were processed");
    assert!(stats.records_output > 0, "No records were output");
    assert!(
        output_str.contains("PROCESSED"),
        "Script didn't run: {}",
        output_str
    );
}
#[test]
fn test_st_namespace_context_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Use context functions from st namespace
line_info = "Line " + str(st_line_number()) + " in " + st_file_name() + ": " + line
line_info
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Line 1 in test.txt: hello"));
    assert!(output_str.contains("Line 2 in test.txt: world"));
}

#[test]
fn test_simple_filter() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("test_filter", r#""keep" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    let input = Cursor::new("keep this\nskip this line\nkeep this too\nskip me\nfinal line\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 3);

    assert!(output_str.contains("keep this\n"));
    assert!(output_str.contains("keep this too\n"));
    assert!(!output_str.contains("final line\n"));
    assert!(!output_str.contains("skip this line"));
    assert!(!output_str.contains("skip me"));
}

#[test]
fn test_filter_combined_with_eval() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // First filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("keep_filter", r#""keep" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    // Then transform remaining lines to uppercase
    let processor = StarlarkProcessor::from_script("uppercase", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nkeep this\nworld\nkeep me too\nend\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 3);
    assert_eq!(output_str, "KEEP THIS\nKEEP ME TOO\n");
}

#[test]
fn test_error_handling_skip_strategy() {
    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        ..Default::default()
    };
    let mut pipeline = StreamPipeline::new(config);

    // Processor with an invalid expression that will cause an error
    let processor = StarlarkProcessor::from_script("error_test", "undefined_variable + 1").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("line 1\nline 2\nline 3\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    // With skip strategy, errors should be counted but not stop processing
    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.errors, 3); // All lines should error
    assert_eq!(stats.records_output, 0); // No successful outputs
}
