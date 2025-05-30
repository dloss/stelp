use std::io::Cursor;
use stelp::processors::{FilterProcessor, StarlarkProcessor};
use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::variables::GlobalVariables;
use stelp::context::LineContext;
use stelp::StreamPipeline;

#[test]
fn test_terminate_working() {
    println!("=== Testing working terminate ===");

    let globals = GlobalVariables::new();
    let ctx = LineContext {
        line_number: 1,
        file_name: None,
        global_vars: &globals,
    };

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# First transform the line
result = line.upper()

# Then check if we should terminate
if "STOP" in line:
    terminate("Stopped at: " + line)

# Return the transformed result
result
        "#,
    )
    .unwrap();

    // Test normal line
    let result1 = processor.process_standalone("hello", &ctx);
    println!("Normal line result: {:?}", result1);

    // Test terminate line
    let result2 = processor.process_standalone("STOP here", &ctx);
    println!("Terminate line result: {:?}", result2);

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
    println!("=== Testing without terminate function ===");

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

    assert_eq!(_stats.lines_processed, 3);
    assert_eq!(_stats.lines_output, 3);
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

    assert_eq!(stats.lines_processed, 2);
    assert_eq!(stats.lines_output, 2);
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

    assert_eq!(stats.lines_processed, 2);
    assert_eq!(stats.lines_output, 4);
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "HELLO\nWORLD\nFOO\nBAR\n"
    );
}

#[test]
fn test_st_namespace_global_variables() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Use st.get_global and st.set_global with string concatenation
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
count = st.get_global("count", 0) + 1
st.set_global("count", count)
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

    assert_eq!(stats.lines_processed, 2);
    assert_eq!(stats.lines_output, 2);
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
result = ""
if st.regex_match("\\d+", line):
    result = st.regex_replace("\\d+", "NUMBER", line)
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

    assert_eq!(stats.lines_processed, 3);
    assert_eq!(stats.lines_output, 3);

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
st.to_json(data)
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.lines_processed, 2);
    assert_eq!(stats.lines_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    // The output should contain JSON strings
    assert!(output_str.contains("hello"));
    assert!(output_str.contains("world"));
}

#[test]
fn test_st_namespace_csv_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Parse CSV and reconstruct with modified data
fields = st.parse_csv(line)
result = ""
if len(fields) >= 2:
    new_fields = [fields[0].upper(), fields[1] + "_modified"]
    result = st.to_csv(new_fields)
else:
    result = line

result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\nbob,data2\nincomplete\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.lines_processed, 3);
    assert_eq!(stats.lines_output, 3);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("ALICE,data1_modified"));
    assert!(output_str.contains("BOB,data2_modified"));
    assert!(output_str.contains("incomplete")); // Unchanged line
}

#[test]
fn test_st_namespace_context_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Use context functions from st namespace - avoid str() on file_name
line_info = "Line " + str(st.line_number()) + " in " + st.file_name() + ": " + line
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

    assert_eq!(stats.lines_processed, 2);
    assert_eq!(stats.lines_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Line 1 in test.txt: hello"));
    assert!(output_str.contains("Line 2 in test.txt: world"));
}

#[test]
fn test_simple_filter() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("test_filter", r#""skip" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    let input = Cursor::new("keep this\nskip this line\nkeep this too\nskip me\nfinal line\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.lines_processed, 5);
    assert_eq!(stats.lines_output, 3);
    assert_eq!(stats.lines_skipped, 2);

    assert!(output_str.contains("keep this\n"));
    assert!(output_str.contains("keep this too\n"));
    assert!(output_str.contains("final line\n"));
    assert!(!output_str.contains("skip this line"));
    assert!(!output_str.contains("skip me"));
}

#[test]
fn test_filter_combined_with_eval() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // First filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("skip_filter", r#""skip" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    // Then transform remaining lines to uppercase
    let processor = StarlarkProcessor::from_script("uppercase", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nskip this\nworld\nskip me too\nend\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.lines_processed, 5);
    assert_eq!(stats.lines_output, 3);
    assert_eq!(stats.lines_skipped, 2);
    assert_eq!(output_str, "HELLO\nWORLD\nEND\n");
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
    assert_eq!(stats.lines_processed, 3);
    assert_eq!(stats.errors, 3); // All lines should error
    assert_eq!(stats.lines_output, 0); // No successful outputs
}