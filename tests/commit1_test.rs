// tests/commit1_test.rs
use std::io::Cursor;
use stelp::{
    config::{ErrorStrategy, PipelineConfig},
    context::{ProcessResult, RecordContext, RecordData},
    processors::{FilterProcessor, StarlarkProcessor},
    variables::GlobalVariables,
    StreamPipeline,
};

#[test]
fn test_commit1_basic_functionality() {
    println!("=== Testing Commit 1: Basic record architecture ===");

    // Test 1: Simple text transformation works
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
    println!("✓ Basic text transformation works");
}

#[test]
fn test_commit1_filter_functionality() {
    println!("=== Testing Commit 1: Filter functionality ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let filter = FilterProcessor::from_expression("test_filter", r#""skip" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    let input = Cursor::new("keep this\nskip this\nkeep this too\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 1);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("keep this\n"));
    assert!(output_str.contains("keep this too\n"));
    assert!(!output_str.contains("skip this"));
    println!("✓ Filter works");
}

#[test]
fn test_commit1_global_variables() {
    println!("=== Testing Commit 1: Global variables ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
count = st_get_global("count", 0) + 1
st_set_global("count", count)
f"Line {count}: {line}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("first\nsecond\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Line 1: first"));
    assert!(output_str.contains("Line 2: second"));
    println!("✓ Global variables work");
}

#[test]
fn test_commit1_control_flow() {
    println!("=== Testing Commit 1: Control flow (emit, skip, terminate) ===");

    // Test emit and skip
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
if "emit" in line:
    emit("Found: " + line)
    skip()
else:
    line.upper()
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("normal line\nemit this\nanother line\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3); // 2 transforms + 1 emit

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("NORMAL LINE"));
    assert!(output_str.contains("Found: emit this"));
    assert!(output_str.contains("ANOTHER LINE"));
    println!("✓ Emit and skip work");

    // Test terminate
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
if "STOP" in line:
    terminate("Stopped at: " + line)
else:
    line.upper()
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nSTOP here\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2); // Only processes until STOP

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("HELLO"));
    assert!(output_str.contains("Stopped at: STOP here"));
    assert!(!output_str.contains("WORLD")); // Should not process this
    println!("✓ Terminate works");

    println!("✅ All Commit 1 tests pass!");
}
