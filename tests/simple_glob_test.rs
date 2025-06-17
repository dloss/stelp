// tests/simple_glob_test.rs
use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::StarlarkProcessor;
use stelp::StreamPipeline;

#[test]
fn test_simple_glob_dictionary() {
    println!("=== Testing Simple glob Dictionary ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test basic glob dictionary usage with standard dict syntax
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Use glob dictionary like a normal dictionary
if "count" in glob:
    glob["count"] = glob["count"] + 1
else:
    glob["count"] = 1
count = glob['count']
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
    println!("Output: {}", output_str);

    // Should increment the counter properly
    assert!(output_str.contains("Line 1: first"));
    assert!(output_str.contains("Line 2: second"));

    println!("✅ Simple glob dictionary works");
}

#[test]
fn test_glob_get_method() {
    println!("=== Testing glob.get() method ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Test .get() method with default values
count = glob.get("count", 0) + 1
glob["count"] = count

missing = glob.get("missing_key", "default_value")

f"Count: {count}, Missing: {missing}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("test\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);

    let output_str = String::from_utf8(output).unwrap();
    println!("Output: {}", output_str);

    assert!(output_str.contains("Count: 1"));
    assert!(output_str.contains("Missing: default_value"));

    println!("✅ glob.get() method works");
}
