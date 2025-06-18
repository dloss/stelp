// tests/window_tests.rs
use std::io::Cursor;
use stelp::{config::PipelineConfig, StarlarkProcessor, StreamPipeline, WindowProcessor};

#[test]
fn test_basic_window_functionality() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Create processor that uses window for change detection
    let inner = StarlarkProcessor::from_script(
        "window_test",
        r#"current = int(line)
size = window_size()
prev = int(window[-2]["line"]) if size >= 2 else current
change = current - prev
f"Value: {current}, Change: {change}""#,
    )
    .unwrap();

    let window_processor = WindowProcessor::new(3, Box::new(inner));
    pipeline.add_processor(Box::new(window_processor));

    let input = Cursor::new("10\n15\n12\n20\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 4);
    assert_eq!(stats.records_output, 4);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Value: 10, Change: 0"));
    assert!(output_str.contains("Value: 15, Change: 5"));
    assert!(output_str.contains("Value: 12, Change: -3"));
    assert!(output_str.contains("Value: 20, Change: 8"));
}

#[test]
fn test_window_helper_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let inner = StarlarkProcessor::from_script(
        "helper_test",
        r#"values = window_values("line")
count = len(values)
f"Current: {line}, Window count: {count}""#,
    )
    .unwrap();

    let window_processor = WindowProcessor::new(3, Box::new(inner));
    pipeline.add_processor(Box::new(window_processor));

    let input = Cursor::new("10\n20\n30\n40\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Current: 10, Window count: 1"));
    assert!(output_str.contains("Current: 20, Window count: 2"));
    assert!(output_str.contains("Current: 30, Window count: 3"));
    assert!(output_str.contains("Current: 40, Window count: 3"));
}

#[test]
fn test_window_with_structured_data() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Single processor that shows window functionality with simple line number
    let inner = StarlarkProcessor::from_script(
        "window_structured",
        r#"# Simple demonstration of window functionality
line_num = LINENUM
window_count = window_size()
f"Line: {line_num}, Window size: {window_count}""#,
    )
    .unwrap();

    let window_processor = WindowProcessor::new(3, Box::new(inner));
    pipeline.add_processor(Box::new(window_processor));

    let input = Cursor::new(
        r#"{"price": 10.5}
{"price": 12.0}
{"price": 11.25}
{"price": 13.5}
"#,
    );
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Line: 1, Window size: 1"));
    assert!(output_str.contains("Line: 2, Window size: 2"));
    assert!(output_str.contains("Line: 3, Window size: 3"));
    assert!(output_str.contains("Line: 4, Window size: 3"));
}

#[test]
fn test_window_size_limitation() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let inner = StarlarkProcessor::from_script(
        "size_test",
        r#"size = window_size()
f"Window size: {size}, Current: {line}""#,
    )
    .unwrap();

    let window_processor = WindowProcessor::new(2, Box::new(inner)); // Window size 2
    pipeline.add_processor(Box::new(window_processor));

    let input = Cursor::new("1\n2\n3\n4\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    assert_eq!(lines.len(), 4);
    assert!(lines[0].contains("Window size: 1")); // First record
    assert!(lines[1].contains("Window size: 2")); // Second record
    assert!(lines[2].contains("Window size: 2")); // Third record (limited by window size)
    assert!(lines[3].contains("Window size: 2")); // Fourth record (limited by window size)
}

#[test]
fn test_window_access_metadata() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let inner = StarlarkProcessor::from_script(
        "metadata_test",
        r#"size = len(window)
if size > 1:
    prev_record = window[-2]
    prev_line_num = prev_record["line_number"]
    result = f"Current line {LINENUM}, Previous was line {prev_line_num}"
else:
    result = f"Current line {LINENUM}, No previous"
result"#,
    )
    .unwrap();

    let window_processor = WindowProcessor::new(3, Box::new(inner));
    pipeline.add_processor(Box::new(window_processor));

    let input = Cursor::new("first\nsecond\nthird\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Current line 1, No previous"));
    assert!(output_str.contains("Current line 2, Previous was line 1"));
    assert!(output_str.contains("Current line 3, Previous was line 2"));
}

#[test]
fn test_window_without_context() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test a regular processor without window context
    let processor = StarlarkProcessor::from_script(
        "no_window_test",
        r#"size = window_size()
values = window_values("line")
count = len(values)
f"Size: {size}, Values: {count}""#,
    )
    .unwrap();

    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("test\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Size: 0, Values: 0"));
}
