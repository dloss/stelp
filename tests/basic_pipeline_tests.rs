// tests/basic_pipeline_tests.rs
use std::io::Cursor;
use stelp::{
    config::PipelineConfig,
    processors::{FilterProcessor, StarlarkProcessor},
    StreamPipeline,
};

#[test]
fn test_basic_text_transformation() {
    println!("=== Testing Basic Pipeline: Text transformation ===");

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
fn test_basic_filter_functionality() {
    println!("=== Testing Basic Pipeline: Filter functionality ===");

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
fn test_basic_global_variables() {
    println!("=== Testing Basic Pipeline: Global variables ===");

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
fn debug_emit_and_skip() {
    use std::io::Cursor;
    use stelp::config::PipelineConfig;
    use stelp::context::{RecordContext, RecordData};
    use stelp::processors::StarlarkProcessor;
    use stelp::variables::GlobalVariables;
    use stelp::StreamPipeline;
    println!("=== Debug: What's actually happening ===");

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

    println!("Stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Raw output bytes: {:?}", output_str.as_bytes());
    println!("Output string: '{}'", output_str);
    println!("Output lines:");
    for (i, line) in output_str.lines().enumerate() {
        println!("  {}: '{}'", i, line);
    }

    // Let's test the processor directly too
    println!("\n=== Direct processor test ===");
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
if "emit" in line:
    emit("Found: " + line)
    skip()
else:
    line.upper()
        "#,
    )
    .unwrap();

    // Test normal line
    let record1 = RecordData::text("normal line".to_string());
    let result1 = processor.process_standalone(&record1, &ctx);
    println!("Normal line result: {:?}", result1);

    // Test emit line
    let record2 = RecordData::text("emit this".to_string());
    let result2 = processor.process_standalone(&record2, &ctx);
    println!("Emit line result: {:?}", result2);
}

#[test]
fn test_basic_control_flow() {
    println!("=== Testing Basic Pipeline: Control flow (emit, skip, exit) ===");

    // Test emit and skip
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
result = line  # Default to original line
if "emit" in line:
    emit("Found: " + line)
    skip()
else:
    result = line.upper()

result
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
    println!("Output: '{}'", output_str);

    assert!(output_str.contains("NORMAL LINE"));
    assert!(output_str.contains("Found: emit this"));
    assert!(output_str.contains("ANOTHER LINE"));
    println!("✓ Emit and skip work");

    // Test exit
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
result = line  # Default to original line
if "STOP" in line:
    exit("Stopped at: " + line)
else:
    result = line.upper()

result
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
    println!("Exit test output: '{}'", output_str);

    assert!(output_str.contains("HELLO"));
    assert!(output_str.contains("Stopped at: STOP here"));
    assert!(!output_str.contains("WORLD")); // Should not process this
    println!("✓ Exit works");

    println!("✅ All basic pipeline tests pass!");
}
