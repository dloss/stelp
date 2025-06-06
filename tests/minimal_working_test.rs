// tests/minimal_working_test.rs
use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::processors::StarlarkProcessor;
use stelp::StreamPipeline;

#[test]
fn test_basic_text_transformation() {
    println!("=== Testing Basic Text transformation ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello world\n");
    let mut output = Vec::new();

    // Just test that it runs without error
    if let Ok(_stats) = pipeline.process_stream(input, &mut output, Some("test.txt")) {
        // Check that some output was produced
        let output_str = String::from_utf8(output).unwrap();
        assert!(!output_str.is_empty(), "Expected some output");
        println!("✓ Basic text transformation test passed");
    } else {
        panic!("Pipeline processing failed");
    }
}

#[test]
fn test_global_variables() {
    println!("=== Testing Global Variables ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
count = glob.get("counter", 0) + 1
glob["counter"] = count
f"Line {count}: {line}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    // Just test that it runs without error
    if let Ok(_stats) = pipeline.process_stream(input, &mut output, Some("test.txt")) {
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("Line 1"));
        assert!(output_str.contains("Line 2"));
        println!("✓ Global variables test passed");
    } else {
        panic!("Pipeline processing failed");
    }
}

#[test]
fn test_emit_and_skip() {
    println!("=== Testing Emit and Skip ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
result = line.upper()
if "emit" in line:
    emit("Found: " + line)
elif "skip" in line:
    skip()
result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("normal\nemit this\nskip this\nnormal2\n");
    let mut output = Vec::new();

    match pipeline.process_stream(input, &mut output, Some("test.txt")) {
        Ok(_stats) => {
            let output_str = String::from_utf8(output).unwrap();
            assert!(output_str.contains("NORMAL"));
            assert!(output_str.contains("Found: emit this"));
            assert!(!output_str.contains("skip this"));
            println!("✓ Emit and skip test passed");
        }
        Err(e) => {
            panic!("Pipeline processing failed: {}", e);
        }
    }
}
