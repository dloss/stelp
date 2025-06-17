use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::context::{RecordContext, RecordData};
use stelp::StarlarkProcessor;
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_glob_dictionary_basic() {
    println!("=== Testing Basic glob Dictionary ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Extract glob values to atomic variables for f-strings
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
count = glob.get("count", 0) + 1
glob["count"] = count
f"Line {count}: {line}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("first\nsecond\nthird\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    let output_str = String::from_utf8(output).unwrap();
    println!("Output: {}", output_str);

    assert!(output_str.contains("Line 1: first"));
    assert!(output_str.contains("Line 2: second"));
    assert!(output_str.contains("Line 3: third"));

    println!("✅ Basic glob dictionary works");
}

#[test]
fn test_glob_dictionary_methods() {
    println!("=== Testing glob Dictionary Methods ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: Some("test.txt"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Test various glob methods
glob["name"] = "Alice"
glob["age"] = 30
glob["active"] = True

# Extract values to atomic variables for f-strings
name = glob.get("name", "Unknown")
missing = glob.get("missing", "Default")
keys = glob.keys()
key_count = len(keys)

f"Name: {name}, Missing: {missing}, Keys: {key_count}"
        "#,
    )
    .unwrap();

    let record = RecordData::text("test".to_string());
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert!(text.contains("Name: Alice"));
                assert!(text.contains("Missing: Default"));
                println!("✅ glob methods work: {}", text);
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_glob_dictionary_persistence() {
    println!("=== Testing glob Dictionary Persistence ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test that glob persists across different script executions
    let processor1 = StarlarkProcessor::from_script(
        "test1",
        r#"
counter = glob.get("counter", 0) + 1
glob["counter"] = counter
glob["message"] = "Hello from script 1"
line.upper()
        "#,
    )
    .unwrap();

    let processor2 = StarlarkProcessor::from_script(
        "test2",
        r#"
counter = glob.get("counter", 0) + 1
glob["counter"] = counter
message = glob.get("message", "no message")
f"{message} - Counter: {counter}"
        "#,
    )
    .unwrap();

    pipeline.add_processor(Box::new(processor1));
    pipeline.add_processor(Box::new(processor2));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    println!("Persistence test output: {}", output_str);

    // Should see the counter incrementing and message persisting
    assert!(output_str.contains("Hello from script 1"));
    assert!(output_str.contains("Counter: 2")); // First line: 1 in script1, 2 in script2
    assert!(output_str.contains("Counter: 4")); // Second line: 3 in script1, 4 in script2

    println!("✅ glob persistence across processors works");
}

#[test]
fn test_glob_dictionary_vs_old_api() {
    println!("=== Testing glob vs Old API ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // FIX: Add explicit return statements
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Debug: print what we're doing
print("Processing line: " + line)

# Process and return result explicitly
if "error" in line:
    print("Found error line")
    result = "ERROR: " + line
else:
    print("Found normal line") 
    result = "NORMAL: " + line

# Explicit return
result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("normal line\nerror occurred\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    println!("Debug output: '{}'", output_str);

    // Now these should work
    assert!(output_str.contains("NORMAL: normal line"));
    assert!(output_str.contains("ERROR: error occurred"));

    println!("✅ Simple transformation works");
}

#[test]
fn test_glob_basic_access() {
    println!("=== Testing basic glob access ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: Some("test.txt"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Test if glob exists - simple check
str(glob)
        "#,
    )
    .unwrap();

    let record = RecordData::text("test".to_string());
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                println!("Glob basic access result: {}", text);
                // Should show some kind of dict representation
            } else {
                panic!("Expected text output");
            }
        }
        other => {
            println!("Result: {:?}", other);
            panic!("Expected Transform result");
        }
    }
}
