// tests/global_namespace_test.rs
use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::context::{RecordContext, RecordData};
use stelp::processors::StarlarkProcessor;
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_global_namespace_basic() {
    println!("=== Testing Global Namespace Basic Functions ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test basic global functions without st. prefix
    let processor = StarlarkProcessor::from_script("test", r#"line.upper()"#).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello world\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(String::from_utf8(output).unwrap(), "HELLO WORLD\n");
    println!("✅ Basic transformation works");
}

#[test]
fn test_meta_variables_alluppercase() {
    println!("=== Testing ALLUPPERCASE Meta Variables ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 42,
        record_count: 10,
        file_name: Some("test.txt"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "meta_test",
        r#"f"Line {LINENUM}, Record {RECNUM} in {FILENAME}: {line}""#,
    )
    .unwrap();

    let record = RecordData::text("hello world".to_string());
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert_eq!(text, "Line 42, Record 10 in test.txt: hello world");
                println!("✅ ALLUPPERCASE meta variables work: {}", text);
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_json_functions_global() {
    println!("=== Testing JSON Functions in Global Namespace ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "json_test",
        r#"
data = parse_json(line)
result = f"User: {data}"
# Explicit return
result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new(r#"{"name": "alice", "age": 30}"#);
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("User:"));
    println!("✅ JSON functions work: {}", output_str);
}

#[test]
fn test_meta_with_none_filename() {
    println!("=== Testing Meta Variables with None Filename ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "none_filename_test",
        r#"
filename = FILENAME if FILENAME else "<stdin>"
result = f"Line {LINENUM} from {filename}: {line}"
# Explicit return
result
        "#,
    )
    .unwrap();

    let record = RecordData::text("test".to_string());
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert_eq!(text, "Line 1 from <stdin>: test");
                println!("✅ Meta handles None filename: {}", text);
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_print_function_global() {
    println!("=== Testing Print Function in Global Namespace ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "print_test",
        r#"
print("Processing: " + line)
line.upper()
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(String::from_utf8(output).unwrap(), "HELLO\n");

    println!("✅ Print function works (output goes to stderr)");
}

#[test]
fn test_none_behavior_explicit() {
    println!("=== Testing None Behavior Explicitly ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test that expressions returning None produce no output
    let processor = StarlarkProcessor::from_script(
        "none_test",
        r#"
# This returns None - should produce no output
print("side effect: " + line)
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("test input\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 0); // No output from None
    assert_eq!(String::from_utf8(output).unwrap(), "");

    println!("✅ None expressions produce no output (correct behavior)");
}

// ADD: Test that explicit None also works
#[test]
fn test_explicit_none_vs_string_none() {
    println!("=== Testing Explicit None vs String 'None' ===");

    let config = PipelineConfig::default();
    let mut pipeline1 = StreamPipeline::new(config.clone());
    let mut pipeline2 = StreamPipeline::new(config);

    // Test explicit None
    let processor1 = StarlarkProcessor::from_script("explicit_none", r#"None"#).unwrap();
    pipeline1.add_processor(Box::new(processor1));

    // Test string "None"
    let processor2 = StarlarkProcessor::from_script("string_none", r#""None""#).unwrap();
    pipeline2.add_processor(Box::new(processor2));

    let input1 = Cursor::new("test\n");
    let input2 = Cursor::new("test\n");
    let mut output1 = Vec::new();
    let mut output2 = Vec::new();

    let stats1 = pipeline1
        .process_stream(input1, &mut output1, Some("test.txt"))
        .unwrap();
    let stats2 = pipeline2
        .process_stream(input2, &mut output2, Some("test.txt"))
        .unwrap();

    // Explicit None should produce no output
    assert_eq!(stats1.records_output, 0);
    assert_eq!(String::from_utf8(output1).unwrap(), "");

    // String "None" should produce "None"
    assert_eq!(stats2.records_output, 1);
    assert_eq!(String::from_utf8(output2).unwrap(), "None\n");

    println!("✅ None vs 'None' distinction works correctly");
}

// The real issue: In Starlark, if/elif/else must be EXPRESSIONS, not STATEMENTS
// The current code treats them as statements, which return None

#[test]
fn test_regex_functions_global() {
    println!("=== Testing Regex Functions in Global Namespace ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // FIXED: Use a conditional expression, not an if statement
    let processor = StarlarkProcessor::from_script(
        "regex_test",
        r#"
regex_replace(r"\d+", "NUMBER", line) if regex_match(r"\d+", line) else line
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
    println!("Debug regex output: '{}'", output_str);

    // Should have the correct transformations
    assert!(output_str.contains("hello NUMBER"));
    assert!(output_str.contains("world"));
    assert!(output_str.contains("test NUMBER"));
    println!("✅ Regex functions work: {}", output_str);
}

#[test]
fn test_emit_skip_exit_global() {
    println!("=== Testing Control Functions in Global Namespace ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // FIXED: Restructure to ensure each branch returns a value or has explicit control flow
    let processor = StarlarkProcessor::from_script(
        "control_test",
        r#"
# Use explicit variable assignment and return
result = None

if "emit" in line:
    emit("Found: " + line)
    result = line.upper()
elif "skip" in line:
    skip()
    # result stays None, but skip() will handle this
elif "exit" in line:
    exit("Stopping at: " + line)
    # result stays None, but exit() will handle this
else:
    result = line.upper()

# Return the result (None for skip/exit cases, string for others)
result
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nemit this\nskip this\nexit here\nworld\n");
    let mut output = Vec::new();

    let _stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    println!("Control test output: {}", output_str);

    // Should process until exit
    assert!(output_str.contains("HELLO"));
    assert!(output_str.contains("Found: emit this"));
    assert!(output_str.contains("EMIT THIS"));
    assert!(output_str.contains("Stopping at: exit here"));
    assert!(!output_str.contains("WORLD"));
    assert!(!output_str.contains("skip this"));

    println!("✅ Control functions work without prefix");
}
