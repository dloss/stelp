// tests/test_prelude.rs - Test the prelude functions directly

use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::processors::StarlarkProcessor;
use stelp::StreamPipeline;

/// Test that we can load the prelude file directly for syntax validation
#[test]
fn test_prelude_syntax() {
    println!("=== Testing prelude.star syntax ===");
    
    // Read the prelude file directly (same as include_str! would read)
    let prelude_content = include_str!("../src/prelude.star");
    
    // Should be able to create a processor with just the prelude
    let result = StarlarkProcessor::from_script("prelude_test", prelude_content);
    assert!(result.is_ok(), "Prelude should have valid Starlark syntax");
    
    println!("✅ Prelude syntax is valid");
}

#[test]
fn test_inc_function_from_prelude() {
    println!("=== Testing inc() from embedded prelude ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Build script with prelude (like main.rs does)
    let prelude = include_str!("../src/prelude.star");
    let user_script = r#"
count = inc("lines")
f"Line {count}: {line}"
        "#;
    let final_script = format!("{}\n\n{}", prelude, user_script);
    
    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("first\nsecond\nthird\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    println!("Stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Output: '{}'", output_str);

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    assert!(output_str.contains("Line 1: first"));
    assert!(output_str.contains("Line 2: second"));
    assert!(output_str.contains("Line 3: third"));

    println!("✅ inc() function works from prelude");
}

#[test]
fn test_all_prelude_functions() {
    println!("=== Testing all prelude functions ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test script that uses multiple prelude functions
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Test inc() and dec()
up = inc("up_counter")
down = dec("down_counter", 2)

# Test get_or_set()
default_val = get_or_set("default", "initial")

# Test toggle()
toggled = toggle("flag")

# Test max_counter() and min_counter()
line_len = len(line)
max_len = max_counter("max_length", line_len)
min_len = min_counter("min_length", line_len)

# Test reset_counter() on first line
if LINENUM == 1:
    reset_val = reset_counter("reset_test")
else:
    reset_val = glob.get("reset_test", 0)

f"Line {up}: len={line_len}, max={max_len}, min={min_len}, toggle={toggled}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("short\nmedium line\nvery long line here\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    println!("Stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Output: '{}'", output_str);

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    // Should have increasing line numbers
    assert!(output_str.contains("Line 1:"));
    assert!(output_str.contains("Line 2:"));
    assert!(output_str.contains("Line 3:"));

    println!("✅ inc() function works correctly");
}

#[test]
fn test_counter_summary() {
    println!("=== Testing counter_summary() function ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Build script with prelude
    let prelude = include_str!("../src/prelude.star");
    let user_script = r#"
# Create various types of counters
total = inc("total")
errors = inc("error_count") 
warnings = inc("warning_counter")
processed = inc("processed")

# On last line, show summary
if LINENUM == 3:
    summary = counter_summary()
    keys = sorted(summary.keys())
    key_count = len(keys)
    f"Summary: {key_count} counters"
else:
    f"Line {total}"
        "#;
    let final_script = format!("{}\n\n{}", prelude, user_script);
    
    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("a\nb\nc\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    println!("Counter summary stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    println!("Counter summary output: '{}'", output_str);

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    assert!(output_str.contains("Line 1"));
    assert!(output_str.contains("Line 2"));
    assert!(output_str.contains("Summary: 4 counters")); // Should find all 4 counter-like keys

    println!("✅ counter_summary() works");
}

#[test]
fn test_prelude_with_includes() {
    println!("=== Testing prelude + includes work together ===");
    
    // Create a temporary include file
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    let mut include_file = NamedTempFile::new().unwrap();
    let include_content = r#"
def process_line(text):
    count = inc("processed_lines")
    return f"Processed #{count}: {text}"
"#;
    
    include_file.write_all(include_content.as_bytes()).unwrap();

    // Test that includes + prelude work together
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Simulate what main.rs does with build_final_script
    let includes = vec![include_file.path().to_path_buf()];
    let user_script = "process_line(line)";
    
    // Build script manually here (in real code, this happens in main.rs)
    let prelude = include_str!("../src/prelude.star");
    let include_content = std::fs::read_to_string(include_file.path()).unwrap();
    let final_script = format!("{}\n\n{}\n\n{}", prelude, include_content, user_script);

    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    println!("Output: {}", output_str);

    assert!(output_str.contains("Processed #1: hello"));
    assert!(output_str.contains("Processed #2: world"));

    println!("✅ Prelude + includes work together");
}