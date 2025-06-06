use std::io::Cursor;
use std::io::Write;
use stelp::config::PipelineConfig;
use stelp::processors::StarlarkProcessor;
use stelp::StreamPipeline;
use tempfile::NamedTempFile;

/// Build the final script by concatenating includes and user script
/// (This simulates what main.rs does)
fn build_script_with_includes(
    includes: &[std::path::PathBuf],
    user_script: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut final_script = String::new();

    // Add includes in order
    for include_path in includes {
        let include_content = std::fs::read_to_string(include_path)
            .map_err(|e| format!("Include file '{}' not found: {}", include_path.display(), e))?;
        final_script.push_str(&include_content);
        final_script.push_str("\n\n");
    }

    // Add user script
    final_script.push_str(user_script);

    Ok(final_script)
}

#[test]
fn test_basic_include() {
    // Create a temporary include file
    let mut include_file = NamedTempFile::new().unwrap();
    writeln!(
        include_file,
        r#"
def greet(name):
    return "Hello, " + name + "!"
"#
    )
    .unwrap();

    // Build final script with include
    let includes = vec![include_file.path().to_path_buf()];
    let final_script = build_script_with_includes(&includes, "greet(line)").unwrap();

    // Test the combined script
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("World");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);

    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "Hello, World!\n");
}

#[test]
fn test_multiple_includes_override() {
    // Create first include file
    let mut include1 = NamedTempFile::new().unwrap();
    writeln!(
        include1,
        r#"
def process(text):
    return "v1: " + text

VALUE = "original"
"#
    )
    .unwrap();

    // Create second include file that overrides
    let mut include2 = NamedTempFile::new().unwrap();
    writeln!(
        include2,
        r#"
def process(text):
    return "v2: " + text

VALUE = "overridden"
"#
    )
    .unwrap();

    // Build final script with both includes
    let includes = vec![include1.path().to_path_buf(), include2.path().to_path_buf()];
    let final_script =
        build_script_with_includes(&includes, r#"process(line) + " (" + VALUE + ")""#).unwrap();

    // Test the combined script
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("test");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);

    let output_str = String::from_utf8(output).unwrap();
    // Should use the second (overriding) definitions
    assert_eq!(output_str, "v2: test (overridden)\n");
}

#[test]
fn test_include_with_starlark_functions() {
    // Create include file that uses global functions (NEW API)
    let mut include_file = NamedTempFile::new().unwrap();
    writeln!(
        include_file,
        r#"
def count_lines():
    count = glob.get("line_count", 0) + 1
    glob["line_count"] = count
    return count

def format_line(text):
    line_num = count_lines()
    return "Line " + str(line_num) + ": " + text
"#
    )
    .unwrap();

    // Build final script
    let includes = vec![include_file.path().to_path_buf()];
    let final_script = build_script_with_includes(&includes, "format_line(line)").unwrap();

    // Test the combined script
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "Line 1: hello\nLine 2: world\n");
}

#[test]
fn test_empty_includes() {
    // Test with no includes - should just use user script
    let includes = vec![];
    let final_script = build_script_with_includes(&includes, "line.upper()").unwrap();
    assert_eq!(final_script, "line.upper()");

    // Test that it works in pipeline
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script("test", &final_script).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(String::from_utf8(output).unwrap(), "HELLO\n");
}

#[test]
fn test_include_file_not_found_error() {
    // Try to build a script with a non-existent include
    let includes = vec![std::path::PathBuf::from("non_existent_file.star")];
    let result = build_script_with_includes(&includes, "line.upper()");

    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("Include file 'non_existent_file.star' not found"));
}
