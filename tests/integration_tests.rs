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
if len(fields) >= 2:
    new_fields = [fields[0].upper(), fields[1] + "_modified"]
    st.to_csv(new_fields)
else:
    line
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

#[test]
fn csv_final_debug() {
    use std::io::Cursor;
    use stelp::processors::StarlarkProcessor;
    use stelp::config::PipelineConfig;
    use stelp::StreamPipeline;

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Parse CSV and reconstruct with modified data
fields = st.parse_csv(line)
if len(fields) >= 2:
    new_fields = [fields[0].upper(), fields[1] + "_modified"]
    result = st.to_csv(new_fields)
    result
else:
    line
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\nbob,data2\nincomplete\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    println!("Final CSV output:");
    println!("'{}'", output_str);
    println!("Lines processed: {}", stats.lines_processed);
    println!("Lines output: {}", stats.lines_output);
    
    // Check what we're looking for
    println!("Contains 'ALICE,data1_modified': {}", output_str.contains("ALICE,data1_modified"));
    println!("Contains 'BOB,data2_modified': {}", output_str.contains("BOB,data2_modified"));
}

#[test]
fn csv_error_debug() {
    use std::io::Cursor;
    use stelp::processors::StarlarkProcessor;
    use stelp::config::{PipelineConfig, ErrorStrategy};
    use stelp::StreamPipeline;

    // Use fail-fast strategy to see errors
    let config = PipelineConfig {
        error_strategy: ErrorStrategy::FailFast,
        ..Default::default()
    };
    let mut pipeline = StreamPipeline::new(config);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Simple test first
emit("BEFORE CSV")
fields = st.parse_csv(line)
emit("AFTER CSV: " + str(fields))
"test_output"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\n");
    let mut output = Vec::new();

    match pipeline.process_stream(input, &mut output, Some("test.txt")) {
        Ok(stats) => {
            let output_str = String::from_utf8(output).unwrap();
            println!("CSV error debug output:");
            println!("'{}'", output_str);
            println!("Stats: {:?}", stats);
        }
        Err(e) => {
            println!("Error occurred: {:?}", e);
        }
    }
}

#[test]
fn test_st_parse_csv_exists() {
    use std::io::Cursor;
    use stelp::processors::StarlarkProcessor;
    use stelp::config::{PipelineConfig, ErrorStrategy};
    use stelp::StreamPipeline;

    // Test basic script first
    let config1 = PipelineConfig {
        error_strategy: ErrorStrategy::FailFast,
        ..Default::default()
    };
    let mut pipeline = StreamPipeline::new(config1);

    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Test if st.parse_csv exists
"Before calling st.parse_csv"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\n");
    let mut output = Vec::new();

    let result = pipeline.process_stream(input, &mut output, Some("test.txt"));
    match result {
        Ok(_) => println!("Basic test passed"),
        Err(e) => println!("Basic test failed: {:?}", e),
    }
    
    // Now test with st.parse_csv
    let processor2 = StarlarkProcessor::from_script(
        "test2", 
        "st.parse_csv(line)"
    ).unwrap();
    
    let config2 = PipelineConfig {
        error_strategy: ErrorStrategy::FailFast,
        ..Default::default()
    };
    let mut pipeline2 = StreamPipeline::new(config2);
    pipeline2.add_processor(Box::new(processor2));
    
    let input2 = Cursor::new("alice,data1\n");
    let mut output2 = Vec::new();
    
    let result2 = pipeline2.process_stream(input2, &mut output2, Some("test.txt"));
    match result2 {
        Ok(_) => {
            let output_str = String::from_utf8(output2).unwrap();
            println!("CSV test output: '{}'", output_str);
        }
        Err(e) => println!("CSV test failed: {:?}", e),
    }
}

#[test]
fn csv_step_by_step_debug() {
    use std::io::Cursor;
    use stelp::processors::StarlarkProcessor;
    use stelp::config::{PipelineConfig, ErrorStrategy};
    use stelp::StreamPipeline;

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::FailFast,
        ..Default::default()
    };

    // Test each step individually
    
    // Step 1: Parse CSV
    println!("=== Step 1: Parse CSV ===");
    let mut pipeline1 = StreamPipeline::new(config.clone());
    let processor1 = StarlarkProcessor::from_script("test1", "st.parse_csv(line)").unwrap();
    pipeline1.add_processor(Box::new(processor1));
    let input1 = Cursor::new("alice,data1\n");
    let mut output1 = Vec::new();
    let result1 = pipeline1.process_stream(input1, &mut output1, Some("test.txt"));
    println!("Step 1 result: {:?}", result1);
    println!("Step 1 output: '{}'", String::from_utf8(output1).unwrap());
    
    // Step 2: Parse and check length
    println!("\n=== Step 2: Parse and check length ===");
    let mut pipeline2 = StreamPipeline::new(config.clone());
    let processor2 = StarlarkProcessor::from_script("test2", r#"
fields = st.parse_csv(line)
len(fields)
    "#).unwrap();
    pipeline2.add_processor(Box::new(processor2));
    let input2 = Cursor::new("alice,data1\n");
    let mut output2 = Vec::new();
    let result2 = pipeline2.process_stream(input2, &mut output2, Some("test.txt"));
    println!("Step 2 result: {:?}", result2);
    println!("Step 2 output: '{}'", String::from_utf8(output2).unwrap());
    
    // Step 3: Parse and access first element
    println!("\n=== Step 3: Access first element ===");
    let mut pipeline3 = StreamPipeline::new(config.clone());
    let processor3 = StarlarkProcessor::from_script("test3", r#"
fields = st.parse_csv(line)
fields[0]
    "#).unwrap();
    pipeline3.add_processor(Box::new(processor3));
    let input3 = Cursor::new("alice,data1\n");
    let mut output3 = Vec::new();
    let result3 = pipeline3.process_stream(input3, &mut output3, Some("test.txt"));
    println!("Step 3 result: {:?}", result3);
    println!("Step 3 output: '{}'", String::from_utf8(output3).unwrap());
    
    // Step 4: Parse and call upper() on first element
    println!("\n=== Step 4: Call upper() on first element ===");
    let mut pipeline4 = StreamPipeline::new(config);
    let processor4 = StarlarkProcessor::from_script("test4", r#"
fields = st.parse_csv(line)
fields[0].upper()
    "#).unwrap();
    pipeline4.add_processor(Box::new(processor4));
    let input4 = Cursor::new("alice,data1\n");
    let mut output4 = Vec::new();
    let result4 = pipeline4.process_stream(input4, &mut output4, Some("test.txt"));
    println!("Step 4 result: {:?}", result4);
    println!("Step 4 output: '{}'", String::from_utf8(output4).unwrap());
}

#[test]
fn csv_exact_script_debug() {
    use std::io::Cursor;
    use stelp::processors::StarlarkProcessor;
    use stelp::config::{PipelineConfig, ErrorStrategy};
    use stelp::StreamPipeline;

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::FailFast,
        ..Default::default()
    };

    // Test the exact failing script with debug output
    let mut pipeline = StreamPipeline::new(config);
    let processor = StarlarkProcessor::from_script("test", r#"
# Parse CSV and reconstruct with modified data
fields = st.parse_csv(line)
emit("DEBUG: fields = " + str(fields))
emit("DEBUG: len(fields) = " + str(len(fields)))

if len(fields) >= 2:
    emit("DEBUG: Inside if condition")
    field0_upper = fields[0].upper()
    field1_modified = fields[1] + "_modified"
    emit("DEBUG: field0_upper = " + field0_upper)
    emit("DEBUG: field1_modified = " + field1_modified)
    new_fields = [field0_upper, field1_modified]
    emit("DEBUG: new_fields = " + str(new_fields))
    csv_result = st.to_csv(new_fields)
    emit("DEBUG: csv_result = " + csv_result)
    csv_result
else:
    emit("DEBUG: In else branch")
    line
        "#).unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\n");
    let mut output = Vec::new();

    let result = pipeline.process_stream(input, &mut output, Some("test.txt"));
    
    match result {
        Ok(stats) => {
            let output_str = String::from_utf8(output).unwrap();
            println!("=== EXACT SCRIPT DEBUG ===");
            println!("Stats: {:?}", stats);
            println!("Output:");
            println!("{}", output_str);
        }
        Err(e) => {
            println!("Error in exact script: {:?}", e);
        }
    }
}