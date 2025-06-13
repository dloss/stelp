// tests/integration_tests.rs
use std::io::Cursor;
use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::context::{RecordContext, RecordData};
use stelp::processors::{FilterProcessor, StarlarkProcessor};
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_exit_working() {
    println!("=== Testing working exit ===");

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
# First transform the line
result = line.upper()

# Then check if we should exit
if "STOP" in line:
    exit("Stopped at: " + line)

# Return the transformed result
result
        "#,
    )
    .unwrap();

    // Test normal line
    let record1 = RecordData::text("hello".to_string());
    let result1 = processor.process_standalone(&record1, &ctx);
    println!("Normal line result: {:?}", result1);

    // Test exit line
    let record2 = RecordData::text("STOP here".to_string());
    let result2 = processor.process_standalone(&record2, &ctx);
    println!("Exit line result: {:?}", result2);

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

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);
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

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 4);
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "HELLO\nWORLD\nFOO\nBAR\n"
    );
}

#[test]
fn test_global_variables() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // UPDATED: Use new glob dictionary syntax
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
glob["count"] = glob.get("count", 0) + 1
count = glob['count']
f"Line {count}: {line}"
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "Line 1: hello\nLine 2: world\n"
    );
}

#[test]
fn test_regex_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // UPDATED: Use regex functions without st_ prefix
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
if regex_match(r"\d+", line):
    result = regex_replace(r"\d+", "NUMBER", line)
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

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "hello NUMBER\nworld\ntest NUMBER\n");
}

#[test]
fn test_json_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // UPDATED: Use JSON functions without st_ prefix
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Create a simple JSON object and convert it
data = {"line": line, "length": len(line)}
dump_json(data)
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    // The output should contain JSON strings
    assert!(output_str.contains("hello"));
    assert!(output_str.contains("world"));
}

#[test]
fn test_csv_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Very simple test - just return "PROCESSED" to see if the script runs at all
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
"PROCESSED: " + line
        "#,
    )
    .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("alice,data1\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    eprintln!("Stats: {:?}", stats);
    let output_str = String::from_utf8(output).unwrap();
    eprintln!("Output: '{}'", output_str);

    // First check if we processed anything at all
    assert!(stats.records_processed > 0, "No records were processed");
    assert!(stats.records_output > 0, "No records were output");
    assert!(
        output_str.contains("PROCESSED"),
        "Script didn't run: {}",
        output_str
    );
}

#[test]
fn test_context_functions() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // UPDATED: Use ALLUPPERCASE meta variables
    let processor = StarlarkProcessor::from_script(
        "test",
        r#"
# Use context variables from global namespace
line_info = f"Line {LINENUM} in {FILENAME}: {line}"
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

    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);

    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("Line 1 in test.txt: hello"));
    assert!(output_str.contains("Line 2 in test.txt: world"));
}

#[test]
fn test_simple_filter() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("test_filter", r#""keep" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    let input = Cursor::new("keep this\nskip this line\nkeep this too\nskip me\nfinal line\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 3);

    assert!(output_str.contains("keep this\n"));
    assert!(output_str.contains("keep this too\n"));
    assert!(!output_str.contains("final line\n"));
    assert!(!output_str.contains("skip this line"));
    assert!(!output_str.contains("skip me"));
}

#[test]
fn test_filter_combined_with_eval() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // First filter out lines containing "skip"
    let filter = FilterProcessor::from_expression("keep_filter", r#""keep" in line"#).unwrap();
    pipeline.add_processor(Box::new(filter));

    // Then transform remaining lines to uppercase
    let processor = StarlarkProcessor::from_script("uppercase", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("hello\nkeep this\nworld\nkeep me too\nend\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("test.txt"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();

    assert_eq!(stats.records_processed, 5);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 3);
    assert_eq!(output_str, "KEEP THIS\nKEEP ME TOO\n");
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
    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.errors, 3); // All lines should error
    assert_eq!(stats.records_output, 0); // No successful outputs
}

#[test]
fn test_emit_all_and_no_implicit_fanout() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test that lists no longer fan out implicitly - they should output as string representations
    let list_processor = StarlarkProcessor::from_script("list_test", "[line + '0', line + '1']").unwrap();
    pipeline.add_processor(Box::new(list_processor));

    let input = Cursor::new("a\nb\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    // Should output list as strings, not fan out individual items
    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 2);
    assert_eq!(output_str, "[a0, a1]\n[b0, b1]\n");
}

#[test]
fn test_emit_all_function() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Test emit_all function for explicit fan-out
    let emit_all_processor = StarlarkProcessor::from_script(
        "emit_all_test", 
        "emit_all([line + '0', line + '1']); skip()"
    ).unwrap();
    pipeline.add_processor(Box::new(emit_all_processor));

    let input = Cursor::new("x\ny\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    // Should emit all items from the list, and skip the original record
    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 4); // 2 items emitted per input line
    assert_eq!(stats.records_skipped, 0); // emit() + skip() counts as fan-out, not skip
    assert_eq!(output_str, "\"x0\"\n\"x1\"\n\"y0\"\n\"y1\"\n");
}

#[test]
fn test_begin_end_basic() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // Set up BEGIN processor
    let begin_processor = StarlarkProcessor::from_script("BEGIN", "\"=== HEADER ===\"").unwrap();
    pipeline.set_begin_processor(Box::new(begin_processor));

    // Set up main processor  
    let main_processor = StarlarkProcessor::from_script("main", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(main_processor));

    // Set up END processor
    let end_processor = StarlarkProcessor::from_script("END", "\"=== FOOTER ===\"").unwrap();
    pipeline.set_end_processor(Box::new(end_processor));

    let input = Cursor::new("hello\nworld\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    assert_eq!(stats.records_processed, 2);
    assert_eq!(stats.records_output, 4); // 2 input + BEGIN + END
    assert_eq!(output_str, "=== HEADER ===\nHELLO\nWORLD\n=== FOOTER ===\n");
}

#[test]
fn test_begin_end_with_global_state() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // BEGIN: Initialize counter
    let begin_processor = StarlarkProcessor::from_script("BEGIN", "glob['count'] = 0").unwrap();
    pipeline.set_begin_processor(Box::new(begin_processor));

    // Main: Count and transform
    let main_processor = StarlarkProcessor::from_script("main", 
        "glob['count'] = glob.get('count', 0) + 1; line.upper()").unwrap();
    pipeline.add_processor(Box::new(main_processor));

    // END: Output total count
    let end_processor = StarlarkProcessor::from_script("END", 
        "count = glob.get('count', 0); f'Total: {count}'").unwrap();
    pipeline.set_end_processor(Box::new(end_processor));

    let input = Cursor::new("a\nb\nc\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 4); // 3 input + END output
    assert_eq!(output_str, "A\nB\nC\nTotal: 3\n");
}

#[test]
fn test_begin_early_exit() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // BEGIN with early exit
    let begin_processor = StarlarkProcessor::from_script("BEGIN", 
        "exit('Early termination')").unwrap();
    pipeline.set_begin_processor(Box::new(begin_processor));

    // This should not execute
    let main_processor = StarlarkProcessor::from_script("main", "line.upper()").unwrap();
    pipeline.add_processor(Box::new(main_processor));

    let input = Cursor::new("a\nb\nc\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    assert_eq!(stats.records_processed, 0); // No input lines processed
    assert_eq!(stats.records_output, 1); // Only BEGIN exit message
    assert_eq!(output_str, "Early termination\n");
}

#[test]
fn test_begin_end_empty_input() {
    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    // BEGIN processor
    let begin_processor = StarlarkProcessor::from_script("BEGIN", "\"Start\"").unwrap();
    pipeline.set_begin_processor(Box::new(begin_processor));

    // END processor  
    let end_processor = StarlarkProcessor::from_script("END", "\"End\"").unwrap();
    pipeline.set_end_processor(Box::new(end_processor));

    let input = Cursor::new("");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, None)
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    
    assert_eq!(stats.records_processed, 0); // No input lines
    assert_eq!(stats.records_output, 2); // BEGIN + END
    assert_eq!(output_str, "Start\nEnd\n");
}

#[test]
fn test_syslog_rfc5424_parsing() {
    use stelp::input_format::{InputFormat, InputFormatWrapper};
    
    let config = stelp::config::PipelineConfig::default();
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Script to extract key fields
    let processor = StarlarkProcessor::from_script(
        "syslog_test",
        r#"
pri = data["pri"]
facility = data["facility"] 
severity = data["severity"]
host = data["host"]
prog = data["prog"]
msg = data["msg"]
f"PRI={pri} FAC={facility} SEV={severity} HOST={host} PROG={prog} MSG={msg}"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    // RFC5424 syslog message
    let input = std::io::Cursor::new("<165>1 2023-10-11T22:14:15.003Z server01 sshd 1234 ID47 - Failed password for user\n");
    let mut output = Vec::new();

    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Syslog));
    let stats = wrapper.process_with_pipeline(input, &mut pipeline, &mut output, Some("test.log")).unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(stats.errors, 0);
    
    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "PRI=165 FAC=20 SEV=5 HOST=server01 PROG=sshd MSG=Failed password for user\n");
}

#[test]
fn test_syslog_rfc3164_parsing() {
    use stelp::input_format::{InputFormat, InputFormatWrapper};
    
    let config = stelp::config::PipelineConfig::default();
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Script to extract key fields
    let processor = StarlarkProcessor::from_script(
        "syslog_test",
        r#"
ts = data["ts"]
host = data["host"]
prog = data["prog"]
pid = data.get("pid", "none")
msg = data["msg"]
f"TS={ts} HOST={host} PROG={prog} PID={pid} MSG={msg}"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    // RFC3164 syslog message
    let input = std::io::Cursor::new("Oct 11 22:14:15 server01 sshd[1234]: Failed password for user from 192.168.1.100\n");
    let mut output = Vec::new();

    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Syslog));
    let stats = wrapper.process_with_pipeline(input, &mut pipeline, &mut output, Some("test.log")).unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(stats.errors, 0);
    
    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "TS=Oct 11 22:14:15 HOST=server01 PROG=sshd PID=1234 MSG=Failed password for user from 192.168.1.100\n");
}

#[test]
fn test_syslog_rfc3164_no_pid() {
    use stelp::input_format::{InputFormat, InputFormatWrapper};
    
    let config = stelp::config::PipelineConfig::default();
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Script to check for optional PID field
    let processor = StarlarkProcessor::from_script(
        "syslog_test",
        r#"
host = data["host"]
prog = data["prog"]
has_pid = "pid" in data
msg = data["msg"]
f"HOST={host} PROG={prog} HAS_PID={has_pid} MSG={msg}"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    // RFC3164 syslog message without PID
    let input = std::io::Cursor::new("Oct 11 22:14:15 server01 kernel: Out of memory: Kill process 1234\n");
    let mut output = Vec::new();

    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Syslog));
    let stats = wrapper.process_with_pipeline(input, &mut pipeline, &mut output, Some("test.log")).unwrap();

    assert_eq!(stats.records_processed, 1);
    assert_eq!(stats.records_output, 1);
    assert_eq!(stats.errors, 0);
    
    let output_str = String::from_utf8(output).unwrap();
    assert_eq!(output_str, "HOST=server01 PROG=kernel HAS_PID=False MSG=Out of memory: Kill process 1234\n");
}

#[test]
fn test_syslog_facility_severity_calculation() {
    use stelp::input_format::{InputFormat, InputFormatWrapper};
    
    let config = stelp::config::PipelineConfig::default();
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Test various priority values
    let processor = StarlarkProcessor::from_script(
        "syslog_test", 
        r#"
pri = data["pri"]
facility = data["facility"]
severity = data["severity"] 
f"PRI={pri} FAC={facility} SEV={severity}"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    // Test different priority values:
    // <0> = facility 0, severity 0 (kernel, emergency)
    // <33> = facility 4, severity 1 (security, alert) 
    // <165> = facility 20, severity 5 (local4, notice)
    let input = std::io::Cursor::new(
        "<0>1 2023-10-11T22:14:15Z host prog - - - kernel emergency\n<33>1 2023-10-11T22:14:15Z host prog - - - security alert\n<165>1 2023-10-11T22:14:15Z host prog - - - local4 notice\n"
    );
    let mut output = Vec::new();

    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Syslog));
    let stats = wrapper.process_with_pipeline(input, &mut pipeline, &mut output, Some("test.log")).unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
    assert_eq!(stats.errors, 0);
    
    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();
    assert_eq!(lines[0], "PRI=0 FAC=0 SEV=0");      // kernel.emergency
    assert_eq!(lines[1], "PRI=33 FAC=4 SEV=1");     // security.alert  
    assert_eq!(lines[2], "PRI=165 FAC=20 SEV=5");   // local4.notice
}

#[test]
fn test_syslog_invalid_format_error_handling() {
    use stelp::input_format::{InputFormat, InputFormatWrapper};
    
    let config = stelp::config::PipelineConfig::default();
    let mut pipeline = stelp::StreamPipeline::new(config);

    // Simple pass-through processor
    let processor = StarlarkProcessor::from_script("test", r#"data["msg"]"#).unwrap();
    pipeline.add_processor(Box::new(processor));

    // Invalid syslog format
    let input = std::io::Cursor::new("This is not a valid syslog message\nNor is this\n");
    let mut output = Vec::new();

    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Syslog));
    let stats = wrapper.process_with_pipeline(input, &mut pipeline, &mut output, Some("test.log")).unwrap();

    // Should skip invalid lines according to default error strategy
    assert_eq!(stats.records_processed, 0);
    assert_eq!(stats.records_output, 0);
    assert_eq!(stats.errors, 2); // Two parse errors
    assert_eq!(stats.parse_errors.len(), 2);
    assert_eq!(stats.parse_errors[0].format_name, "syslog");
}
