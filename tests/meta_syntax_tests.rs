// tests/meta_syntax_tests.rs
use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::context::{RecordContext, RecordData};
use stelp::processors::StarlarkProcessor;
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_meta_linenum_syntax() {
    println!("=== Testing LINENUM syntax ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 42,
        record_count: 10,
        file_name: Some("test.txt"),
        global_vars: &globals,
        debug: false,
    };

    let processor =
        StarlarkProcessor::from_script("meta_test", r#"f"Line {LINENUM} in {FILENAME}: {line}""#)
            .unwrap();

    let record = RecordData::text("hello world".to_string());
    let result = processor.process_standalone(&record, &ctx);

    println!("Result: {:?}", result);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert_eq!(text, "Line 42 in test.txt: hello world");
                println!("✅ LINENUM syntax works!");
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_meta_dot_notation() {
    println!("=== Testing meta variables in f-strings ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 42,
        record_count: 10,
        file_name: Some("test.txt"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "meta_dot_test",
        r#"
# Test simple variable access that works in f-strings
line_num = LINENUM
filename = FILENAME
f"Line {line_num} in {filename}: {line}"
        "#,
    )
    .unwrap();

    let record = RecordData::text("hello world".to_string());
    let result = processor.process_standalone(&record, &ctx);

    println!("Result: {:?}", result);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert_eq!(text, "Line 42 in test.txt: hello world");
                println!("✅ LINENUM variables work in f-strings!");
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_meta_properties() {
    println!("=== Testing various meta properties ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 5,
        record_count: 3,
        file_name: Some("data.log"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "meta_props_test",
        r#"
# Test all meta variables
result = []
result.append(f"linenum: {LINENUM}")
result.append(f"record_count: {RECNUM}")
result.append(f"filename: {FILENAME}")
" | ".join(result)
        "#,
    )
    .unwrap();

    let record = RecordData::text("test line".to_string());
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            if let Some(text) = output.as_text() {
                assert!(text.contains("linenum: 5"));
                assert!(text.contains("record_count: 3"));
                assert!(text.contains("filename: data.log"));
                println!("✅ All meta properties work: {}", text);
            } else {
                panic!("Expected text output");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_meta_in_pipeline() {
    println!("=== Testing meta in full pipeline ===");

    let config = PipelineConfig::default();
    let mut pipeline = StreamPipeline::new(config);

    let processor =
        StarlarkProcessor::from_script("pipeline_meta", r#"f"[{FILENAME}:{LINENUM}] {line}""#)
            .unwrap();
    pipeline.add_processor(Box::new(processor));

    let input = Cursor::new("first line\nsecond line\nthird line\n");
    let mut output = Vec::new();

    let stats = pipeline
        .process_stream(input, &mut output, Some("input.txt"))
        .unwrap();

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);

    let output_str = String::from_utf8(output).unwrap();
    println!("Pipeline output:\n{}", output_str);

    assert!(output_str.contains("[input.txt:1] first line"));
    assert!(output_str.contains("[input.txt:2] second line"));
    assert!(output_str.contains("[input.txt:3] third line"));

    println!("✅ Meta works in full pipeline!");
}

#[test]
fn test_meta_with_structured_data() {
    println!("=== Testing meta with structured data ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: Some("data.json"),
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "structured_meta",
        r#"
if data:
    # In data mode, modify the data variable to include meta info
    data = {"meta": f"Record {LINENUM}: structured data from {FILENAME}", "original": data}
else:
    # This would be line mode - return formatted text
    f"Text {LINENUM}: {line}"
        "#,
    )
    .unwrap();

    // Test with structured data
    let json_data = serde_json::json!({"name": "Alice", "age": 30});
    let record = RecordData::structured(json_data);
    let result = processor.process_standalone(&record, &ctx);

    match result {
        stelp::context::ProcessResult::Transform(output) => {
            println!(
                "Output type: text={}, structured={}",
                output.is_text(),
                output.is_structured()
            );
            if let Some(structured) = output.as_structured() {
                // Should now have meta field with expected content
                assert_eq!(structured["meta"], "Record 1: structured data from data.json");
                assert_eq!(structured["original"]["name"], "Alice");
                assert_eq!(structured["original"]["age"], 30);
                println!("✅ Meta works with structured data: {:?}", structured);
            } else if let Some(text) = output.as_text() {
                panic!("Expected structured output, got text: {}", text);
            } else {
                panic!("Expected structured output, got unknown type");
            }
        }
        other => panic!("Expected Transform result, got: {:?}", other),
    }
}

#[test]
fn test_meta_none_filename() {
    println!("=== Testing meta with None filename ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None, // No filename (e.g., stdin)
        global_vars: &globals,
        debug: false,
    };

    let processor = StarlarkProcessor::from_script(
        "no_filename_test",
        r#"
filename = FILENAME if FILENAME else "<stdin>"
f"Line {LINENUM} from {filename}: {line}"
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
