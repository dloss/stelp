// tests/level_filter_tests.rs - Tests for log level filtering functionality
use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::context::{RecordContext, RecordData};
use stelp::input_format::{InputFormat, InputFormatWrapper};
use stelp::pipeline::processors::LevelFilterProcessor;
use stelp::pipeline::stream::RecordProcessor;
use stelp::variables::GlobalVariables;
use stelp::StreamPipeline;

#[test]
fn test_level_filter_text_include_only() {
    println!("=== Testing level filter with text records - include only ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    let mut processor = LevelFilterProcessor::new("test_filter", Some("error,warn"), None);

    // Test error level - should pass
    let record1 = RecordData::text("ERROR: Database connection failed".to_string());
    let result1 = processor.process(&record1, &ctx);
    println!("Error level result: {:?}", result1);
    assert!(matches!(result1, stelp::ProcessResult::Transform(_)));

    // Test info level - should be skipped
    let record2 = RecordData::text("INFO: Application started".to_string());
    let result2 = processor.process(&record2, &ctx);
    println!("Info level result: {:?}", result2);
    assert!(matches!(result2, stelp::ProcessResult::Skip));

    // Test warn level - should pass
    let record3 = RecordData::text("WARN: Deprecated function used".to_string());
    let result3 = processor.process(&record3, &ctx);
    println!("Warn level result: {:?}", result3);
    assert!(matches!(result3, stelp::ProcessResult::Transform(_)));
}

#[test]
fn test_level_filter_text_exclude_only() {
    println!("=== Testing level filter with text records - exclude only ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    let mut processor = LevelFilterProcessor::new("test_filter", None, Some("debug,info"));

    // Test error level - should pass (not excluded)
    let record1 = RecordData::text("ERROR: Database connection failed".to_string());
    let result1 = processor.process(&record1, &ctx);
    assert!(matches!(result1, stelp::ProcessResult::Transform(_)));

    // Test info level - should be skipped (excluded)
    let record2 = RecordData::text("INFO: Application started".to_string());
    let result2 = processor.process(&record2, &ctx);
    assert!(matches!(result2, stelp::ProcessResult::Skip));

    // Test debug level - should be skipped (excluded)
    let record3 = RecordData::text("DEBUG: Query executed".to_string());
    let result3 = processor.process(&record3, &ctx);
    assert!(matches!(result3, stelp::ProcessResult::Skip));
}

#[test]
fn test_level_filter_priority_exclude_over_include() {
    println!("=== Testing level filter priority - exclude over include ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    // Include error,warn,info but exclude error
    let mut processor =
        LevelFilterProcessor::new("test_filter", Some("error,warn,info"), Some("error"));

    // Test error level - should be skipped (excluded takes priority)
    let record1 = RecordData::text("ERROR: Database connection failed".to_string());
    let result1 = processor.process(&record1, &ctx);
    assert!(matches!(result1, stelp::ProcessResult::Skip));

    // Test warn level - should pass (included and not excluded)
    let record2 = RecordData::text("WARN: Deprecated function used".to_string());
    let result2 = processor.process(&record2, &ctx);
    assert!(matches!(result2, stelp::ProcessResult::Transform(_)));

    // Test info level - should pass (included and not excluded)
    let record3 = RecordData::text("INFO: Application started".to_string());
    let result3 = processor.process(&record3, &ctx);
    assert!(matches!(result3, stelp::ProcessResult::Transform(_)));

    // Test debug level - should be skipped (not included)
    let record4 = RecordData::text("DEBUG: Query executed".to_string());
    let result4 = processor.process(&record4, &ctx);
    assert!(matches!(result4, stelp::ProcessResult::Skip));
}

#[test]
fn test_level_filter_structured_data() {
    println!("=== Testing level filter with structured data ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    let mut processor = LevelFilterProcessor::new("test_filter", Some("error,warn"), None);

    // Test structured record with error level
    let json_data1 = serde_json::json!({
        "level": "error",
        "message": "Database connection failed",
        "timestamp": "2024-01-01T10:00:00Z"
    });
    let record1 = RecordData::structured(json_data1);
    let result1 = processor.process(&record1, &ctx);
    assert!(matches!(result1, stelp::ProcessResult::Transform(_)));

    // Test structured record with info level
    let json_data2 = serde_json::json!({
        "level": "info",
        "message": "Application started",
        "timestamp": "2024-01-01T10:00:00Z"
    });
    let record2 = RecordData::structured(json_data2);
    let result2 = processor.process(&record2, &ctx);
    assert!(matches!(result2, stelp::ProcessResult::Skip));

    // Test structured record with different level field name
    let json_data3 = serde_json::json!({
        "severity": "warn",
        "message": "Deprecated function used",
        "timestamp": "2024-01-01T10:00:00Z"
    });
    let record3 = RecordData::structured(json_data3);
    let result3 = processor.process(&record3, &ctx);
    assert!(matches!(result3, stelp::ProcessResult::Transform(_)));
}

#[test]
fn test_level_filter_no_level_found() {
    println!("=== Testing level filter with no level found ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    // With include filter - should skip records without levels
    let mut processor1 = LevelFilterProcessor::new("test_filter", Some("error,warn"), None);
    let record1 = RecordData::text("Some random text without level".to_string());
    let result1 = processor1.process(&record1, &ctx);
    assert!(matches!(result1, stelp::ProcessResult::Skip));

    // With exclude filter only - should pass records without levels
    let mut processor2 = LevelFilterProcessor::new("test_filter", None, Some("debug"));
    let record2 = RecordData::text("Some random text without level".to_string());
    let result2 = processor2.process(&record2, &ctx);
    assert!(matches!(result2, stelp::ProcessResult::Transform(_)));

    // With no filters - should pass records without levels
    let mut processor3 = LevelFilterProcessor::new("test_filter", None, None);
    let record3 = RecordData::text("Some random text without level".to_string());
    let result3 = processor3.process(&record3, &ctx);
    assert!(matches!(result3, stelp::ProcessResult::Transform(_)));
}

#[test]
fn test_level_filter_case_insensitive() {
    println!("=== Testing level filter case insensitivity ===");

    let globals = GlobalVariables::new();
    let ctx = RecordContext {
        line_number: 1,
        record_count: 1,
        file_name: None,
        global_vars: &globals,
        debug: false,
    };

    let mut processor = LevelFilterProcessor::new("test_filter", Some("ERROR,WARN"), None);

    // Test lowercase error in text
    let record1 = RecordData::text("error: Database connection failed".to_string());
    let result1 = processor.process(&record1, &ctx);
    assert!(matches!(result1, stelp::ProcessResult::Transform(_)));

    // Test uppercase WARN in text
    let record2 = RecordData::text("WARN: Deprecated function used".to_string());
    let result2 = processor.process(&record2, &ctx);
    assert!(matches!(result2, stelp::ProcessResult::Transform(_)));

    // Test mixed case in structured data
    let json_data = serde_json::json!({
        "level": "Error",
        "message": "Database connection failed"
    });
    let record3 = RecordData::structured(json_data);
    let result3 = processor.process(&record3, &ctx);
    assert!(matches!(result3, stelp::ProcessResult::Transform(_)));
}

#[test]
fn test_level_filter_in_pipeline() {
    println!("=== Testing level filter in full pipeline ===");

    let config = PipelineConfig::default();

    let mut pipeline = StreamPipeline::new(config);

    // Add level filter processor
    let level_filter = LevelFilterProcessor::new("level_filter", Some("error,warn"), None);
    pipeline.add_processor(Box::new(level_filter));

    let format_wrapper = InputFormatWrapper::new(Some(&InputFormat::Line));

    // Test input with mixed log levels
    let input = "INFO: Application started\nERROR: Database connection failed\nDEBUG: Query executed\nWARN: Deprecated function used\n";
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(Cursor::new(input), &mut pipeline, &mut output, Some("test"))
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    println!("Pipeline output: {}", output_str);

    // Should only contain ERROR and WARN lines
    assert!(output_str.contains("ERROR: Database connection failed"));
    assert!(output_str.contains("WARN: Deprecated function used"));
    assert!(!output_str.contains("INFO: Application started"));
    assert!(!output_str.contains("DEBUG: Query executed"));

    // Should have processed 4 records but only output 2
    assert_eq!(stats.records_processed, 4);
    assert_eq!(stats.records_output, 2);
    assert_eq!(stats.records_skipped, 2);
}
