use std::io::Cursor;
use stelp::config::PipelineConfig;
use stelp::input_format::{InputFormat, InputFormatWrapper};
use stelp::output_format::OutputFormat;
use stelp::StreamPipeline;

#[test]
fn test_automatic_flattening_jsonl_to_csv() {
    let input = r#"{"user":{"name":"Alice","age":30},"tags":["admin","user"]}
{"user":{"name":"Bob","age":25},"tags":["guest"]}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Csv;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should have flattened headers (order may vary)
    assert!(result.contains("tags.0"));
    assert!(result.contains("tags.1"));
    assert!(result.contains("user.age"));
    assert!(result.contains("user.name"));
    // Should have flattened data
    assert!(result.contains("admin"));
    assert!(result.contains("user"));
    assert!(result.contains("30"));
    assert!(result.contains("Alice"));
    assert!(result.contains("guest"));
    assert!(result.contains("25"));
    assert!(result.contains("Bob"));
}

#[test]
fn test_automatic_flattening_jsonl_to_tsv() {
    let input = r#"{"config":{"theme":"dark","lang":"en"},"id":123}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Tsv;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should have tab-separated flattened headers (order may vary)
    assert!(result.contains("config.theme"));
    assert!(result.contains("config.lang"));
    assert!(result.contains("id"));
    // Should have tab-separated flattened data
    assert!(result.contains("dark"));
    assert!(result.contains("en"));
    assert!(result.contains("123"));
}

#[test]
fn test_automatic_flattening_jsonl_to_fields() {
    let input = r#"{"user":{"name":"Alice"},"status":"active"}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Fields;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should have space-separated flattened values (order may vary)
    assert!(result.contains("Alice"));
    assert!(result.contains("active"));
}

#[test]
fn test_no_flattening_for_jsonl_output() {
    let input = r#"{"user":{"name":"Alice","age":30}}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Jsonl;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should preserve nested structure (key order may vary)
    assert!(result.contains(r#""user":"#));
    assert!(result.contains(r#""name":"Alice""#));
    assert!(result.contains(r#""age":30"#));
    // Should NOT contain flattened keys
    assert!(!result.contains("user.name"));
}

#[test]
fn test_no_flattening_for_simple_objects() {
    let input = r#"{"name":"Alice","age":30,"active":true}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Csv;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should have simple headers (no dots since no nesting, order may vary)
    assert!(result.contains("name"));
    assert!(result.contains("age"));
    assert!(result.contains("active"));
    // Should have simple data
    assert!(result.contains("Alice"));
    assert!(result.contains("30"));
    assert!(result.contains("true"));
}

#[test]
fn test_flattening_with_key_selection() {
    let input = r#"{"user":{"name":"Alice","age":30},"meta":{"created":"2023-01-01"}}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Csv;
    config.keys = Some(vec!["user.name".to_string(), "meta.created".to_string()]);
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should only include selected flattened keys
    assert!(result.contains("user.name,meta.created"));
    assert!(result.contains("Alice,2023-01-01"));
    // Should NOT include user.age
    assert!(!result.contains("user.age"));
}

#[test]
fn test_flattening_complex_nesting() {
    let input = r#"{"data":{"users":[{"info":{"name":"Alice","details":{"age":30}}}]}}"#;
    
    let mut config = PipelineConfig::default();
    config.output_format = OutputFormat::Csv;
    
    let mut pipeline = StreamPipeline::new(config);
    let mut output = Vec::new();
    
    let wrapper = InputFormatWrapper::new(Some(&InputFormat::Jsonl));
    wrapper.process_with_pipeline(
        Cursor::new(input),
        &mut pipeline,
        &mut output,
        Some("test.jsonl"),
    ).unwrap();
    
    let result = String::from_utf8(output).unwrap();
    
    // Should handle deep nesting with dot notation (order may vary)
    assert!(result.contains("data.users.0.info.name"));
    assert!(result.contains("data.users.0.info.details.age"));
    assert!(result.contains("Alice"));
    assert!(result.contains("30"));
}