use std::io::Cursor;
use stelp::chunking::{ChunkConfig, ChunkStrategy};
use stelp::config::{ErrorStrategy, PipelineConfig};
use stelp::input_format::InputFormatWrapper;
use stelp::processors::StarlarkProcessor;
use stelp::StreamPipeline;
use regex::Regex;

#[test]
fn test_multiline_log_processing() {
    let log_input = "2024-01-01 10:00:00 INFO Starting application
2024-01-01 10:00:01 ERROR Exception occurred
java.lang.RuntimeException: Something went wrong
    at com.example.Service.doSomething(Service.java:42)
    at com.example.Controller.handle(Controller.java:23)
2024-01-01 10:00:02 INFO Application recovered";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let chunk_config = ChunkConfig {
        strategy: ChunkStrategy::StartPattern(
            Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap()
        ),
        max_chunk_lines: 100,
        max_chunk_size: 10000,
    };

    let mut pipeline = StreamPipeline::new(config);
    
    // Add a processor to count lines in each chunk
    let processor = StarlarkProcessor::from_script(
        "line_counter",
        r#"
line_count = len(line.split('\n'))
if line_count > 1:
    first_line = line.split('\n')[0]
    result = f"MULTILINE({line_count}): {first_line}"
else:
    result = f"SINGLE: {line}"
result
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let format_wrapper = InputFormatWrapper::new(None).with_chunking(chunk_config);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(log_input),
            &mut pipeline,
            &mut output,
            Some("test.log"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should have 3 chunks
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains("SINGLE: 2024-01-01 10:00:00 INFO Starting"));
    assert!(lines[1].contains("MULTILINE(4): 2024-01-01 10:00:01 ERROR"));
    assert!(lines[2].contains("SINGLE: 2024-01-01 10:00:02 INFO Application"));

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
    assert_eq!(stats.errors, 0);
}

#[test]
fn test_fixed_line_chunking() {
    let input = "line1
line2
line3
line4
line5
line6
line7";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let chunk_config = ChunkConfig {
        strategy: ChunkStrategy::FixedLines(3),
        max_chunk_lines: 100,
        max_chunk_size: 10000,
    };

    let mut pipeline = StreamPipeline::new(config);
    
    // Add a processor to show chunk boundaries
    let processor = StarlarkProcessor::from_script(
        "chunk_marker",
        r#"
replaced = line.replace('\n', ' | ')
result = f"CHUNK: {replaced}"
result
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let format_wrapper = InputFormatWrapper::new(None).with_chunking(chunk_config);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(input),
            &mut pipeline,
            &mut output,
            Some("test.txt"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should have 3 chunks: [1,2,3], [4,5,6], [7]
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "CHUNK: line1 | line2 | line3");
    assert_eq!(lines[1], "CHUNK: line4 | line5 | line6");
    assert_eq!(lines[2], "CHUNK: line7");

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
}

#[test]
fn test_delimiter_chunking() {
    let input = "section1
data1
---
section2
data2
---
section3
data3";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let chunk_config = ChunkConfig {
        strategy: ChunkStrategy::Delimiter("---".to_string()),
        max_chunk_lines: 100,
        max_chunk_size: 10000,
    };

    let mut pipeline = StreamPipeline::new(config);
    
    // Add a processor to extract section name
    let processor = StarlarkProcessor::from_script(
        "section_extractor",
        r#"
first_line = line.split('\n')[0]
if first_line.startswith('section'):
    result = f"SECTION: {first_line}"
else:
    result = f"OTHER: {first_line}"
result
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let format_wrapper = InputFormatWrapper::new(None).with_chunking(chunk_config);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(input),
            &mut pipeline,
            &mut output,
            Some("test.txt"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should have 3 sections
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "SECTION: section1");
    assert_eq!(lines[1], "SECTION: section2");
    assert_eq!(lines[2], "SECTION: section3");

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
}

#[test]
#[ignore] // TODO: Global state preservation needs further investigation with batch processing
fn test_chunking_with_global_state() {
    let input = "item1
item2
---
item3
item4
---
item5";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let chunk_config = ChunkConfig {
        strategy: ChunkStrategy::Delimiter("---".to_string()),
        max_chunk_lines: 100,
        max_chunk_size: 10000,
    };

    let mut pipeline = StreamPipeline::new(config);
    
    // Add a processor that counts chunks and items
    let processor = StarlarkProcessor::from_script(
        "chunk_counter",
        r#"
chunk_num = inc("chunk_count")
item_count = len(line.split('\n'))
"Chunk " + str(chunk_num) + ": " + str(item_count) + " items"
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let format_wrapper = InputFormatWrapper::new(None).with_chunking(chunk_config);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(input),
            &mut pipeline,
            &mut output,
            Some("test.txt"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Basic test - make sure we get output from all chunks
    assert_eq!(lines.len(), 3);
    
    // Each line should contain chunk information and item counts
    for line in &lines {
        assert!(line.contains("Chunk"));
        assert!(line.contains("items"));
    }

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
}

#[test]
fn test_chunking_safety_limits() {
    // Create input that would exceed line limit
    let long_input = "line1
line2
line3
line4
line5
line6
line7
line8
line9
line10";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let chunk_config = ChunkConfig {
        strategy: ChunkStrategy::FixedLines(20), // Would normally take all lines
        max_chunk_lines: 3, // But safety limit kicks in
        max_chunk_size: 10000,
    };

    let mut pipeline = StreamPipeline::new(config);
    
    let processor = StarlarkProcessor::from_script(
        "line_counter",
        r#"
line_count = len(line.split('\n'))
result = f"Lines: {line_count}"
result
        "#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    let format_wrapper = InputFormatWrapper::new(None).with_chunking(chunk_config);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(long_input),
            &mut pipeline,
            &mut output,
            Some("test.txt"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should break into chunks of max 3 lines each
    assert!(lines.len() >= 3); // At least 3 chunks due to safety limit
    
    // Each chunk should have at most 3 lines
    for line in &lines {
        let line_count: usize = line.split(' ').last().unwrap().parse().unwrap();
        assert!(line_count <= 3);
    }

    assert_eq!(stats.records_processed, lines.len());
    assert_eq!(stats.records_output, lines.len());
}

#[test]
fn test_no_chunking_compatibility() {
    let input = "line1
line2
line3";

    let config = PipelineConfig {
        error_strategy: ErrorStrategy::Skip,
        debug: false,
        ..Default::default()
    };

    let mut pipeline = StreamPipeline::new(config);
    
    let processor = StarlarkProcessor::from_script(
        "passthrough",
        r#"line.upper()"#,
    ).unwrap();
    
    pipeline.add_processor(Box::new(processor));

    // No chunking config - should process line by line
    let format_wrapper = InputFormatWrapper::new(None);
    let mut output = Vec::new();

    let stats = format_wrapper
        .process_with_pipeline(
            Cursor::new(input),
            &mut pipeline,
            &mut output,
            Some("test.txt"),
        )
        .unwrap();

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.trim().split('\n').collect();

    // Should process each line individually
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0], "LINE1");
    assert_eq!(lines[1], "LINE2");
    assert_eq!(lines[2], "LINE3");

    assert_eq!(stats.records_processed, 3);
    assert_eq!(stats.records_output, 3);
}