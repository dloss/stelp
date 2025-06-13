use std::io::Cursor;
use stelp::chunking::{ChunkConfig, ChunkStrategy, chunk_lines, parse_chunk_strategy};
use regex::Regex;

#[test]
fn test_line_strategy() {
    let input = "line1\nline2\nline3";
    let config = ChunkConfig {
        strategy: ChunkStrategy::Line,
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0], "line1");
    assert_eq!(chunks[1], "line2");
    assert_eq!(chunks[2], "line3");
}

#[test]
fn test_fixed_lines_strategy() {
    let input = "line1\nline2\nline3\nline4\nline5";
    let config = ChunkConfig {
        strategy: ChunkStrategy::FixedLines(2),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0], "line1\nline2");
    assert_eq!(chunks[1], "line3\nline4");
    assert_eq!(chunks[2], "line5");
}

#[test]
fn test_timestamp_pattern_strategy() {
    let input = "2024-01-01 10:00:00 Start\nContinuation line\n2024-01-01 10:01:00 Next entry\nMore data";
    let config = ChunkConfig {
        strategy: ChunkStrategy::StartPattern(
            Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap()
        ),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0], "2024-01-01 10:00:00 Start\nContinuation line");
    assert_eq!(chunks[1], "2024-01-01 10:01:00 Next entry\nMore data");
}

#[test]
fn test_delimiter_strategy() {
    let input = "section1\ndata1\n---\nsection2\ndata2\n---\nsection3\nfinal data";
    let config = ChunkConfig {
        strategy: ChunkStrategy::Delimiter("---".to_string()),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0], "section1\ndata1");
    assert_eq!(chunks[1], "section2\ndata2");
    assert_eq!(chunks[2], "section3\nfinal data");
    
    // Verify delimiters are not included
    assert!(!chunks[0].contains("---"));
    assert!(!chunks[1].contains("---"));
    assert!(!chunks[2].contains("---"));
}

#[test]
fn test_safety_limits_line_count() {
    let input = "line1\nline2\nline3\nline4\nline5\nline6";
    let config = ChunkConfig {
        strategy: ChunkStrategy::FixedLines(10), // Won't trigger normally
        max_chunk_lines: 2, // This will trigger safety limit
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    assert_eq!(chunks[0], "line1\nline2");
    assert_eq!(chunks[1], "line3\nline4");
    assert_eq!(chunks[2], "line5\nline6");
}

#[test]
fn test_java_stacktrace_pattern() {
    let input = r#"2024-01-01 10:00:00 INFO Starting application
2024-01-01 10:00:01 ERROR Exception occurred
java.lang.RuntimeException: Something went wrong
    at com.example.Service.doSomething(Service.java:42)
    at com.example.Controller.handle(Controller.java:23)
    at java.base/java.lang.Thread.run(Thread.java:829)
2024-01-01 10:00:02 INFO Application recovered"#;
    
    let config = ChunkConfig {
        strategy: ChunkStrategy::StartPattern(
            Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}").unwrap()
        ),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    
    // First chunk should just be the startup message
    assert_eq!(chunks[0], "2024-01-01 10:00:00 INFO Starting application");
    
    // Second chunk should contain the full stack trace
    assert!(chunks[1].contains("java.lang.RuntimeException"));
    assert!(chunks[1].contains("at com.example.Service.doSomething"));
    assert!(chunks[1].contains("at java.base/java.lang.Thread.run"));
    
    // Third chunk should be the recovery message
    assert_eq!(chunks[2], "2024-01-01 10:00:02 INFO Application recovered");
}

#[test]
fn test_python_stacktrace_pattern() {
    let input = r#"[2024-01-01 10:00:00] INFO: Starting process
[2024-01-01 10:00:01] ERROR: Unhandled exception
Traceback (most recent call last):
  File "/app/main.py", line 15, in <module>
    result = process_data(data)
  File "/app/processor.py", line 42, in process_data
    return calculate(value)
ValueError: Invalid input value
[2024-01-01 10:00:02] INFO: Process restarted"#;
    
    let config = ChunkConfig {
        strategy: ChunkStrategy::StartPattern(
            Regex::new(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]").unwrap()
        ),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 3);
    
    // First chunk
    assert_eq!(chunks[0], "[2024-01-01 10:00:00] INFO: Starting process");
    
    // Second chunk should contain the full Python traceback
    assert!(chunks[1].contains("Traceback (most recent call last)"));
    assert!(chunks[1].contains("File \"/app/main.py\""));
    assert!(chunks[1].contains("ValueError: Invalid input value"));
    
    // Third chunk
    assert_eq!(chunks[2], "[2024-01-01 10:00:02] INFO: Process restarted");
}

#[test]
fn test_parse_chunk_strategy() {
    // Test valid strategies
    assert!(matches!(
        parse_chunk_strategy("line").unwrap(), 
        ChunkStrategy::Line
    ));
    
    assert!(matches!(
        parse_chunk_strategy("lines:5").unwrap(),
        ChunkStrategy::FixedLines(5)
    ));
    
    assert!(matches!(
        parse_chunk_strategy("delimiter:---").unwrap(),
        ChunkStrategy::Delimiter(ref s) if s == "---"
    ));
    
    // Test pattern strategy
    let pattern_strategy = parse_chunk_strategy("start-pattern:^\\d{4}").unwrap();
    if let ChunkStrategy::StartPattern(regex) = pattern_strategy {
        assert!(regex.is_match("2024"));
        assert!(!regex.is_match("not a year"));
    } else {
        panic!("Expected StartPattern strategy");
    }
    
    // Test invalid strategies
    assert!(parse_chunk_strategy("invalid").is_err());
    assert!(parse_chunk_strategy("lines:abc").is_err());
    assert!(parse_chunk_strategy("start-pattern:[invalid").is_err());
}

#[test]
fn test_empty_input() {
    let input = "";
    let config = ChunkConfig {
        strategy: ChunkStrategy::Line,
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 0);
}

#[test]
fn test_single_line_input() {
    let input = "single line";
    let config = ChunkConfig {
        strategy: ChunkStrategy::FixedLines(3),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], "single line");
}

#[test]
fn test_delimiter_not_found() {
    let input = "line1\nline2\nline3";
    let config = ChunkConfig {
        strategy: ChunkStrategy::Delimiter("---".to_string()),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], "line1\nline2\nline3");
}

#[test]
fn test_pattern_not_found() {
    let input = "line1\nline2\nline3";
    let config = ChunkConfig {
        strategy: ChunkStrategy::StartPattern(
            Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap()
        ),
        ..Default::default()
    };

    let chunks = chunk_lines(Cursor::new(input), config).unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0], "line1\nline2\nline3");
}