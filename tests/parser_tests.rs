// tests/parser_tests.rs - Unit tests for format parsers

use stelp::input_format::{CsvParser, JsonlParser, LineParser};

#[test]
fn test_jsonl_parser_valid() {
    let parser = JsonlParser::new();
    let line =
        r#"{"level": "ERROR", "message": "Database failed", "timestamp": "2024-01-15T10:00:00Z"}"#;

    let result = parser.parse_line(line);
    assert!(result.is_ok());

    let data = result.unwrap();
    assert_eq!(data["level"], "ERROR");
    assert_eq!(data["message"], "Database failed");
}

#[test]
fn test_jsonl_parser_invalid() {
    let parser = JsonlParser::new();
    let line = r#"{"invalid": json syntax"#;

    let result = parser.parse_line(line);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Failed to parse JSONL"));
}

#[test]
fn test_jsonl_parser_empty() {
    let parser = JsonlParser::new();
    let line = "";

    let result = parser.parse_line(line);
    assert!(result.is_err());
}

#[test]
fn test_jsonl_parser_whitespace() {
    let parser = JsonlParser::new();
    let line = r#"  {"level": "INFO"}  "#;

    let result = parser.parse_line(line);
    assert!(result.is_ok());

    let data = result.unwrap();
    assert_eq!(data["level"], "INFO");
}

#[test]
fn test_csv_parser_basic() {
    let mut parser = CsvParser::new();

    // Set up headers
    let header_result = parser.parse_headers("timestamp,level,message");
    assert!(header_result.is_ok());

    // Parse data line
    let line = "2024-01-15T10:00:00Z,ERROR,Database failed";
    let result = parser.parse_line(line);
    assert!(result.is_ok());

    let data = result.unwrap();
    assert_eq!(data["timestamp"], "2024-01-15T10:00:00Z");
    assert_eq!(data["level"], "ERROR");
    assert_eq!(data["message"], "Database failed");
}

#[test]
fn test_csv_parser_quoted_fields() {
    let mut parser = CsvParser::new();

    // Set up headers
    let header_result = parser.parse_headers("name,message,status");
    assert!(header_result.is_ok());

    // Parse data line with quoted fields
    let line = r#"web01,"Connection failed, retrying",ERROR"#;
    let result = parser.parse_line(line);
    assert!(result.is_ok());

    let data = result.unwrap();
    assert_eq!(data["name"], "web01");
    assert_eq!(data["message"], "Connection failed, retrying");
    assert_eq!(data["status"], "ERROR");
}

#[test]
fn test_csv_parser_field_count_mismatch() {
    let mut parser = CsvParser::new();

    // Set up headers (3 fields)
    let header_result = parser.parse_headers("timestamp,level,message");
    assert!(header_result.is_ok());

    // Parse data line with wrong number of fields
    let line = "2024-01-15T10:00:00Z,ERROR"; // Only 2 fields
    let result = parser.parse_line(line);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("has 2 fields but expected 3"));
}

#[test]
fn test_csv_parser_no_headers() {
    let parser = CsvParser::new();

    // Try to parse without headers
    let line = "2024-01-15T10:00:00Z,ERROR,Failed";
    let result = parser.parse_line(line);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("CSV headers not initialized"));
}

#[test]
fn test_csv_parser_empty_headers() {
    let mut parser = CsvParser::new();

    // Try to set empty headers
    let result = parser.parse_headers("");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("CSV headers cannot be empty"));
}

#[test]
fn test_csv_parser_whitespace_handling() {
    let mut parser = CsvParser::new();

    // Set up headers with whitespace
    let header_result = parser.parse_headers(" timestamp , level , message ");
    assert!(header_result.is_ok());

    // Parse data line with whitespace
    let line = " 2024-01-15T10:00:00Z , ERROR , Database failed ";
    let result = parser.parse_line(line);
    assert!(result.is_ok());

    let data = result.unwrap();
    assert_eq!(data["timestamp"], "2024-01-15T10:00:00Z");
    assert_eq!(data["level"], "ERROR");
    assert_eq!(data["message"], "Database failed");
}
