use assert_cmd::Command;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_logfmt_auto_detection() {
    // Create test JSON data
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        r#"{{"ts":"2024-01-01T10:00:00Z","level":"info","msg":"Server started"}}"#
    )
    .unwrap();
    writeln!(
        temp_file,
        r#"{{"ts":"2024-01-01T10:00:01Z","level":"error","msg":"Connection failed"}}"#
    )
    .unwrap();

    // Test with forced no-color (should be plain logfmt)
    let output = Command::cargo_bin("stelp")
        .unwrap()
        .arg("--no-color")
        .arg("-f")
        .arg("jsonl")
        .arg(temp_file.path())
        .output()
        .expect("Failed to execute stelp");

    let result = String::from_utf8(output.stdout).unwrap();

    // Should contain logfmt output without ANSI codes
    assert!(result.contains("level=info"));
    assert!(result.contains("level=error"));
    assert!(result.contains("ts="));
    assert!(result.contains("msg="));
    assert!(!result.contains("\x1b["));
}

#[test]
fn test_color_forcing() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, r#"{{"level":"error","msg":"test"}}"#).unwrap();

    // Test forced color output
    let output = Command::cargo_bin("stelp")
        .unwrap()
        .arg("--color")
        .arg("-f")
        .arg("jsonl")
        .arg(temp_file.path())
        .output()
        .expect("Failed to execute stelp");

    let result = String::from_utf8(output.stdout).unwrap();

    // Should contain ANSI color codes
    assert!(result.contains("\x1b["));
    // Both should contain the key and value (though possibly separated by color codes)
    assert!(result.contains("level"));
    assert!(result.contains("error"));
}

#[test]
fn test_field_priority_ordering() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, r#"{{"zebra":"last","timestamp":"2024-01-01T10:00:00Z","level":"error","message":"test","alpha":"middle"}}"#).unwrap();

    let output = Command::cargo_bin("stelp")
        .unwrap()
        .arg("--no-color")
        .arg("-f")
        .arg("jsonl")
        .arg(temp_file.path())
        .output()
        .expect("Failed to execute stelp");

    let result = String::from_utf8(output.stdout).unwrap();
    let parts: Vec<&str> = result.trim().split_whitespace().collect();

    // timestamp should come first, then level, then message
    assert!(parts[0].starts_with("timestamp="));
    assert!(parts[1].starts_with("level="));
    assert!(parts[2].starts_with("message="));
    // alpha should come before zebra (alphabetical)
    assert!(result.find("alpha=").unwrap() < result.find("zebra=").unwrap());
}

#[test]
fn test_quoting_behavior() {
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(
        temp_file,
        r#"{{"simple":"value","spaced":"has spaces","empty":"","quoted":"has\"quotes"}}"#
    )
    .unwrap();

    let output = Command::cargo_bin("stelp")
        .unwrap()
        .arg("--no-color")
        .arg("-f")
        .arg("jsonl")
        .arg(temp_file.path())
        .output()
        .expect("Failed to execute stelp");

    let result = String::from_utf8(output.stdout).unwrap();

    assert!(result.contains("simple=value")); // No quotes needed
    assert!(result.contains("spaced=\"has spaces\"")); // Quotes due to space
    assert!(result.contains("empty=\"\"")); // Quotes due to empty
    assert!(result.contains("quoted=\"has\\\"quotes\"")); // Escaped quotes
}
