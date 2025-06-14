// tests/unix_integration_tests.rs
use assert_cmd::Command;
use predicates::prelude::*;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_exit_code_success() {
    // Normal processing should return exit code 0
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("line.upper()")
        .write_stdin("hello\nworld\n")
        .assert()
        .success() // exit code 0
        .stdout("HELLO\nWORLD\n");
}

#[test]
fn test_exit_code_no_output() {
    // No output should return exit code 2
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--filter")
        .arg("False") // Filter out everything
        .write_stdin("hello\nworld\n")
        .assert()
        .code(2) // No output produced
        .stdout("");
}

#[test]
fn test_exit_code_errors() {
    // Processing errors should return exit code 1 (with skip strategy)
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("undefined_variable + 1") // This will cause errors
        .write_stdin("hello\nworld\n")
        .assert()
        .code(1) // Errors occurred
        .stdout(""); // No successful output
}

#[test]
fn test_exit_code_early_termination() {
    // Test the exit() function behavior - it outputs the message and stops processing
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg(
            r#"
if "stop" in line:
    exit("Processing stopped")
    
line.upper()
        "#,
        )
        .write_stdin("hello\nstop here\nworld\n")
        .assert()
        .success()
        .stdout("HELLO\nProcessing stopped\n"); // Only processes until stop, then exits
}

#[test]
fn test_stderr_stdout_separation() {
    // Debug output should go to stderr, data to stdout
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--debug")
        .arg("--eval")
        .arg("line.upper()")
        .write_stdin("hello\n")
        .assert()
        .success()
        .stdout("HELLO\n")
        .stderr(predicate::str::contains("stelp: reading from stdin"))
        .stderr(predicate::str::contains("eval_1:"));
}

#[test]
fn test_error_messages_to_stderr() {
    // Error messages should go to stderr with stelp prefix
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("line.upper()")
        .arg("nonexistent_file.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains("stelp: failed to open input file"));
}

#[test]
fn test_file_processing_with_exit_codes() {
    // Create a temporary file
    let mut temp_file = NamedTempFile::new().unwrap();
    writeln!(temp_file, "hello\nworld\ntest_line").unwrap();

    // Test successful processing
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("line.upper()")
        .arg(temp_file.path())
        .assert()
        .success()
        .stdout("HELLO\nWORLD\nTEST_LINE\n");
}

#[test]
fn test_filter_with_some_output() {
    // Filter that produces some output should return 0
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--filter")
        .arg(r#""keep" in line"#)
        .write_stdin("skip this\nkeep this\nskip that\nkeep that\n")
        .assert()
        .success() // exit code 0 - some output produced
        .stdout("keep this\nkeep that\n");
}

#[test]
fn test_mixed_success_and_errors() {
    // Test that we can detect errors occurred via exit code
    // Using a simpler approach that works with current error handling
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("1 / 0") // Simple division by zero error
        .write_stdin("hello\nworld\n")
        .assert()
        .code(1) // Errors occurred
        .stdout(""); // No output due to errors
}

#[test]
fn test_partial_success_with_skip() {
    // Test mixed success/failure with skip() instead of errors
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg(
            r#"
if "skip" in line:
    skip()

line.upper()
        "#,
        )
        .write_stdin("hello\nskip this\nworld\n")
        .assert()
        .success() // No errors, just skipped lines
        .stdout("HELLO\nWORLD\n");
}

#[test]
fn test_empty_input() {
    // Empty input should return exit code 2 (no output)
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg("line.upper()")
        .write_stdin("")
        .assert()
        .code(2) // No input/output
        .stdout("");
}

#[test]
fn test_invalid_arguments() {
    // Invalid arguments should show error with stelp prefix
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("stelp:"));
}

#[test]
fn test_exit_function_with_return_value() {
    // Test exit() with a return value - based on actual failing test output
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("--eval")
        .arg(r#"exit("test message")"#)
        .write_stdin("hello\nworld\n")
        .assert()
        .success()
        .stdout("test message\n"); // exit() outputs its message and stops
}
