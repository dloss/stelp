// tests/keys_option_tests.rs

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_keys_option_jsonl() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-k")
        .arg("name,age")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC","job":"engineer"}"#)
        .assert()
        .success()
        .stdout(r#"{"name":"alice","age":30}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_csv() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-F")
        .arg("csv")
        .arg("-k")
        .arg("name,job")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC","job":"engineer"}"#)
        .assert()
        .success()
        .stdout("name,job\nalice,engineer\n");
}

#[test]
fn test_keys_option_logfmt() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("logfmt")
        .arg("-k")
        .arg("level,msg")
        .write_stdin("level=ERROR msg=\"Database failed\" user=alice timestamp=2024-01-15")
        .assert()
        .success()
        .stdout("level=ERROR msg=\"Database failed\"\n");
}

#[test]
fn test_keys_option_key_order() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-k")
        .arg("age,name")  // Different order than input
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}"#)
        .assert()
        .success()
        .stdout(r#"{"age":30,"name":"alice"}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_missing_keys() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-k")
        .arg("name,missing,age")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}"#)
        .assert()
        .success()
        .stdout(r#"{"name":"alice","age":30}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_single_key() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-k")
        .arg("name")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}"#)
        .assert()
        .success()
        .stdout(r#"{"name":"alice"}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_with_filter() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("--filter")
        .arg(r#"data["age"] > 25"#)
        .arg("-k")
        .arg("name,age")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}
{"name":"bob","age":20,"city":"LA"}"#)
        .assert()
        .success()
        .stdout(r#"{"name":"alice","age":30}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_with_transform() {
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-e")
        .arg(r#"data = {"name": data["name"].upper(), "age": data["age"]}"#)
        .arg("-k")
        .arg("name")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}"#)
        .assert()
        .success()
        .stdout(r#"{"name":"ALICE"}"#.to_string() + "\n");
}

#[test]
fn test_keys_option_no_keys_no_filtering() {
    // Without --keys, all fields should be preserved
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}"#)
        .assert()
        .success()
        .stdout(predicate::str::contains("name"))
        .stdout(predicate::str::contains("age"))
        .stdout(predicate::str::contains("city"));
}

#[test]
fn test_keys_option_empty_keys() {
    // Test edge case with empty keys (should show no fields)
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-k")
        .arg("")
        .write_stdin(r#"{"name":"alice","age":30}"#)
        .assert()
        .success()
        .stdout("{}\n");
}

#[test]
fn test_keys_option_csv_headers() {
    // Test that CSV headers respect key order
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-f")
        .arg("jsonl")
        .arg("-F")
        .arg("csv")
        .arg("-k")
        .arg("age,name")
        .write_stdin(r#"{"name":"alice","age":30,"city":"NYC"}
{"name":"bob","age":25,"city":"LA"}"#)
        .assert()
        .success()
        .stdout("age,name\n30,alice\n25,bob\n");
}