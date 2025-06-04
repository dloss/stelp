// tests/readme_smoke_test.rs
// Comprehensive smoke test for all major README functionality

use assert_cmd::Command;
use predicates::prelude::*;

/// Comprehensive smoke test that exercises all major README features
#[test]
fn test_comprehensive_readme_smoke_test() {
    println!("=== Running comprehensive README smoke test ===");
    
    // Test 1: Basic transformation
    println!("Testing basic transformation...");
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-e")
        .arg("line.upper()")
        .write_stdin("hello world")
        .assert()
        .success()
        .stdout("HELLO WORLD\n");
    
    // Test 2: JSON processing
    println!("Testing JSON processing...");
    let mut cmd2 = Command::cargo_bin("stelp").unwrap();
    cmd2.arg("-e")
        .arg(r#"
data = st_parse_json(line)
f"User: {data}, Action: processed"
        "#)
        .write_stdin(r#"{"user": "alice", "action": "login"}"#)
        .assert()
        .success()
        .stdout(predicate::str::contains("User:"))
        .stdout(predicate::str::contains("Action: processed"));
    
    // Test 3: CSV processing
    println!("Testing CSV processing...");
    let mut cmd3 = Command::cargo_bin("stelp").unwrap();
    cmd3.arg("-e")
        .arg(r#"
fields = st_parse_csv(line)
name = fields[0]
age = fields[1]
f"Name: {name}, Age: {age}"
        "#)
        .write_stdin("Alice,25\nBob,30")
        .assert()
        .success()
        .stdout("Name: Alice, Age: 25\nName: Bob, Age: 30\n");
    
    // Test 4: Global state
    println!("Testing global state...");
    let mut cmd4 = Command::cargo_bin("stelp").unwrap();
    cmd4.arg("-e")
        .arg(r#"
count = st_get_global("counter", 0) + 1
st_set_global("counter", count)
f"Line {count}: {line}"
        "#)
        .write_stdin("first\nsecond\nthird")
        .assert()
        .success()
        .stdout("Line 1: first\nLine 2: second\nLine 3: third\n");
    
    // Test 5: Emit and skip
    println!("Testing emit and skip...");
    let mut cmd5 = Command::cargo_bin("stelp").unwrap();
    cmd5.arg("-e")
        .arg(r#"
if "emit" in line:
    emit("Found emit line")
    line.upper()
elif "skip" in line:
    skip()
else:
    line.upper()
        "#)
        .write_stdin("normal\nemit this\nskip this\nnormal again")
        .assert()
        .success()
        .stdout(predicate::str::contains("normal"))
        .stdout(predicate::str::contains("Found emit line"))
        .stdout(predicate::str::contains("emit this"))
        .stdout(predicate::str::contains("normal again"))
        .stdout(predicate::str::contains("skip this").not());
    
    // Test 6: Meta variables
    println!("Testing meta variables...");
    let mut cmd6 = Command::cargo_bin("stelp").unwrap();
    cmd6.arg("-e")
        .arg("f\"Line {meta_linenum}: {line}\"")
        .write_stdin("first\nsecond")
        .assert()
        .success()
        .stdout("Line 1: first\nLine 2: second\n");
    
    // Test 7: Filter and transform pipeline
    println!("Testing filter pipeline...");
    let mut cmd7 = Command::cargo_bin("stelp").unwrap();
    cmd7.arg("--filter")
        .arg("len(line) > 3")
        .arg("-e")
        .arg("line.upper()")
        .write_stdin("hi\nhello\nworld\nok")
        .assert()
        .success()
        .stdout("HELLO\nWORLD\n");

    println!("✅ All README smoke tests passed!");
}

/// Test that demonstrates README's main value proposition
#[test]  
fn test_readme_value_proposition() {
    println!("=== Testing README value proposition ===");
    
    // Demonstrate: "Process text streams with Python-like syntax"
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-e")
        .arg("line.upper()")
        .write_stdin("hello world")
        .assert()
        .success()
        .stdout("HELLO WORLD\n");
    
    // Demonstrate: "Multi-step pipelines"
    let mut cmd2 = Command::cargo_bin("stelp").unwrap();
    cmd2.arg("--filter")
        .arg("len(line) > 5")
        .arg("-e")
        .arg("line.upper()")
        .write_stdin("short\nvery long line\nno")
        .assert()
        .success()
        .stdout("VERY LONG LINE\n");
    
    println!("✅ README value proposition demonstrated!");
}