// tests/integration_global_namespace.rs
use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_global_namespace_integration() {
    println!("=== Integration Test: Global Namespace ===");
    
    // Test 1: Basic transformation
    println!("Testing basic transformation...");
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-e")
        .arg("line.upper()")
        .write_stdin("hello world")
        .assert()
        .success()
        .stdout("HELLO WORLD\n");
    println!("✅ Basic transformation works");
    
    // Test 2: Meta variables in f-strings  
    println!("Testing meta variables in f-strings...");
    let mut cmd2 = Command::cargo_bin("stelp").unwrap();
    cmd2.arg("-e")
        .arg("f\"Line {LINENUM}: {line}\"")
        .write_stdin("test line")
        .assert()
        .success()
        .stdout("Line 1: test line\n");
    println!("✅ Meta variables in f-strings work");
    
    // Test 3: Global state functions
    println!("Testing global state functions...");
    let mut cmd3 = Command::cargo_bin("stelp").unwrap();
    cmd3.arg("-e")
        .arg(r#"
count = get_global("counter", 0) + 1
set_global("counter", count)
f"Count: {count}"
        "#)
        .write_stdin("line1\nline2")
        .assert()
        .success()
        .stdout("Count: 1\nCount: 2\n");
    println!("✅ Global state functions work");
    
    // Test 4: Regex functions without prefix
    println!("Testing regex functions without prefix...");
    let mut cmd4 = Command::cargo_bin("stelp").unwrap();
    cmd4.arg("-e")
        .arg(r#"regex_replace(r"\d+", "NUM", line) if regex_match(r"\d+", line) else line"#)
        .write_stdin("test123\nhello")
        .assert()
        .success()
        .stdout("testNUM\nhello\n");
    println!("✅ Regex functions work without prefix");
    
    // Test 5: Emit and control flow - FIXED LOGIC
    println!("Testing emit and control flow...");
    let mut cmd5 = Command::cargo_bin("stelp").unwrap();
    cmd5.arg("-e")
        .arg(r#"
result = line.upper()
if "emit" in line:
    emit("Found: " + line)
elif "skip" in line:
    skip()
result
        "#)
        .write_stdin("normal\nemit this\nskip this\nnormal2")
        .assert()
        .success()
        .stdout(predicate::str::contains("NORMAL"))
        .stdout(predicate::str::contains("Found: emit this"))
        .stdout(predicate::str::contains("EMIT THIS"))
        .stdout(predicate::str::contains("NORMAL2"))
        .stdout(predicate::str::contains("skip this").not());
    println!("✅ Emit and control flow work");
    
    println!("✅ All global namespace integration tests pass!");
}

#[test]
fn test_no_namespace_pollution() {
    println!("=== Testing No Namespace Pollution ===");
    
    // Verify that global functions don't require st_ prefix
    let mut cmd = Command::cargo_bin("stelp").unwrap();
    cmd.arg("-e")
        .arg("get_global('test', 'default')")
        .write_stdin("line")
        .assert()
        .success()
        .stdout("default\n");
    
    // Verify that meta variables are directly accessible
    let mut cmd2 = Command::cargo_bin("stelp").unwrap();
    cmd2.arg("-e")
        .arg("str(LINENUM)")
        .write_stdin("line")
        .assert()
        .success()
        .stdout("1\n");
    
    println!("✅ No namespace pollution - functions are in global scope");
}