# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Stelp is a command-line tool that processes text streams using Starlark scripts (Starlark Event and Line Processor). It provides a streaming text processing pipeline with Python-like syntax for transformation, filtering, and analysis of data.

## Common Development Commands

### Building and Testing
```bash
# Build the project
cargo build --release

# Run all tests
cargo test

# Run specific test file
cargo test integration_tests

# Run specific test
cargo test test_simple_transform

# Run tests with output
cargo test -- --nocapture

# Check code without building
cargo check

# Format code
cargo fmt

# Run clippy linter
cargo clippy

# Build and run with example
cargo run -- -e 'line.upper()' <<< "hello world"
```

### Running Examples
```bash
# Basic transformation
echo "hello world" | cargo run -- -e 'line.upper()'

# Using example script
cargo run -- -s examples/log_processor.star sample.log

# Filter and transform pipeline
seq 1 10 | cargo run -- --filter 'int(line) % 2 == 0' -e 'f"Even: {line}"'

# Apache Combined Log Format parsing
echo '192.168.1.1 - - [25/Dec/2021:10:24:56 +0000] "GET /api/status HTTP/1.1" 200 1234' | cargo run -- -e 'ts_str = regex_replace(r".*\[([^\]]+)\].*", r"\1", line); epoch = guess_ts(ts_str); format_ts(epoch, "%Y-%m-%d %H:%M:%S") + " " + line'

# Syslog format parsing 
echo "Dec 25 10:24:56 server1 nginx: 192.168.1.1 - GET /api/status" | cargo run -- -e 'ts_str = regex_replace(r"^(\w+ \d+ \d+:\d+:\d+).*", r"\1", line); epoch = guess_ts(ts_str); format_ts(epoch, "%Y-%m-%d %H:%M:%S") + " " + line'

# Derive examples for structured data transformation
echo -e "name,price,quantity\nAlice,10.50,3\nBob,25.00,2" | cargo run -- -f csv --derive 'total = float(price) * float(quantity)'

# Complex derive with field creation, modification, and deletion
echo -e "user,score,attempts,temp\nalice,85,3,debug\nbob,92,2,test" | cargo run -- -f csv --derive '
efficiency = float(score) / float(attempts)
grade = "A" if float(score) >= 90 else "B" if float(score) >= 80 else "C"
passed = float(score) >= 70
temp = None  # Delete field via None assignment
' 

# Using stelp_data for invalid identifiers and complex manipulation
echo -e "user,score,meta-data\nalice,85,debug\nbob,92,test" | cargo run -- -f csv --derive '
stelp_data["created_date"] = "2024-01-01"
stelp_data["meta-data"] = None  # Remove invalid identifier key
count = stelp_inc("processed")  # Use stelp_ prefixed functions
stelp_emit("Processing: " + user)  # Emit debug info
'

# Pattern extraction examples
echo "192.168.1.1 admin 200 1.5" | cargo run -- --extract-vars '{ip} {user} {status:int} {time:float}' --eval 'ip = data["ip"]; status = data["status"]; data = None; f"Request from {ip}: {status}"'

echo "doesn't match pattern" | cargo run -- --extract-vars '{ip} {user}' --eval 'data or "no match"'

echo "192.168.1.1 admin not_a_number" | cargo run -- --extract-vars '{ip} {user} {status:int}' --eval 'data or "conversion failed"' --debug

# Apache Combined Log Format parsing
echo '192.168.1.1 - - [25/Dec/2021:10:24:56 +0000] "GET /api/status HTTP/1.1" 200 1234' | cargo run -- --extract-vars '{ip} - - [{timestamp}] "{method} {path} {protocol}" {status:int} {size:int}' --filter 'data["status"] >= 400' -F jsonl

# System monitoring
printf "CPU: 85.2%% Memory: 76.1%%\nCPU: 45.0%% Memory: 62.3%%\nCPU: 92.1%% Memory: 88.9%%\n" | cargo run -- --extract-vars 'CPU: {cpu:float}% Memory: {memory:float}%' --filter 'data and data["cpu"] > 80.0' --eval 'cpu = data["cpu"]; data = None; f"High CPU: {cpu}%"'

# Mixed pipeline with filter and derive
echo -e "name,price,quantity\nAlice,10.50,3\nBob,25.00,2\nCharlie,5.00,1" | cargo run -- -f csv --filter 'float(data["price"]) > 10' --derive 'total = float(price) * float(quantity); discount = 0.1 if float(price) > 20 else 0'

# Remove keys at output stage
echo -e "name,price,quantity,debug\nAlice,10.50,3,temp\nBob,25.00,2,test" | cargo run -- -f csv --derive 'total = float(price) * float(quantity)' --remove-keys debug

# Column extraction with cols() function - klp-compatible
echo "alpha beta gamma delta epsilon" | cargo run -- -e 'cols(line, 0)'  # First column
echo "alpha beta gamma delta epsilon" | cargo run -- -e 'cols(line, -1)' # Last column

# Multiple column selection
echo "GET /api/users HTTP/1.1" | cargo run -- -e 'method, path, protocol = cols(line, 0, 1, 2); f"{method} -> {path}"'

# Column ranges and slices
echo "a b c d e f g" | cargo run -- -e 'cols(line, "1:3")'  # Columns 1-2 (b c)
echo "a b c d e f g" | cargo run -- -e 'cols(line, "2:")'   # From column 2 to end
echo "a b c d e f g" | cargo run -- -e 'cols(line, ":3")'   # From start to column 2

# Multiple indices as string
echo "alpha beta gamma delta" | cargo run -- -e 'cols(line, "0,2")'     # First and third columns
echo "alpha beta gamma delta" | cargo run -- -e 'cols(line, "-2,-1")'   # Last two columns

# Custom separators
echo "alice,25,engineer,remote" | cargo run -- -e 'name, age, role = cols(line, 0, 1, 2, sep=",")'
echo "a b c d" | cargo run -- -e 'cols(line, "0,2", outsep=":")'  # Custom output separator

# Structured data with cols()
echo '{"request": "GET /api/users HTTP/1.1"}' | cargo run -- -f jsonl --derive 'method = cols(request, 0); path = cols(request, 1)'

# Apache log processing with cols()
echo '192.168.1.1 - - [25/Dec/2021:10:24:56 +0000] "GET /api/status HTTP/1.1" 200 1234' | cargo run -- -e 'ip = cols(line, 0); method = cols(line, 4); f"Request from {ip}: {method}"'
```

## Architecture Overview

### Core Components

**Pipeline Architecture**: The system uses a streaming pipeline pattern where data flows through multiple processing stages:
- `StreamPipeline` - Main orchestrator that manages processors and handles I/O
- `RecordProcessor` trait - Interface for pipeline stages (filters, transformers)
- `RecordData` - Unified data model supporting both text and structured data
- `ProcessResult` - Enum defining pipeline stage outcomes (transform, skip, error, exit, etc.)

**Starlark Integration**: The tool embeds the Starlark language (Python subset) for user scripts:
- `StarlarkProcessor` - Executes transformation scripts
- `FilterProcessor` - Evaluates filter expressions  
- `DeriveProcessor` - Transforms structured data with injected field variables
- `GlobalVariables` - Manages persistent state across records
- Global functions (emit, skip, exit, inc) provide control flow

**Data Flow Model**:
1. Input → `InputFormatWrapper` → `RecordData`
2. `RecordData` → Pipeline stages → `ProcessResult`
3. `ProcessResult` → `OutputFormatter` → Output

### Key Modules

- `src/pipeline/` - Core pipeline processing logic
  - `stream.rs` - Main `StreamPipeline` orchestrator
  - `processors.rs` - Starlark and filter processors
  - `context.rs` - Processing context and record types
  - `global_functions.rs` - Built-in Starlark functions
  - `glob_dict.rs` - Global variable management
- `src/input_format.rs` - Input format parsing (JSON, CSV, logfmt)
- `src/output_format.rs` - Output format handling
- `src/variables.rs` - Global state management
- `src/main.rs` - CLI argument parsing and pipeline setup

### Important Patterns

**Error Handling**: Two strategies via `ErrorStrategy` enum:
- `Skip` - Continue processing on errors (default)
- `FailFast` - Stop on first error

**Global State**: Managed through `glob` dictionary in Starlark scripts:
```python
# Increment counter
count = inc("total")  # Uses prelude helper
# Or manually:
glob["counter"] = glob.get("counter", 0) + 1
```

**Control Flow**: Scripts can control pipeline execution:
- `emit("text")` - Output additional lines
- `skip()` - Skip current record
- `exit(code=0, msg=None)` - Terminate processing with exit code
  - `exit()` - Exit with code 0 (success)
  - `exit(3)` - Exit with code 3  
  - `exit("error message")` - Exit with code 0 and message (backward compatibility)
  - `exit(1, "error occurred")` - Exit with code 1 and message

**F-string Limitations**: F-strings in Starlark are very limited and only support atomic values (plain variables). No dot access, brackets, or function calls are allowed - extract to variables first:
```python
# Wrong: f"Count: {glob.get('counter')}"
# Wrong: f"Name: {data['name']}" 
# Wrong: f"Upper: {line.upper()}"
# Right:
count = glob.get("counter")
name = data["name"]
upper_line = line.upper()
f"Count: {count}, Name: {name}, Upper: {upper_line}"
```

**Meta Variables**: Available in scripts as globals:
- `LINENUM` - Current line number (1-based)
- `FILENAME` - Current filename or None
- `RECNUM` - Record number within file
- `line` - Current line text (for text records)
- `data` - Structured data (for JSON/CSV records)

## Test Structure

Tests are organized in the `tests/` directory:
- Integration tests in `tests/integration_tests.rs` show complete pipeline usage
- Each test typically creates a `StreamPipeline`, adds processors, and processes sample input
- Test helper pattern: Create config → Create pipeline → Add processors → Process input → Assert results

When writing tests, use `Cursor::new()` for string input and `Vec<u8>` for output capture.

## Starlark Script Development

Scripts have access to:
- Standard Starlark functions
- Built-in functions: `emit()`, `skip()`, `exit()`, `inc()`
- Exit function: `exit(code=0, msg=None)` - terminate processing with exit code
- Regex functions: `regex_match()`, `regex_replace()`, `regex_find_all()`
- JSON functions: `parse_json()`, `dump_json()`
- CSV functions: `parse_csv()`, `dump_csv()`
- Timestamp functions: `parse_ts()`, `format_ts()`, `now()`, `ts_diff()`, `ts_add()`, `guess_ts()`
- Column extraction: `cols()` - klp-compatible column extraction with slice and multi-index support
- Global state via `glob` dictionary
- Meta variables: `LINENUM`, `FILENAME`, `RECNUM`

The `src/prelude.star` file is automatically included and provides helper functions like `inc()` for counter management.

## Derive Mode

The `--derive` feature provides ergonomic structured data transformation by automatically injecting data dict keys as Starlark variables.

### Key Features

**Variable Injection**: Data fields become direct variables:
```python
# CSV with columns: name,price,quantity
total = price * quantity  # Direct access instead of data["price"] * data["quantity"]
```

**Conflict Resolution**: All Stelp functionality uses `stelp_` prefix:
```python
# Data variables have clean namespace
user_count = stelp_inc("users")      # Stelp counter function
file_info = stelp_FILENAME           # Stelp meta variable  
stelp_emit("Debug: " + name)         # Stelp emit function
state = stelp_glob["app_state"]      # Stelp global state
```

**Field Manipulation**:
```python
# Field creation/modification via direct assignment
total = price * quantity
category = "expensive" if price > 100 else "affordable"

# Field deletion via None assignment
temp_field = None  # Removes temp_field from output

# Invalid identifiers via stelp_data
stelp_data["invalid-key"] = "value"        # Keys with dashes, spaces
stelp_data["nested"] = {"config": "value"} # Nested structures
stelp_data["meta-data"] = None             # Delete invalid identifier keys
```

**Output Filtering**: Remove keys at output stage:
```bash
stelp --derive 'total = price * qty' --remove-keys temp,debug data.csv
```

### Requirements

- **Structured data only**: Use `-f csv/jsonl/etc` 
- **Valid identifiers**: Data keys must be valid Starlark variable names
- **Dict format**: Arrays and primitives not supported