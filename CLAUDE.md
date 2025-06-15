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
- `exit("message")` - Terminate processing

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
- Regex functions: `regex_match()`, `regex_replace()`, `regex_find_all()`
- JSON functions: `parse_json()`, `dump_json()`
- CSV functions: `parse_csv()`, `dump_csv()`
- Timestamp functions: `parse_ts()`, `format_ts()`, `now()`, `ts_diff()`, `ts_add()`, `guess_ts()`
- Global state via `glob` dictionary
- Meta variables: `LINENUM`, `FILENAME`, `RECNUM`

The `src/prelude.star` file is automatically included and provides helper functions like `inc()` for counter management.