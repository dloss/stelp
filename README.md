# Stelp

A CLI tool for processing text streams with Starlark (Python-like) scripts.

> [!WARNING]
> Experimental software. APIs may change without notice.

## Quick Start

```bash
# Basic transformation
echo "hello world" | stelp -e 'line.upper()'
# Output: HELLO WORLD

# Multiple transformations
stelp -e 'line.split(",")[0]' -e 'line.upper()' data.csv

# Filter and transform
stelp --filter 'len(line) > 5' -e 'line.upper()' input.txt

# Multi-file processing
stelp -e 'f"[{FILENAME}:{LINENUM}] {line.upper()}"' *.log
```

## Core Features

- **Line-by-line processing** with Python-like syntax via Starlark
- **Code reuse** via `-I/--include` for shared functions and constants
- **Multi-step pipelines** with `--eval` and `--filter` chaining
- **Rich built-ins** for regex, JSON, CSV, and text processing
- **Context awareness** via `LINENUM`, `FILENAME`, `RECNUM` variables
- **Global state** that persists across lines and files
- **Output control** - transform, emit multiple lines, filter, or exit early

## Built-in Functions

### Text Processing
```python
line.upper()                                    # Standard string methods
regex_match(r'\d+', line)                      # Regex matching
regex_replace(r'\d+', 'NUM', line)             # Regex replacement
regex_find_all(r'\w+', line)                   # Find all matches
```

### Data Formats
```python
data = parse_json(line)                        # Parse JSON
to_json({"key": "value"})                      # Generate JSON
fields = parse_csv(line, delimiter=",")        # Parse CSV
to_csv(["a", "b", "c"])                        # Generate CSV
```

### Context & State
```python
LINENUM                                        # Current line number  
FILENAME                                       # Current file name
RECNUM                                         # Record number in file
get_global("counter", 0)                       # Get global variable
set_global("counter", 42)                      # Set global variable
```

### Output Control
```python
emit("extra output line")                      # Output additional line
skip()                                         # Skip current line
exit("processing complete")                    # Stop processing
print("debug info")                           # Debug to stderr
```

## Command Line

```bash
stelp [OPTIONS] [FILES...]

# Core options
-e, --eval <EXPR>        Evaluation expression (can be repeated)
    --filter <EXPR>      Filter expression (can be repeated)  
-I, --include <FILE>     Include Starlark file (can be repeated)
-s, --script <FILE>      Script file
-o, --output <FILE>      Output file (default: stdout)
    --debug              Show processing details
    --fail-fast          Stop on first error
```

## Advanced Usage

### Shared Libraries
Create reusable code with `-I/--include`:

```python
# utils.star
def clean_line(text):
    return regex_replace(r'\s+', ' ', text.strip())

def parse_timestamp(line):
    matches = regex_find_all(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', line)
    return matches[0] if matches else None

ERROR_THRESHOLD = 100
```

```bash
# Use shared functions
stelp -I utils.star -e 'clean_line(line)' messy.txt
stelp -I utils.star -e 'parse_timestamp(line) or "no timestamp"' logs.txt
```

### Pipeline Processing
```bash
# Multi-step pipeline (processed in order)
stelp --filter '"ERROR" in line' \
      -e 'regex_replace(r"\[ERROR\]", "[🔴]", line)' \
      -e 'f"[{LINENUM}] {line}"' \
      error.log
```

### Global State & Counting
```bash
stelp -e '
count = get_global("total", 0) + 1
set_global("total", count)
if "ERROR" in line:
    error_count = get_global("errors", 0) + 1  
    set_global("errors", error_count)
    emit(f"🚨 Error #{error_count}: {line}")
f"[{count}] {line}"
' server.log
```

### JSON Processing
```bash
# Extract and transform JSON fields
echo '{"user": "alice", "action": "login"}' | \
stelp -e '
data = parse_json(line)
f"{data[\"user\"]} performed {data[\"action\"]}"
'
```

### CSV Processing  
```bash
# Process CSV with headers
stelp -e '
if LINENUM == 1:
    line  # Keep header
else:
    fields = parse_csv(line)
    if int(fields[1]) >= 18:  # Age column
        to_csv([fields[0], "adult"])
    else:
        skip()
' users.csv
```

### Log Analysis with Early Exit
```bash
stelp -e '
if "FATAL" in line:
    emit(f"💀 Fatal error at line {LINENUM}: {line}")
    exit("Processing stopped due to fatal error")
elif "ERROR" in line:
    error_count = get_global("errors", 0) + 1
    set_global("errors", error_count)
    f"Error #{error_count}: {line}"
else:
    line
' application.log
```

### Multi-file Processing
```bash
# Process multiple files with accumulated state
stelp -e '
file_lines = get_global(f"lines_{FILENAME}", 0) + 1
set_global(f"lines_{FILENAME}", file_lines)

total_lines = get_global("total_lines", 0) + 1
set_global("total_lines", total_lines)

f"[{FILENAME}:{LINENUM}] (file: {file_lines}, total: {total_lines}) {line}"
' file1.txt file2.txt file3.txt
```

## Script Files

For complex processing, use script files:

```python
# process_logs.star
def categorize_level(line):
    if "ERROR" in line:
        return "error"
    elif "WARN" in line:
        return "warning"  
    else:
        return "info"

# Main processing
category = categorize_level(line)
count = get_global(f"{category}_count", 0) + 1
set_global(f"{category}_count", count)

if category == "error":
    emit(f"🔴 Error #{count}: {line}")
    
f"[{category.upper()}:{count}] {line}"
```

```bash
stelp -s process_logs.star server.log
```

## Context Variables

Context variables provide information about the current processing state:

```python
LINENUM           # Current line number (1-based)
FILENAME          # Current filename or None for stdin
RECNUM            # Record number within current file (1-based)
```

Use directly in f-strings or expressions:
```python
f"Line {LINENUM} in {FILENAME}: {line}"
f"Processing record {RECNUM}"
```

## Variable Scopes

- **Local variables**: Reset for each line (`parts = line.split()`)
- **Global variables**: Persist across lines (`get_global()`, `set_global()`)  
- **Meta variables**: Context information (`LINENUM`, `FILENAME`, `RECNUM`)
- **Shared functions**: Defined in include files (`-I utils.star`)

## Exit Codes

- `0`: Success (some output produced)
- `1`: Processing errors occurred  
- `2`: No output produced

## Installation

```bash
git clone <repository>
cd stelp  
cargo build --release
# Binary: target/release/stelp
```

## Examples

```bash
# Generate test data
seq 1 100 | stelp -e 'f"Item {line}: {LINENUM}"'

# Parse Apache logs
stelp -e 'regex_replace(r"(\d+\.\d+\.\d+\.\d+).*", r"IP: \1", line)' access.log

# Count patterns across files  
stelp -e '
if regex_match(r"ERROR", line):
    set_global("errors", get_global("errors", 0) + 1)
    
f"Total errors so far: {get_global(\"errors\", 0)}"
' *.log

# CSV transformation
echo -e "name,age\nAlice,25\nBob,30" | stelp -e '
if LINENUM == 1:
    line + ",category"
else:
    fields = parse_csv(line)
    category = "senior" if int(fields[1]) >= 30 else "junior"
    to_csv([fields[0], fields[1], category])
'
```

## License

MIT