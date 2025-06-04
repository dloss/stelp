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
stelp -e 'st_parse_csv(line)[0]' -e 'line.upper()' data.csv

# Filter and transform
stelp --filter 'len(line) > 5' -e 'line.upper()' input.txt

# Multi-file processing with meta variables
filename = meta_filename if meta_filename else "stdin"
stelp -e 'f"[{filename}:{meta_linenum}] {line.upper()}"' *.log
```

## Core Features

- **Line-by-line processing** with Python-like syntax via Starlark
- **Code reuse** via `-I/--include` for shared functions and constants
- **Multi-step pipelines** with `--eval` and `--filter` chaining
- **Rich built-ins** for regex, JSON, CSV, and text processing
- **Context awareness** via `meta_linenum`, `meta_filename`, etc.
- **Global state** that persists across lines and files
- **Output control** - transform, emit multiple lines, filter, or exit early

## Built-in Functions

### Text Processing
```python
line.upper()                                    # Standard string methods
st_regex_match(r'\d+', line)                   # Regex matching
st_regex_replace(r'\d+', 'NUM', line)          # Regex replacement
st_regex_find_all(r'\w+', line)                # Find all matches
```

### Data Formats
```python
data = st_parse_json(line)                     # Parse JSON
st_to_json({"key": "value"})                   # Generate JSON
fields = st_parse_csv(line, delimiter=",")     # Parse CSV
st_to_csv(["a", "b", "c"])                     # Generate CSV
```

### Context & State
```python
meta_linenum                                   # Current line number  
meta_filename                                  # Current file name
st_get_global("counter", 0)                   # Get global variable
st_set_global("counter", 42)                  # Set global variable
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
    return st_regex_replace(r'\s+', ' ', text.strip())

def parse_timestamp(line):
    matches = st_regex_find_all(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', line)
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
      -e 'st_regex_replace(r"\[ERROR\]", "[🔴]", line)' \
      -e 'line_num = str(meta_linenum); f"[{line_num}] {line}"' \
      error.log
```

### Global State & Counting
```bash
stelp -e '
count = st_get_global("total", 0) + 1
st_set_global("total", count)
if "ERROR" in line:
    error_count = st_get_global("errors", 0) + 1  
    st_set_global("errors", error_count)
    emit(f"🚨 Error #{error_count}: {line}")
f"[{count}] {line}"
' server.log
```

### JSON Processing
```bash
# Extract and transform JSON fields
echo '{"user": "alice", "action": "login"}' | \
stelp -e '
data = st_parse_json(line)
user = data["user"]
action = data["action"]
f"{user} performed {action}"
'
```

### CSV Processing  
```bash
# Process CSV with headers
stelp -e '
if meta_linenum == 1:
    line  # Keep header
else:
    fields = st_parse_csv(line)
    age = int(fields[1])  # Age column
    if age >= 18:
        st_to_csv([fields[0], "adult"])
    else:
        skip()
' users.csv
```

### Log Analysis with Early Exit
```bash
stelp -e '
if "FATAL" in line:
    line_num = str(meta_linenum)
    emit(f"💀 Fatal error at line {line_num}: {line}")
    exit("Processing stopped due to fatal error")
elif "ERROR" in line:
    error_count = st_get_global("errors", 0) + 1
    st_set_global("errors", error_count)
    f"Error #{error_count}: {line}"
else:
    line
' application.log
```

### Multi-file Processing
```bash
# Process multiple files with accumulated state
stelp -e '
filename = meta_filename if meta_filename else "stdin"
file_lines = st_get_global(f"lines_{filename}", 0) + 1
st_set_global(f"lines_{filename}", file_lines)

total_lines = st_get_global("total_lines", 0) + 1
st_set_global("total_lines", total_lines)

line_num = str(meta_linenum)
f"[{filename}:{line_num}] (file: {file_lines}, total: {total_lines}) {line}"
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
count = st_get_global(f"{category}_count", 0) + 1
st_set_global(f"{category}_count", count)

if category == "error":
    emit(f"🔴 Error #{count}: {line}")

category_upper = category.upper()
result = f"[{category_upper}:{count}] {line}"
result
```

```bash
stelp -s process_logs.star server.log
```

## Context Variables

The `meta` variables provide context about the current processing state:

```python
meta_linenum           # Current line number (1-based)
meta_filename          # Current filename or None for stdin
meta_line_number       # Alias for linenum  
meta_record_count      # Records processed in current file
meta_file_name         # Alias for filename
```

Use in f-strings or regular expressions:
```python
line_num = str(meta_linenum)
filename = meta_filename if meta_filename else "stdin"
f"Line {line_num} in {filename}: {line}"

record_num = str(meta_record_count)
f"Processing record {record_num}"
```

## F-String Limitations

Starlark f-strings have some limitations compared to Python. Complex expressions need to be assigned to variables first:

```python
# ❌ This doesn't work:
f"User {data['user']} performed {data['action']}"

# ✅ This works:
user = data["user"]
action = data["action"]
f"User {user} performed {action}"

# ❌ This doesn't work:
f"[{category.upper()}:{count}] {line}"

# ✅ This works:
category_upper = category.upper()
f"[{category_upper}:{count}] {line}"
```

## Variable Scopes

- **Local variables**: Reset for each line (`parts = line.split()`)
- **Global variables**: Persist across lines (`st_get_global()`, `st_set_global()`)  
- **Meta variables**: Context information (`meta_linenum`, `meta_filename`)
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
seq 1 100 | stelp -e 'line_num = str(meta_linenum); f"Item {line}: {line_num}"'

# Parse Apache logs
stelp -e 'st_regex_replace(r"(\d+\.\d+\.\d+\.\d+).*", r"IP: \1", line)' access.log

# Count patterns across files  
stelp -e '
if st_regex_match(r"ERROR", line):
    st_set_global("errors", st_get_global("errors", 0) + 1)

error_count = str(st_get_global("errors", 0))
f"Total errors so far: {error_count}"
' *.log

# CSV transformation
echo -e "name,age\nAlice,25\nBob,30" | stelp -e '
if meta_linenum == 1:
    line + ",category"
else:
    fields = st_parse_csv(line)
    age = int(fields[1])
    category = "senior" if age >= 30 else "junior"
    st_to_csv([fields[0], fields[1], category])
'
```

## License

MIT