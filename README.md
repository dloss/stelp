# Stelp

A high-performance CLI tool that processes stdin line-by-line using Starlark (Python-like) scripts.

## Features

- **Line-by-line processing** with Starlark transformation scripts
- **Multi-step pipelines** with global state management
- **Rich built-in functions** for text processing, regex, JSON, CSV
- **Flexible output control** - transform, emit multiple lines, filter, or terminate
- **Error handling** with skip or fail-fast strategies
- **Performance focused** - 10K-50K lines/second for simple transformations
- **Multi-file processing** - process multiple files with accumulated statistics

## Installation

```bash
git clone <repository>
cd stelp
cargo build --release
# Binary will be available at target/release/stelp
```

## Quick Start

### Basic Usage

```bash
# Simple transformation
echo "hello world" | stelp --eval 'line.upper()'
# Output: HELLO WORLD

# Process files directly
stelp --eval 'line.upper()' input.txt

# Multiple files
stelp --eval 'line.upper()' file1.txt file2.txt file3.txt

# Multiple evaluation expressions
stelp --eval 'line.split(",")[0]' --eval 'line.upper()' data.csv

# Using script files
stelp -f script.star input1.txt input2.txt
```

### Advanced Examples

#### Global Variables and Counting
```bash
stelp --eval '
count = st.get_global("total", 0) + 1
st.set_global("total", count)
f"Line {count}: {line}"
' data.txt
```

#### Filtering and Multi-line Output
```bash
stelp --eval '
if "ERROR" in line:
    emit(f"🚨 {line}")
    emit("---")
    skip()
else:
    line.upper()
' logs.txt
```

#### CSV Processing
```bash
stelp --eval '
fields = st.parse_csv(line)
result = ""
if len(fields) >= 3:
    result = st.to_csv([fields[0].upper(), fields[2], "processed"])
else:
    skip()

result
' data.csv
```

#### JSON Processing
```bash
stelp --eval '
data = st.parse_json(line)
data["timestamp"] + " | " + data["event"]
' events.json
```

#### Log Processing with Termination
```bash
stelp --eval '
result = ""
if "FATAL" in line:
    emit(f"Fatal error found: {line}")
    terminate("Processing stopped due to fatal error")

if st.regex_match(r"\[ERROR\]", line):
    result = st.regex_replace(r"\[ERROR\]", "[🔴 ERROR]", line)
else:
    result = line

result
' server.log
```

## Built-in Functions

### String Operations
- Standard Starlark string methods: `upper()`, `lower()`, `strip()`, `split()`, `replace()`, etc.
- `st.regex_match(pattern, text)` - Check if text matches regex
- `st.regex_replace(pattern, replacement, text)` - Replace using regex
- `st.regex_find_all(pattern, text)` - Find all matches

### Data Processing
- `st.parse_json(text)` - Parse JSON string to dict/list
- `st.to_json(value)` - Convert value to JSON string
- `st.parse_csv(line, delimiter=",")` - Parse CSV line to list
- `st.to_csv(values, delimiter=",")` - Convert list to CSV line
- `st.parse_kv(line, sep="=", delim=" ")` - Parse key-value pairs

### Global Variables
- `st.get_global(name, default=None)` - Get global variable
- `st.set_global(name, value)` - Set global variable

### Context Information
- `st.line_number()` - Current line number
- `st.file_name()` - Current file name (if processing files)

### Output Control
- `emit(text)` - Output an additional line
- `skip()` - Skip outputting the current line
- `terminate()` - Stop processing entirely

## Variable Scopes

### Local Variables (Per-Line)
```python
# These reset for each line
parts = line.split(",")
name = parts[0].strip()
# Process and return result
```

### Global Variables (Pipeline-Wide)
```python
# These persist across all lines
total = st.get_global("total", 0) + 1
st.set_global("total", total)

if total > 1000:
    terminate("Processed enough lines")
```

## Command-Line Options

```
stelp [OPTIONS] [FILE]...

Arguments:
  [FILE]...                Input files to process (default: stdin if none provided)

Options:
  -e, --eval <EXPRESSION>  Pipeline evaluation expressions (executed in order)
  -f, --file <FILE>        Script file containing pipeline definition
  -o, --output <FILE>      Output file (default: stdout)
      --debug              Debug mode - show processing details
      --fail-fast          Fail on first error instead of skipping
      --progress <N>       Show progress every N lines
      --max-line-length <N> Maximum line length [default: 1048576]
      --buffer-size <N>    Buffer size for I/O [default: 65536]
  -h, --help               Print help
  -V, --version            Print version
```

## Script Files

Create reusable processing scripts:

```python
# process_logs.star

# Helper function
def format_timestamp(line):
    return st.regex_replace(r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})', r'\1T\2Z', line)

# Processing logic
line = line.strip()

if len(line) == 0:
    skip()

# Track lines
count = st.get_global("total", 0) + 1
st.set_global("total", count)

# Process errors specially  
result = ""
if "ERROR" in line:
    error_count = st.get_global("errors", 0) + 1
    st.set_global("errors", error_count)
    emit(f"[{count}] Error #{error_count}: {line}")
    skip()
else:
    # Format and output
    formatted = format_timestamp(line)
    result = f"[{count}] {formatted}"

result
```

Run with:
```bash
stelp -f process_logs.star logs.txt
```

## Performance

- **Simple transformations**: 10K-50K lines/second
- **Complex scripts with globals**: 1K-10K lines/second  
- **Memory usage**: Scales with pipeline complexity, not input size
- **Streaming**: Processes data without buffering entire input

## Error Handling

### Skip Strategy (Default)
```bash
stelp --eval 'st.parse_json(line)["field"]' data.json  # Skips invalid JSON lines
```

### Fail-Fast Strategy
```bash
stelp --fail-fast --eval 'st.parse_json(line)["field"]' data.json  # Stops on first error
```

## Best Practices

### Conditional Transformations
When using `if/else` statements for transformations, always use explicit result variables:

```python
# ✅ Good - Use explicit result variable
result = ""
if condition:
    result = some_transformation(line)
else:
    result = line

result
```

```python
# ❌ Avoid - Direct if/else as final expression
if condition:
    some_transformation(line)
else:
    line
```

This ensures your transformations are properly applied and returned.

## Examples Repository

See the `examples/` directory for more complex use cases:

- Log file processing and analysis
- CSV data transformation
- JSON event stream processing
- Text report generation
- Data validation pipelines

## Testing

Run the test suite:
```bash
cargo test
```

Run with sample data:
```bash
# Generate test data
seq 1 1000 | stelp --eval 'f"Item {line}: {st.line_number()}"'

# Process CSV
echo -e "name,age,city\nAlice,30,NYC\nBob,25,LA" | stelp --eval '
result = ""
if st.line_number() == 1:
    result = line  # Keep header
else:
    fields = st.parse_csv(line)
    if int(fields[1]) >= 30:
        result = st.to_csv([fields[0], fields[1], fields[2], "senior"])
    else:
        skip()

result
'

# Process multiple files
stelp --eval 'line.upper()' file1.txt file2.txt file3.txt

# Use with shell globbing
stelp --eval 'f"[{st.file_name()}] {line}"' *.log
```

## License

MIT