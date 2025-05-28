# Starproc

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
cd starproc
cargo build --release
# Binary will be available at target/release/starproc
```

## Quick Start

### Basic Usage

```bash
# Simple transformation
echo "hello world" | starproc --step 'line.upper()'
# Output: HELLO WORLD

# Process files directly
starproc --step 'line.upper()' input.txt

# Multiple files
starproc --step 'line.upper()' file1.txt file2.txt file3.txt

# Multiple steps
starproc --step 'line.split(",")[0]' --step 'line.upper()' data.csv

# Using script files
starproc -f script.star input1.txt input2.txt
```

### Advanced Examples

#### Global Variables and Counting
```bash
starproc --step '
count = get_global("total", 0) + 1
set_global("total", count)
f"Line {count}: {line}"
' data.txt
```

#### Filtering and Multi-line Output
```bash
starproc --step '
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
starproc --step '
fields = parse_csv(line)
if len(fields) >= 3:
    to_csv([fields[0].upper(), fields[2], "processed"])
else:
    skip()
' data.csv
```

#### JSON Processing
```bash
starproc --step '
try:
    data = parse_json(line)
    data["timestamp"] + " | " + data["event"]
except:
    skip()
' events.json
```

#### Log Processing with Termination
```bash
starproc --step '
if "FATAL" in line:
    emit(f"Fatal error found: {line}")
    terminate("Processing stopped due to fatal error")

if regex_match(r"\[ERROR\]", line):
    regex_replace(r"\[ERROR\]", "[🔴 ERROR]", line)
else:
    line
' server.log
```

## Built-in Functions

### String Operations
- Standard Starlark string methods: `upper()`, `lower()`, `strip()`, `split()`, `replace()`, etc.
- `regex_match(pattern, text)` - Check if text matches regex
- `regex_replace(pattern, replacement, text)` - Replace using regex
- `regex_find_all(pattern, text)` - Find all matches

### Data Processing
- `parse_json(text)` - Parse JSON string to dict/list
- `to_json(value)` - Convert value to JSON string
- `parse_csv(line, delimiter=",")` - Parse CSV line to list
- `to_csv(values, delimiter=",")` - Convert list to CSV line
- `parse_kv(line, sep="=", delim=" ")` - Parse key-value pairs

### Global Variables
- `get_global(name, default=None)` - Get global variable
- `set_global(name, value)` - Set global variable

### Context Information
- `line_number()` - Current line number
- `file_name()` - Current file name (if processing files)

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
total = get_global("total", 0) + 1
set_global("total", total)

if total > 1000:
    terminate("Processed enough lines")
```

## Command-Line Options

```
starproc [OPTIONS] [FILE]...

Arguments:
  [FILE]...                Input files to process (default: stdin if none provided)

Options:
  -s, --step <EXPRESSION>  Pipeline steps (executed in order)
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
    return regex_replace(r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})', r'\1T\2Z', line)

# Processing logic
line = line.strip()

if len(line) == 0:
    skip()

# Track lines
count = get_global("total", 0) + 1
set_global("total", count)

# Process errors specially  
if "ERROR" in line:
    error_count = get_global("errors", 0) + 1
    set_global("errors", error_count)
    emit(f"[{count}] Error #{error_count}: {line}")
    skip()

# Format and output
formatted = format_timestamp(line)
f"[{count}] {formatted}"
```

Run with:
```bash
starproc -f process_logs.star logs.txt
```

## Performance

- **Simple transformations**: 10K-50K lines/second
- **Complex scripts with globals**: 1K-10K lines/second  
- **Memory usage**: Scales with pipeline complexity, not input size
- **Streaming**: Processes data without buffering entire input

## Error Handling

### Skip Strategy (Default)
```bash
starproc --step 'parse_json(line)["field"]' data.json  # Skips invalid JSON lines
```

### Fail-Fast Strategy
```bash
starproc --fail-fast --step 'parse_json(line)["field"]' data.json  # Stops on first error
```

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
seq 1 1000 | starproc --step 'f"Item {line}: {line_number()}"'

# Process CSV
echo -e "name,age,city\nAlice,30,NYC\nBob,25,LA" | starproc --step '
if line_number() == 1:
    line  # Keep header
else:
    fields = parse_csv(line)
    if int(fields[1]) >= 30:
        to_csv([fields[0], fields[1], fields[2], "senior"])
    else:
        skip()
'

# Process multiple files
starproc --step 'line.upper()' file1.txt file2.txt file3.txt

# Use with shell globbing
starproc --step 'f"[{file_name()}] {line}"' *.log
```

## License

MIT