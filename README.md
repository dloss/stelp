# Starproc

A high-performance CLI tool that processes stdin line-by-line using Starlark (Python-like) scripts.

## Features

- **Line-by-line processing** with Starlark transformation scripts
- **Multi-step pipelines** with global state management
- **Rich built-in functions** for text processing, regex, JSON, CSV
- **Flexible output control** - transform, emit multiple lines, filter, or terminate
- **Error handling** with skip or fail-fast strategies
- **Performance focused** - 10K-50K lines/second for simple transformations

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
echo "hello world" | starproc 'line.upper()'
# Output: HELLO WORLD

# Multiple steps
echo "hello,world" | starproc 'line.split(",")[0]' 'line.upper()'
# Output: HELLO

# Using --step flag
echo "test" | starproc --step 'line.upper()' --step 'line + "!"'
# Output: TEST!
```

### Advanced Examples

#### Global Variables and Counting
```bash
cat data.txt | starproc '
count = get_global("total", 0) + 1
set_global("total", count)
f"Line {count}: {line}"
'
```

#### Filtering and Multi-line Output
```bash
cat logs.txt | starproc '
if "ERROR" in line:
    emit(f"🚨 {line}")
    emit("---")
    skip()
else:
    line.upper()
'
```

#### CSV Processing
```bash
cat data.csv | starproc '
fields = parse_csv(line)
if len(fields) >= 3:
    to_csv([fields[0].upper(), fields[2], "processed"])
else:
    skip()
'
```

#### JSON Processing
```bash
cat events.json | starproc '
try:
    data = parse_json(line)
    data["timestamp"] + " | " + data["event"]
except:
    skip()
'
```

#### Log Processing with Termination
```bash
cat server.log | starproc '
if "FATAL" in line:
    emit(f"Fatal error found: {line}")
    terminate("Processing stopped due to fatal error")

if regex_match(r"\[ERROR\]", line):
    regex_replace(r"\[ERROR\]", "[🔴 ERROR]", line)
else:
    line
'
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
starproc [OPTIONS] [EXPRESSION]...

Arguments:
  [EXPRESSION]...  Pipeline steps (executed in order)

Options:
  -s, --step <EXPRESSION>     Additional pipeline steps
  -f, --file <FILE>          Script file containing pipeline
  -i, --input <FILE>         Input file (default: stdin)
  -o, --output <FILE>        Output file (default: stdout)
      --debug                Debug mode - show processing details
      --fail-fast            Fail on first error instead of skipping
      --progress <N>         Show progress every N lines
      --max-line-length <N>  Maximum line length [default: 1048576]
      --buffer-size <N>      Buffer size for I/O [default: 65536]
  -h, --help                 Print help
  -V, --version              Print version
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
cat logs.txt | starproc --file process_logs.star
```

## Performance

- **Simple transformations**: 10K-50K lines/second
- **Complex scripts with globals**: 1K-10K lines/second  
- **Memory usage**: Scales with pipeline complexity, not input size
- **Streaming**: Processes data without buffering entire input

## Error Handling

### Skip Strategy (Default)
```bash
starproc 'parse_json(line)["field"]'  # Skips invalid JSON lines
```

### Fail-Fast Strategy
```bash
starproc --fail-fast 'parse_json(line)["field"]'  # Stops on first error
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
seq 1 1000 | starproc 'f"Item {line}: {line_number()}"'

# Process CSV
echo -e "name,age,city\nAlice,30,NYC\nBob,25,LA" | starproc '
if line_number() == 1:
    line  # Keep header
else:
    fields = parse_csv(line)
    if int(fields[1]) >= 30:
        to_csv([fields[0], fields[1], fields[2], "senior"])
    else:
        skip()
'
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run `cargo test` and `cargo clippy`
5. Submit a pull request

## License

MIT OR Apache-2.0