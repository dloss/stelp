# Stelp

A high-performance CLI tool that processes stdin line-by-line using Starlark (Python-like) scripts.

## Features

- **Line-by-line processing** with Starlark transformation scripts
- **Code reuse with --include** - share functions and constants across invocations
- **Multi-step pipelines** with global state management
- **Rich built-in functions** for text processing, regex, JSON, CSV
- **Flexible output control** - transform, emit multiple lines, filter, or exit
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

### Code Reuse with --include

Share functions and constants across invocations:

```bash
# Create reusable helpers
cat > helpers.star << 'EOF'
def clean_line(text):
    return text.strip().replace('\t', ' ')

def is_error_line(text):
    return "ERROR" in text.upper()

MAX_LINE_LENGTH = 1000
EOF

# Use shared functions
stelp --include helpers.star --eval 'clean_line(line)' messy.txt

# Multiple includes (processed in order)
stelp --include constants.star --include utils.star --eval 'process(line)' data.txt

# Works with filters and script files too
stelp --include validators.star --filter 'is_valid(line)' --eval 'transform(line)' input.txt
stelp --include shared.star -f main_script.star input.txt
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

#### Shared Log Processing
```bash
# Create log_utils.star
cat > log_utils.star << 'EOF'
def parse_log_level(line):
    if "[ERROR]" in line:
        return "ERROR"
    elif "[WARN]" in line:
        return "WARN"
    elif "[INFO]" in line:
        return "INFO"
    return "DEBUG"

def colorize_level(level):
    colors = {"ERROR": "\033[31m", "WARN": "\033[33m", "INFO": "\033[32m"}
    reset = "\033[0m"
    color = colors.get(level, "")
    return f"{color}{level}{reset}"
EOF

# Use shared functions
stelp --include log_utils.star --eval '
level = parse_log_level(line)
colored_level = colorize_level(level)
line.replace(f"[{level}]", f"[{colored_level}]")
' server.log
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
    exit("Processing stopped due to fatal error")

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
- `exit()` - Stop processing entirely

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
    exit("Processed enough lines")
```

### Shared Functions (Via --include)
```python
# In helpers.star
def validate_email(email):
    return st.regex_match(r"[^@]+@[^@]+", email)

# In your script
stelp --include helpers.star --eval 'validate_email(line)'
```

## Command-Line Options

```
stelp [OPTIONS] [FILE]...

Arguments:
  [FILE]...                Input files to process (default: stdin if none provided)

Options:
      --include <FILE>     Include Starlark files (processed in order)
  -e, --eval <EXPRESSION>  Pipeline evaluation expressions (executed in order)
  -f, --file <FILE>        Script file containing pipeline definition
      --filter <EXPR>      Filter expressions - remove lines where expression is true
  -o, --output <FILE>      Output file (default: stdout)
      --debug              Debug mode - show processing details
      --fail-fast          Fail on first error instead of skipping
  -h, --help               Print help
  -V, --version            Print version
```

## Include Files

Create reusable libraries for common tasks:

### Constants and Configuration
```python
# config.star
MAX_RETRIES = 3
API_BASE_URL = "https://api.example.com"
VALID_STATUSES = ["active", "pending", "disabled"]
```

### Utility Functions
```python
# text_utils.star
def normalize_whitespace(text):
    return st.regex_replace(r'\s+', ' ', text.strip())

def extract_email(text):
    matches = st.regex_find_all(r'[^@\s]+@[^@\s]+\.[^@\s]+', text)
    return matches[0] if matches else None

def is_valid_json(text):
    try:
        st.parse_json(text)
        return True
    except:
        return False
```

### Domain-Specific Processing
```python
# log_processor.star
def parse_apache_log(line):
    # Custom Apache log parser
    return st.regex_replace(
        r'(\S+) \S+ \S+ \[(.*?)\] "(.*?)" (\d+) (\d+|-)', 
        r'{"ip":"\1","time":"\2","request":"\3","status":\4,"size":\5}', 
        line
    )

def categorize_http_status(status):
    status_int = int(status)
    if status_int < 300:
        return "success"
    elif status_int < 400:
        return "redirect"
    elif status_int < 500:
        return "client_error"
    else:
        return "server_error"
```

Usage:
```bash
stelp --include config.star --include text_utils.star --include log_processor.star \
      --eval 'parse_apache_log(normalize_whitespace(line))' access.log
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
stelp --include helpers.star -f process_logs.star logs.txt  # With includes
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

### Organize Include Files
```
includes/
├── constants.star      # Shared configuration
├── validators.star     # Data validation functions  
├── formatters.star     # Output formatting helpers
├── parsers.star        # Input parsing utilities
└── domain/
    ├── logs.star       # Log-specific functions
    └── api.star        # API-specific functions
```

### Use Include Order for Overrides
```bash
# Base functionality first, then specializations
stelp --include base.star --include company_overrides.star --eval 'process(line)'
```

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

# Test include functionality
echo "def greet(name): return 'Hello, ' + name" > greet.star
echo "World" | stelp --include greet.star --eval 'greet(line)'
```

## License

MIT