# Stelp

One-line log processing with Python-like syntax.

```bash
# Install
cargo install --git https://github.com/dloss/stelp
```

## Start Simple

```bash
# Extract structured data from logs using column patterns
echo "2024-01-15 ERROR Connection timeout after 30 seconds" | \
  stelp -e 'date, level, message = cols(line, 0, 1, "2:"); f"[{level}] {date}: {message}"'

# Parse HTTP requests with column extraction
echo "GET /api/users HTTP/1.1" | stelp -e 'method, path, protocol = cols(line, 0, 1, 2); f"{method} -> {path}"'

# Count errors in real-time with state
tail -f app.log | stelp --filter '"ERROR" in line' -e 'count = inc("errors"); f"Error #{count}: {line}"'

# Extract specific columns from CSV-like data
echo "alice,25,engineer,remote" | stelp -e 'name, age, role = cols(line, 0, 1, 2, sep=","); f"{name} is a {age}yo {role}"'
```

## Common Tasks

### Log Processing That Actually Helps
```bash
# Extract IPs and status codes from Apache logs
stelp -f combined -k ip,status --filter 'int(data["status"]) >= 400' access.log

# Monitor failed SSH attempts with counting
tail -f /var/log/auth.log | stelp --filter '"Failed password" in line' \
  -e 'count = inc("failed_ssh"); f"SSH failure #{count}: {line}"'

# Parse structured log components
echo "2024-01-15 ERROR user:alice msg:login_failed" | \
  stelp -e 'date, level, userpart, msgpart = cols(line, 0, 1, 2, 3); user = userpart.split(":")[1]; msg = msgpart.split(":")[1]; f"[{level}] {user}: {msg}"'

# Find slow database queries in JSON logs
stelp -f jsonl --filter 'data["query_time"] > 1.0' -k timestamp,query,query_time slow.jsonl
```

### Basic Text Operations (When You Need Them)
```bash
# Show only long lines (over 50 characters)
stelp --filter 'len(line) > 50' /var/log/app.log

# Add line numbers
stelp -e 'f"{LINENUM}: {line}"' /var/log/app.log

# Replace text
stelp -e 'line.replace("ERROR", "PROBLEM")' /var/log/app.log

# Convert CSV to JSON
stelp -f csv -F jsonl data.csv
```

## Input Formats (Auto-detected)

| Format | Example | Auto-detect |
|--------|---------|-------------|
| **Text** | `ERROR: Connection failed` | Default |
| **JSON Lines** | `{"level":"error","msg":"failed"}` | `.jsonl` files |
| **CSV** | `name,email,status` | `.csv` files |
| **Logfmt** | `level=error msg="failed"` | `.logfmt` files |
| **Syslog** | `Jan 1 10:00:00 host app: message` | Common log format |
| **Apache/Nginx** | `192.168.1.1 - - [timestamp] "GET /"` | Access logs |

Force format with `-f`: `stelp -f jsonl data.txt`

## Core Concepts (5 minutes to learn)

### Text Processing (Default Mode)
```python
line                    # Current line text
cols(line, 0, 1, 2)     # Extract columns (whitespace split)
cols(line, "1:3")       # Extract column ranges (slice notation)
cols(line, "0,2", sep=",") # Extract columns with custom separator
LINENUM, FILENAME, RECNUM # Line number, filename, record number
glob["key"]             # Global counters/state
inc("counter")          # Increment counter, returns new value
```

### Structured Data Processing
When using `-f csv`, `-f jsonl`, etc., you get:
```python
data                    # Parsed data (dict for JSON, CSV rows)
data["field"]           # Access specific fields
```

### ⚠️ F-String Limitation
F-strings only work with simple variables. Extract complex expressions first:
```python
# ❌ Wrong: f"User: {data['user']}"
# ✅ Right: 
user = data["user"]
f"User: {user}"

# ❌ Wrong: f"Count: {glob.get('total', 0)}"
# ✅ Right:
count = glob.get("total", 0)  
f"Count: {count}"
```

### Processing Pipeline
```bash
# Commands run in order: filter → transform
stelp --filter '"192" in line' -e 'ip, user = cols(line, 0, 1); f"User {user.upper()} from {ip}"'
```

### Data vs Line Mode
- **Default**: Text processing with `line` variable
- **With `-f csv/jsonl/etc`**: Structured data processing with `data` variable
- **Auto-switching**: Stelp picks the right mode based on your input format

## Advanced Features (when you need them)

### Column Extraction (Primary Method)
```bash
# Extract columns by index (0-based, negative indexing supported)
echo "alpha beta gamma delta" | stelp -e 'cols(line, 0)'        # "alpha" (first)
echo "alpha beta gamma delta" | stelp -e 'cols(line, -1)'       # "delta" (last)

# Extract multiple columns (returns list)
echo "GET /api/users HTTP/1.1" | stelp -e 'method, path, version = cols(line, 0, 1, 2)'

# Slice notation for ranges
echo "a b c d e f g" | stelp -e 'cols(line, "1:3")'    # "b c" (columns 1-2)
echo "a b c d e f g" | stelp -e 'cols(line, "2:")'     # "c d e f g" (from 2 to end)
echo "a b c d e f g" | stelp -e 'cols(line, ":3")'     # "a b c" (start to 2)

# Multiple indices with custom separators
echo "alice,25,engineer" | stelp -e 'cols(line, "0,2", sep=",")'        # "alice engineer"
echo "a b c d" | stelp -e 'cols(line, "0,2", outsep=":")'               # "a:c"

# Mix different selector types in one call (klp-compatible)
echo "a b c d e f g h i j" | stelp -e 'first, middle, range, last = cols(line, 0, "1,3", "5:8", -1)'
# Returns: ["a", "b d", "f g h", "j"]
# - Integer args (0, -1) return individual columns
# - String args ("1,3", "5:8") combine/slice columns with outsep
```

### Built-in Pattern Extraction
```bash
# Extract emails, IPs, URLs, etc. from text
echo "Contact support@example.com for help" | stelp -e 'extract_pattern("email", line)'
stelp --list-patterns    # Show all available patterns
```

### Window Functions
```bash
# Show current and previous values
seq 1 10 | stelp --window 2 -e 'curr = int(line); prev = int(window[-2]["line"]) if window_size() >= 2 else 0; f"Current: {curr}, Previous: {prev}"'

# Change detection
printf "10\n15\n12\n18\n" | stelp --window 2 -e 'curr = int(line); prev = int(window[-2]["line"]) if window_size() >= 2 else curr; change = curr - prev; f"Value: {curr}, Change: {change}"'
```

### Multi-file Processing
```bash
# Process multiple logs with context
stelp -e 'f"[{FILENAME}:{LINENUM}] {line}"' /var/log/*.log

# Cross-file counters
stelp -e 'count = inc(f"errors_{FILENAME}"); f"File {FILENAME}: {count} errors"' *.log
```

## CLI Reference

```bash
stelp [OPTIONS] [FILES...]

# Essential options
-f, --input-format <FMT>    Input: line, jsonl, csv, logfmt, syslog, combined
-F, --output-format <FMT>   Output: line, jsonl, csv, logfmt  
-e, --eval <EXPR>           Transform expression  
    --filter <EXPR>         Keep lines where expression is true
-k, --keys <KEYS>           Select/order output columns
    --levels <LEVELS>       Show only these log levels
    --window <N>            Keep last N records for analysis
    --plain                 Output values only, not key=value pairs
```

## Built-in Functions

```python
# Column Extraction (Primary)
cols(text, 0, 1, 2)            # Extract multiple columns 
cols(text, "1:3")              # Extract column ranges (slice)
cols(text, "0,2", sep=",")     # Custom input/output separators

# Text/Regex
regex_match(pattern, text)      # Test if pattern matches
regex_replace(pattern, repl, text)  # Replace matches
extract_pattern("email", text)  # Extract emails, IPs, URLs, etc.

# JSON/Data
parse_json(text)               # Parse JSON string
dump_json(obj)                 # Convert to JSON string

# Timestamps  
parse_ts(text)                 # Parse timestamp to Unix epoch
format_ts(timestamp, "%Y-%m-%d")  # Format timestamp
guess_ts(text)                 # Auto-detect timestamp format
now()                          # Current Unix timestamp

# Control Flow
skip()                         # Skip current line
exit("message")                # Stop processing
emit("text")                   # Output additional line (line mode only)
```

## Performance Tips

- **Use `--filter` early**: Filter before expensive operations like regex or parsing
- **Minimize window size**: Large `--window` values use more memory
- **Avoid complex regex**: Simple string operations are faster than regex when possible
- **Use `skip()` for early exit**: Don't process lines you don't need

## Exit Codes

- `0` - Success
- `1` - Processing errors (with --fail-fast)
- `2` - No output produced

---

**Quick help**: `stelp --help` | **List patterns**: `stelp --list-patterns` | **Version**: `stelp --version`

*Stelp: Python-like syntax, Unix philosophy.*