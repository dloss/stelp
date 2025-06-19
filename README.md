# Stelp

One-line log processing with Python-like syntax.

```bash
# Install
cargo install --git https://github.com/dloss/stelp
```

## Start Simple

```bash
# Turn unstructured logs into structured data
echo "2024-01-15 ERROR Connection timeout after 30 seconds" | \
  stelp --extract-vars '{date} {level} {message}' --derive 'severity = 1 if level == "ERROR" else 5' -F jsonl

# Count errors in real-time with state
tail -f app.log | stelp --filter '"ERROR" in line' -e 'count = inc("errors"); f"Error #{count}: {line}"'

# Process JSON logs instantly
echo '{"level":"error","msg":"failed"}' | stelp -f jsonl --filter 'data["level"] == "error"' -k msg

# Extract emails from any text
echo "Contact support@example.com for help" | stelp -e 'extract_pattern("email", line)'
```

## Common Tasks

### Log Processing That Actually Helps
```bash
# Extract IPs and status codes from Apache logs
stelp -f combined -k ip,status --filter 'int(data["status"]) >= 400' access.log

# Monitor failed SSH attempts with counting
tail -f /var/log/auth.log | stelp --filter '"Failed password" in line' \
  -e 'count = inc("failed_ssh"); f"SSH failure #{count}: {line}"'

# Parse custom log formats
echo "2024-01-15 ERROR user:alice msg:login_failed" | \
  stelp --extract-vars '{date} {level:word} user:{user} msg:{message}' -F jsonl

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
# Commands run in order: extract → filter → transform
stelp --extract-vars '{ip} {user}' --filter 'data["ip"].startswith("192")' -e 'data["user"].upper()'
```

### Data vs Line Mode
- **Default**: Text processing with `line` variable
- **With `-f csv/jsonl/etc`**: Structured data processing with `data` variable
- **Auto-switching**: Stelp picks the right mode based on your input format

## Advanced Features (when you need them)

### Pattern Extraction
```bash
# Extract structured data from text
echo "user=alice score=85 attempts=3" | \
  stelp --extract-vars 'user={user} score={score:int} attempts={attempts:int}' -F jsonl

# Built-in patterns
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
    --extract-vars <PATTERN> Extract data using {field} or {field:type} patterns
-k, --keys <KEYS>           Select/order output columns
    --levels <LEVELS>       Show only these log levels
    --window <N>            Keep last N records for analysis
    --plain                 Output values only, not key=value pairs
```

## Built-in Functions

```python
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