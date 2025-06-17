# Stelp

Stream processing tool using [Starlark](https://github.com/bazelbuild/starlark) (Python-like) scripts. Transform, filter, and analyze text/structured data with familiar syntax.

> [!WARNING]  
> Experimental tool. [Vibe-coded](https://en.wikipedia.org/wiki/Vibe_coding). APIs may change without notice.

## Installation

```bash
git clone <repository>
cd stelp  
cargo build --release
# Binary: target/release/stelp
```

## Quick Examples

```bash
# Text transformation
echo "hello world" | stelp -e 'line.upper()'                    # → HELLO WORLD
seq 1 10 | stelp --filter 'int(line) % 2 == 0' -e 'f"Even: {line}"'  # Filter + transform

# Pattern extraction (convert text to structured data)
echo "192.168.1.1 admin 200" | stelp --extract-vars '{ip} {user} {status:int}' -F jsonl
echo "CPU: 85.2%" | stelp --extract-vars 'CPU: {cpu:float}%' --filter 'data["cpu"] > 80'

# Structured data processing
stelp -e 'data["user"] = data["user"].upper()' -F jsonl users.jsonl   # Modify data variable
echo '{"user":"alice","age":25}' | stelp -f jsonl -F jsonl -e 'data["user"] = data["user"].upper()'

# Derive mode for ergonomic field access
echo "name,price,qty" | stelp -f csv --derive 'total = float(price) * float(qty)'
stelp -f csv --derive 'category = "expensive" if float(price) > 20 else "cheap"' data.csv

# Log analysis with counters
stelp -e 'count = inc("total"); f"[{count}] {line}"' server.log

# Multi-output with emit() (line mode)
echo "user1,user2" | stelp -e 'emit_all(line.split(",")); None'
```

## Usage

```bash
stelp [OPTIONS] [FILES...]

# Core options
    --extract-vars <PATTERN> Extract structured data using patterns like '{field}' or '{field:type}'
-e, --eval <EXPR>           Evaluation expressions (executed in pipeline order)
    --filter <EXPR>         Filter expressions (keep lines where true)
-d, --derive <EXPR>         Transform structured data with field variable injection
-s, --script <FILE>         Script file with processing logic
    --begin/--end <EXPR>    Run before/after input processing

# Data formats  
-f, --input-format <FMT>    Input: line, jsonl, csv, logfmt, syslog, combined (auto-detects by extension)
-F, --output-format <FMT>   Output: line, jsonl, csv, logfmt  
-k, --keys <KEYS>           Select/order output columns (comma-separated)
-K, --remove-keys <KEYS>    Remove keys from structured output (comma-separated)

# Control
-o, --output <FILE>         Output file (default: stdout)
    --debug                 Show processing details
    --fail-fast             Stop on first error (default: skip errors)
```

## Format Auto-Detection

Auto-detects format from file extensions: `*.jsonl`, `*.csv`, `*.logfmt` (otherwise treats as plain text). Use `-f` to override.

## Core Concepts

### Variables & Context
```python
line                     # Current line text (for text input)
data                     # Parsed data dict (for structured input: jsonl/csv/logfmt/syslog)
LINENUM, FILENAME, RECNUM # Meta variables (line number, filename, record number)
glob["key"]              # Global state across lines
inc("counter")           # Increment global counter, returns new value
```

### Derive Mode (--derive)
For structured data, `--derive` provides direct field access instead of `data["field"]`:
```python
# Standard mode: data["price"] * data["quantity"] 
# Derive mode:   price * quantity

total = float(price) * float(quantity)           # Direct field access
category = "expensive" if float(price) > 20 else "cheap"
temp_field = None                                # Delete field

# Stelp functions use stelp_ prefix to avoid conflicts  
count = stelp_inc("processed")
stelp_data["invalid-key"] = "value"              # For non-identifier keys
```

### Pattern Extraction (--extract-vars)
Extract structured data from unstructured text using template patterns:
```python
# Pattern syntax
{field}        # Extract as string (default)
{field:int}    # Extract and convert to integer  
{field:float}  # Extract and convert to float
{field:word}   # Extract word characters only (\w+)
```

```bash
# Apache log processing
echo '192.168.1.1 - admin [25/Dec/2021:10:24:56] "GET /api HTTP/1.1" 200 1234' | \
  stelp --extract-vars '{ip} - {user} [{timestamp}] "{method} {path}" {status:int} {size:int}' \
  --filter 'data["status"] >= 400' -F jsonl

# System monitoring
echo "CPU: 85.2% Memory: 76.1%" | \
  stelp --extract-vars 'CPU: {cpu:float}% Memory: {memory:float}%' \
  --filter 'data["cpu"] > 80.0' \
  --eval 'cpu = data["cpu"]; data = None; f"High CPU: {cpu}%"'
        
# No match handling (graceful passthrough)
echo "unmatched text" | stelp --extract-vars '{ip} {user}' --eval 'data or "no match"'
```

### Pipeline Processing
Commands execute in order: `--extract-vars` → `--filter` → `--derive` → `-e` → ... (first to last)
```bash
stelp --filter 'len(line) > 3' -e 'line.upper()' -e 'f"Result: {line}"'
# 1. Filter: keep long lines  2. Uppercase  3. Add prefix
```

### Control Flow & Data Modes

**Line Mode** (processing text): `data` is None, `line` contains text
**Data Mode** (processing structured data): `data` contains parsed data, `line` is None

Data mode is automatically enabled with `--input-format` options (jsonl, csv, etc.) or when you assign to `data` in your script.

```python
# Available in both modes
skip()                   # Skip current line (no output)
exit("reason")           # Stop processing with message

# Only available in line mode (when data is None)
emit("text")             # Output additional line + continue processing
emit_all(["a","b"])      # Output each item as separate line  

# Processing modes:
# • Line mode: return value becomes output (None = skip)
# • Data mode: `data` variable passes through (return value ignored)
```

### F-String Limitations ⚠️
F-strings only work with simple variables. Extract complex expressions first:
```python
# ❌ Wrong: f"User: {data['user']}, Count: {glob.get('total')}"
# ✅ Right (line mode):
user = data["user"] if data else "none"
count = glob.get("total", 0)
f"User: {user}, Count: {count}"
# ✅ Right (data mode):
data = {"formatted": f"User: {data['user']}, Count: {glob.get('total', 0)}"}
```

### Structured Data Formats

Input formats parse into `data` dictionary. Use `-k/--keys` to select/order output columns.

```bash
# JSON Lines (auto-detected from .jsonl extension)
stelp -e 'data["name"] = data["name"].upper()' -F jsonl users.jsonl

# Manual format specification
echo '{"name":"alice","age":25}' | stelp -f jsonl -F jsonl -e 'data["name"] = data["name"].upper()'

# CSV (auto-detects headers)
echo -e "name,age\nalice,25" | stelp -f csv -F jsonl -e 'data["name"] = data["name"].upper()'

# logfmt (key=value pairs) - show specific field
echo "user=alice level=info msg='login success'" | stelp -f logfmt -F jsonl -k user

# Syslog (RFC3164/5424) - show program name
echo 'Oct 11 22:14:15 srv sshd[1234]: Failed login' | stelp -f syslog -F jsonl -k prog

# Apache/Nginx logs (standard & extended combined format) - show IP
echo '192.168.1.1 - - [10/Oct/2023:13:55:36] "GET / HTTP/1.1" 200 1234' | stelp -f combined -F jsonl -k ip

# Column selection/ordering
echo '{"name":"alice","age":25,"city":"NYC"}' | stelp -f jsonl -F csv -k "name,city"
```

### CSV Output from Schema-less Data

When converting to CSV, use `--keys` to prevent data loss:
```bash
# Default: warns about missing fields
echo -e '{"a":1,"b":2}\n{"a":1,"c":3}' | stelp -f jsonl -F csv
# → Warning: keys 'c' found but not in schema

# Explicit keys: no warnings, no data loss  
echo -e '{"a":1,"b":2}\n{"a":1,"c":3}' | stelp -f jsonl -F csv --keys a,b,c
# → a,b,c\n1,2,\n1,,3
```

## Common Patterns

### Pattern Extraction & Log Processing
```bash
# Extract Apache/Nginx log fields
echo '192.168.1.1 - - [25/Dec/2021:10:24:56] "GET /api/status HTTP/1.1" 200 1234' | \
  stelp --extract-vars '{ip} - - [{timestamp}] "{method} {path} {protocol}" {status:int} {size:int}' \
  --filter 'data["status"] >= 400' \
  --eval 'ip = data["ip"]; status = data["status"]; data = None; f"Error from {ip}: {status}"'

# Parse custom log format
echo "2023-12-25 10:24:56 ERROR user:alice message:login failed" | \
  stelp --extract-vars '{date} {time} {level:word} user:{user} message:{message}' \
  --filter 'data["level"] == "ERROR"' -F jsonl

# System metrics monitoring  
printf "CPU: 85.2%% Memory: 76.1%%\nCPU: 45.0%% Memory: 62.3%%\nCPU: 92.1%% Memory: 88.9%%\n" | \
  stelp --extract-vars 'CPU: {cpu:float}% Memory: {memory:float}%' \
  --filter 'data and data["cpu"] > 80.0' \
  --eval 'cpu = data["cpu"]; data = None; f"High CPU: {cpu}%"'

# Mixed processing: extract → transform → convert back to text
echo "user=alice score=85 attempts=3" | \
  stelp --extract-vars 'user={user} score={score:int} attempts={attempts:int}' \
  --eval 'efficiency = data["score"] / data["attempts"]; data = None; f"{user}: {efficiency:.1f}"'
```

### Text Processing
```bash
# Transform + filter + count
stelp --filter 'len(line) > 50' -e 'count = inc("long"); f"[{count}] {line.upper()}"' input.txt

# Regex processing with line numbers
stelp -e 'regex_replace(r"ERROR", "🔴", line)' -e 'f"[{LINENUM}] {line}"' error.log

# Log timestamp normalization
cat access.log | stelp -e 'ts_str = regex_replace(r".*\[([^\]]+)\].*", r"\1", line); epoch = guess_ts(ts_str); f"{format_ts(epoch, \"%Y-%m-%d %H:%M:%S\")} {line}"'

# Global state tracking
stelp -e 'count = inc("total"); error_count = inc("errors") if "ERROR" in line else glob.get("errors", 0); f"Total: {count}, Errors: {error_count}"' server.log
```

### Fan-out Processing  
```bash
# Split lines into multiple outputs (line mode)
echo "user:alice,bob" | stelp -e 'emit_all(line.split(":")[1].split(",")); None'

# Conditional emit in line mode
stelp -e 'if "ERROR" in line: emit(f"🚨 {line}"); line' server.log
```

### BEGIN/END Processing
```bash
# Headers/footers (like AWK)
stelp --begin '"=== REPORT ==="' --end '"=== END ==="' -e 'line.upper()' input.txt

# Early termination
stelp --begin 'exit("No processing needed")' -e 'line.upper()' input.txt
```

### Structured Data Processing
```bash
# JSON processing with validation
echo '{"user":"alice","status":"active"}' | stelp -f jsonl -F jsonl --filter 'data["status"] == "active"' -k user

# CSV transformation  
stelp -f csv -F csv --filter 'int(data["age"]) >= 18' -k name users.csv

# Log format analysis - extract IPs from 4xx/5xx responses
cat /var/log/nginx/access.log | stelp -f combined -F jsonl --filter 'data["status"] >= 400' -k ip | sort | uniq -c

# Syslog filtering - show critical messages  
cat /var/log/syslog | stelp -f syslog -F jsonl --filter 'data.get("severity", 0) <= 3' -e 'data = {"alert": f"CRITICAL: {data["msg"]}"}'
```

### Multi-file Processing
```bash
# Cross-file state tracking
stelp -e 'file_lines = inc(f"lines_{FILENAME}"); total = inc("total"); f"[{FILENAME}:{LINENUM}] {total} {line}"' *.log
```

## Script Files

For complex logic, use script files with `-s`:

```python
# process_logs.star  
def categorize_level(line):
    return "error" if "ERROR" in line else "warning" if "WARN" in line else "info"

category = categorize_level(line)
count = inc(f"{category}_count")

if category == "error":
    emit(f"🔴 Error #{count}: {line}")
    
category_upper = category.upper()
f"[{category_upper}:{count}] {line}"
```

```bash
stelp -s process_logs.star server.log
```

## Built-in Functions

```python
# String/Regex
regex_match(pattern, text)         # Test if pattern matches
regex_replace(pattern, repl, text) # Replace pattern with replacement  
regex_find_all(pattern, text)      # Find all matches

# JSON/CSV  
parse_json(text)                   # Parse JSON string → dict
dump_json(obj)                     # Serialize dict → JSON string
parse_csv(text)                    # Parse CSV line → list
dump_csv(list)                     # Serialize list → CSV line

# Timestamps
parse_ts(text, format=None)        # Parse timestamp to Unix epoch
format_ts(timestamp, format=None)  # Format Unix timestamp to string
guess_ts(text)                     # Auto-detect timestamp format
now()                              # Current Unix timestamp
ts_diff(ts1, ts2)                  # Calculate time difference
ts_add(timestamp, seconds)         # Add/subtract time
```

## Variable Scopes & Exit Codes

**Scopes**: Local vars reset per line. Global state via `glob["key"]` or `inc("counter")`. Meta vars: `LINENUM`, `FILENAME`, `RECNUM`.

**Exit codes**: `0` = success, `1` = processing errors, `2` = no output produced.

## License

MIT

---

*Stelp: Starlark Event and Line Processor*