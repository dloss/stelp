# Stelp

One-line log processing with Python-like syntax.

> [!WARNING]  
> Experimental tool. [Vibe-coded](https://en.wikipedia.org/wiki/Vibe_coding). APIs may change without notice.

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

# Process task durations from CSV data
echo -e "task,duration\nbackup,5h30m\ncleanup,2h 30m\narchive,1 day" | \
  stelp -f csv --derive 'hours = parse_duration(duration) / 3600; category = "long" if hours > 8 else "short"'

# Visual log level overview (big picture view)
echo -e '{"timestamp":"2024-01-01T10:00:00Z","level":"error","msg":"DB error"}\n{"timestamp":"2024-01-01T10:00:01Z","level":"warn","msg":"Memory high"}\n{"timestamp":"2024-01-01T10:00:02Z","level":"info","msg":"Started"}' | stelp -f jsonl --levelmap
# Output: 2024-01-01T10:00:00Z ewi

# Show only essential log fields (timestamp, level, message)
echo '{"timestamp":"2024-01-01T10:00:00Z","level":"INFO","message":"User login","user":"alice","ip":"1.2.3.4"}' | stelp -f jsonl --common
# Output: timestamp=2024-01-01T10:00:00Z level=INFO message="User login"

# Include additional fields with --common
echo '{"ts":"2024-01-01T10:00:00Z","lvl":"WARN","msg":"High CPU","service":"web","cpu":95}' | stelp -f jsonl --common --keys service,cpu
# Output: ts=2024-01-01T10:00:00Z lvl=WARN msg="High CPU" service=web cpu=95
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

> **Note**: Logfmt output automatically orders timestamp keys (`timestamp`, `ts`, `time`, `t`, `at`, `_ts`, `@t`) and level keys (`level`, `loglevel`, `log_level`, `lvl`, `severity`, `levelname`, `@l`) first for optimal readability.

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

### Column Extraction
```bash
# Basic column extraction from log entries
echo "GET /api/users HTTP/1.1 200" | stelp -e 'cols(line, 0)'        # "GET" (method)
echo "alice 127.0.0.1 admin login" | stelp -e 'cols(line, -1)'       # "login" (action)

# Parse HTTP access logs 
echo "GET /api/users HTTP/1.1 200" | stelp -e 'method, path, status = cols(line, 0, 1, -1); f"{method} {path} -> {status}"'
# Returns: "GET /api/users -> 200"

# Extract ranges for log analysis
echo "2024-01-15 10:30:45 INFO user.login alice success" | stelp -e 'timestamp = cols(line, "0:2"); level = cols(line, 2); f"{level}: {timestamp}"'
# Returns: "INFO: 2024-01-15 10:30:45"

# CSV-like data with custom separators  
echo "alice,25,engineer,remote" | stelp -e 'cols(line, "0,2", sep=",")'  # "alice engineer"

# Create structured data from text using --derive
echo "alice 127.0.0.1 login success" | stelp --derive 'user, ip, action, result = cols(line, 0, 1, 2, 3)' -F jsonl
# Returns: {"action":"login","ip":"127.0.0.1","result":"success","user":"alice"}

# Process server logs into structured format
echo "2024-01-15 ERROR database connection timeout" | stelp --derive 'date, level, service, error = cols(line, 0, 1, 2, "3:")' -F logfmt
# Returns: date=2024-01-15 level=ERROR service=database error="connection timeout"
```

### Derive Mode (Structured Data Transformation)
```bash
# Transform CSV data with direct field access
echo -e "name,price,quantity\nAlice,10.50,3" | stelp -f csv --derive 'total = float(price) * float(quantity)'

# Working variables starting with underscore (not included in output)
echo -e "name,score\nAlice,85" | stelp -f csv --derive '_ = float(score) / 100; _temp = "test"; temp_var = "normal"; grade = "A" if _ > 0.9 else "B"'
# Output: name=Alice score=85 grade=B temp_var=normal (underscore-prefixed variables excluded)

# Create/modify/delete fields
echo '{"user":"alice","temp":123}' | stelp -f jsonl --derive 'active = True; temp = None' -F jsonl
# Output: {"user":"alice","active":true} (temp field deleted)
```

### Built-in Pattern Extraction
```bash
# Extract emails, IPs, URLs, etc. from text
echo "Contact support@example.com for help" | stelp -e 'extract_pattern("email", line)'
stelp --list-patterns    # Show all available patterns
```

### Duration Processing
```bash
# Parse various duration formats
echo -e "5d\n3h30m\n2.5s\n2h 30m\n1 hour 30 minutes\n1y" | \
  stelp -e 'seconds = parse_duration(line); hours = seconds / 3600; f"Duration: {line} = {hours} hours"'

# Analyze log processing times
echo "Processing completed in 2h45m" | stelp -e 'duration_str = line.split()[-1]; minutes = parse_duration(duration_str) / 60; f"Completed in {minutes} minutes"'

# Filter tasks by duration
echo -e "task,time\nbackup,12h\ncleanup,30m\nrestore,3d" | \
  stelp -f csv --filter 'parse_duration(data["time"]) > 3600' -k task,time  # Over 1 hour
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
    --derive <EXPR>         Transform structured data with direct field access
-k, --keys <KEYS>           Select/order output columns
-c, --common               Show only timestamp, level, message fields (plus any --keys)
    --levels <LEVELS>       Show only these log levels
-M, --levelmap             Visual log level overview (requires -f format)
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

# Timestamps & Duration
parse_ts(text)                 # Parse timestamp to Unix epoch
format_ts(timestamp, "%Y-%m-%d")  # Format timestamp
guess_ts(text)                 # Auto-detect timestamp format
now()                          # Current Unix timestamp
parse_duration(text)           # Parse duration to seconds (e.g., "5d", "2h30m", "1.5s")

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