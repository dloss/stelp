# Multiline Processing Examples

Stelp now supports multiline text processing through several chunking strategies. This allows you to process log files with stack traces, configuration blocks, and other multiline records as single units.

## Chunking Strategies

### 1. Fixed Line Count

Process text in chunks of a fixed number of lines:

```bash
# Process every 3 lines as a chunk
seq 1 10 | stelp --chunk-lines 3 -e 'replaced = line.replace(chr(10), " | "); f"Chunk: {replaced}"'
```

### 2. Start Pattern (Timestamp-based)

Start a new chunk whenever a line matches a regex pattern:

```bash
# Generate sample logs and chunk by timestamp
python3 examples/multiline_demo.py | stelp --chunk-start-pattern '^[0-9]{4}-[0-9]{2}-[0-9]{2}' -e 'first_line = line.split(chr(10))[0]; f"Log Entry: {first_line}"'
```

### 3. Delimiter-based

Split chunks on delimiter lines (delimiters are not included in output):

```bash
# Process configuration sections
python3 examples/multiline_demo.py | tail -n +15 | stelp --chunk-delimiter='---' -e 'lines = line.split(chr(10)); section = lines[0]; f"Config Section: {section}"'
```

## Real-world Examples

### Processing Java Stack Traces

Extract error information from Java application logs:

```bash
# Count lines in each log entry
python3 examples/multiline_demo.py | head -n 13 | stelp --chunk-start-pattern '^[0-9]{4}-[0-9]{2}-[0-9]{2}' -e '
lines = line.split(chr(10))
first_line = lines[0]
line_count = len(lines)
if line_count > 1:
    f"MULTILINE LOG ({line_count} lines): {first_line}"
else:
    f"SINGLE LOG: {first_line}"
'
```

### Processing Python Tracebacks

Extract traceback information:

```bash
# Extract just the error type and file from Python logs
python3 examples/multiline_demo.py | sed -n '15,25p' | stelp --chunk-start-pattern '^\\[' -e '
lines = line.split(chr(10))
if len(lines) > 1:
    # Find the actual error line
    error_line = ""
    for l in lines:
        if "Error:" in l or "Exception:" in l:
            error_line = l.strip()
            break
    if error_line:
        f"ERROR: {error_line}"
    else:
        first_line = lines[0]
        f"TRACEBACK: {first_line}"
else:
    lines[0]
'
```

### Configuration Processing

Process configuration blocks:

```bash
# Extract configuration values
echo "host=localhost
port=5432
database=myapp
---
host=redis.internal  
port=6379
timeout=30" | stelp --chunk-delimiter='---' -e '
lines = line.split(chr(10))
config_items = []
for l in lines:
    if "=" in l:
        key_val = l.split("=")
        if len(key_val) == 2:
            config_items.append(key_val[0].strip())
items_list = ", ".join(config_items)
f"Config block with: {items_list}"
'
```

## Safety Limits

All chunking strategies include safety limits to prevent memory issues:

```bash
# Chunks are limited to 1000 lines and 1MB by default
# You can override these limits:
cat large_file.log | stelp --chunk-start-pattern '^ERROR' --chunk-max-lines 500 --chunk-max-size 524288 -e 'line_count = len(line.split(chr(10))); f"Error chunk with {line_count} lines"'
```

## Combining with Other Features

Chunking works with all other Stelp features:

```bash
# Use chunking with filters
python3 examples/multiline_demo.py | stelp --chunk-start-pattern '^[0-9]{4}' --filter 'len(line.split(chr(10))) > 2' -e 'first_line = line.split(chr(10))[0]; f"Multi-line entry: {first_line}"'

# Use chunking with structured output
python3 examples/multiline_demo.py | stelp --chunk-start-pattern '^[0-9]{4}' -F jsonl -e '
lines = line.split(chr(10))
first_line_parts = lines[0].split(" ")
timestamp = first_line_parts[0]
level = first_line_parts[2] if len(first_line_parts) > 2 else "INFO"
line_count = len(lines)
is_multiline = len(lines) > 1
{
    "timestamp": timestamp,
    "level": level,
    "line_count": line_count,
    "is_multiline": is_multiline
}
'
```

## Performance Notes

- Chunking processes the entire input before pipeline execution, so it uses more memory than line-by-line processing
- For very large files, consider using external tools like `split` to break them into smaller pieces first
- The safety limits help prevent out-of-memory conditions with malformed input