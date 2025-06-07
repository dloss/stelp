# Stelp Structured Data Processing Design

## Overview

This document defines Stelp's structured data processing capabilities, enabling seamless transitions between text-based and structured data processing within pipelines.

## Core Concept

Each pipeline stage receives a record that is **either** text-based **or** structured data:
- Text records have `line` (string) and `data = None`
- Structured records have `data` (dict/list) and `line = None`
- `glob` (global state)

## Automatic Input Parsing

### Input Format Options
```bash
stelp --input-format csv file.csv           # Auto-parse as CSV → data = ["col1", "col2", ...]
stelp --input-format json file.json         # Auto-parse as JSON → data = {...}
stelp --input-format fields file.txt        # Auto-split on whitespace → data = ["field1", "field2", ...]
stelp --input-format kv file.conf           # Auto-parse key=value → data = {"key": "value", ...}
stelp file.txt                              # Default: line-based → line = "text content"

# Short form
stelp -f csv file.csv
stelp -f json file.json
stelp -f fields file.txt
stelp -f kv file.txt
```

### Default Parsing Behavior
- **csv**: Standard CSV parsing with comma delimiter, no headers → `["col1", "col2", ...]`
- **json**: JSON object/array parsing → `{...}` or `[...]`
- **fields**: Whitespace-separated fields (like awk) → `["field1", "field2", ...]`
- **kv**: Key=value pairs separated by whitespace → `{"key": "value", ...}`

## API Design

### Variables Available in Each Stage

```python
# Record content (mutually exclusive)
line     # string or None - text content
data     # dict/list or None - structured data

# Context and state (always available)
glob     # dict - global state across all records/files (writable)
```

### Built-in Meta Properties

```python
FILENAME     # current filename being processed
LINENUM      # 1-based line number in current file
RECNUM       # records processed so far in this file
```

### Core Functions

```python
inc("counter_name", delta=1)              # increment global counter
split(line, delimiter=None)               # → list (awk-style field splitting)
parse_csv(line, headers=None, delimiter=",")  # → list or dict
parse_json(line)                          # → dict/list
parse_kv(line, sep="=", delim=" ")       # → dict
to_csv(data, delimiter=",")              # dict/list → CSV string
to_json(data)                            # → JSON string
```

## Return Value Processing

| Return Type | Next Stage Gets | Behavior |
|-------------|----------------|----------|
| `"string"` | `line="string", data=None` | Text processing |
| `{"key": "val"}` | `line=None, data={"key": "val"}` | Structured processing |
| `[item1, item2]` | Multiple records (fan-out) | Each item becomes separate record |
| Variable assignment | Modified record passed through | In-place modification |
| `None`/`skip()` | Record dropped | Filtering |
| `emit("text")` | Additional output | Extra output line |

## Usage Patterns

### Automatic CSV Processing
```bash
# Process CSV with positional access
stelp -f csv --eval 'f"{data[0]} is {data[1]} years old"' users.csv

# Need headers? Parse manually in pipeline
stelp -f csv --eval 'parse_csv_with_headers(data, ["name", "age", "city"])' \
      --eval 'f"{data[\"name\"]} lives in {data[\"city\"]}"' users.csv
```

### Automatic JSON Processing
```bash
# Process JSON objects
stelp -f json --eval 'f"User {data[\"id\"]}: {data[\"status\"]}"' events.json

# Extract specific fields
stelp -f json --eval 'data["user"]["email"]' nested.json
```

### Automatic Field Splitting
```bash
# Process whitespace-separated fields (like awk)
stelp -f fields --eval 'f"{data[0]} -> {data[2]}"' logfile.txt

# Need custom delimiter? Parse manually
stelp --eval 'split(line, ":")' --eval 'data[0]' /etc/passwd
```

### Automatic Key-Value Processing
```bash
# Process simple config files (key=value format)
stelp -f kv --eval 'f"Setting: {data}"' config.conf

# Extract specific settings
stelp -f kv --filter 'data.get("enabled") == "true"' --eval 'data["name"]' settings.conf
```

### Mixed Processing Pipeline
```bash
# Parse JSON, process, output as CSV
stelp -f json --eval 'to_csv([data["name"], data["age"], data["city"]])' users.json
```

### Fan-out Processing
```bash
# JSON array auto-parsing with fan-out
stelp -f json --eval 'data["events"]' \
      --eval 'f"Event: {data[\"type\"]} at {data[\"timestamp\"]}"' events.json
# Stage 1: data={"events": [...]} → [data=event1, data=event2, ...] (fan-out)
```

### Global State Management
```bash
stelp -f json --eval 'inc(f"status_{data[\"status\"]}")' \
      --eval 'f"Request: {data[\"path\"]} (total {data[\"status\"]}: {glob[f\"status_{data[\"status\"]}\"]})"'
```

### In-place Data Modification
```bash
stelp --eval 'data = parse_csv(line, headers=["name", "age"])' \
      --eval 'data["processed"] = True; data["age"] = int(data["age"])' \
      --eval 'to_json(data)'
```

## Advanced Examples

### Multi-format Processing
```bash
# Process different file types automatically
stelp -f json --eval 'data["user"]["name"]' users.json
stelp -f csv --csv-headers --eval 'data["name"]' users.csv
stelp -f fields --eval 'data[0]' users.txt
```

### Data Validation Pipeline
```bash
stelp -f csv --eval '
# Convert to dict first if we need field names
fields = parse_csv_with_headers(data, ["email", "name", "age"])
errors = []
if not regex_match(r".+@.+", fields["email"]):
    errors.append("invalid_email")
if int(fields["age"]) < 0:
    errors.append("invalid_age")

if errors:
    emit(f"INVALID line {LINENUM}: {errors}")
    skip()
else:
    fields["validated"] = True
    fields
' --eval 'f"✓ {data[\"name\"]} ({data[\"email\"]})"' users.csv
```

### Format Conversion
```bash
# CSV to JSON (manual header mapping)
stelp -f csv --eval 'parse_csv_with_headers(data, ["name", "age", "city"])' \
      --eval 'to_json(data)' input.csv > output.json

# JSON to CSV  
stelp -f json --eval 'to_csv([data["name"], data["age"], data["city"]])' input.json > output.csv

# Fields to JSON
stelp -f fields --eval '{"user": data[0], "id": data[1], "status": data[2]}' \
      --eval 'to_json(data)' users.txt > users.json
```

### Aggregation with Global State
```bash
stelp --eval 'parse_json(line)' \
      --eval '
user_id = data["user_id"]
action = data["action"]

# Track user activity
inc(f"user_{user_id}_total")
inc(f"action_{action}_total")

# Store user's latest action
glob[f"user_{user_id}_latest"] = action

f"User {user_id}: {action} (total actions: {glob[f\"user_{user_id}_total\"]})"
'
```

### Mixed Processing with Emit
```bash
stelp --eval 'parse_json(line)' \
      --eval '
if data["level"] == "ERROR":
    inc("error_count")
    emit(f"🚨 ERROR #{glob[\"error_count\"]}: {data[\"message\"]}")
    
if data["level"] == "WARN":
    inc("warning_count")

# Continue processing all records
f"[{data[\"level\"]}] {data[\"message\"]}"
'
```

## Data Type Conversions

### Awk-style Field Splitting
```python
split(line)                    # → ["field1", "field2", "field3"] (whitespace)
split(line, ",")               # → ["field1", "field2", "field3"] (comma)
split(line, ":")               # → ["field1", "field2", "field3"] (colon)
```

### CSV Parsing Options
```python
parse_csv(line)                           # → ["col1", "col2", "col3"]
parse_csv(line, headers=["a", "b", "c"])  # → {"a": "col1", "b": "col2", "c": "col3"}
parse_csv(line, headers=True)             # → {"col0": "col1", "col1": "col2", "col2": "col3"}
```

### Fan-out Behavior
```python
# String fan-out
["hello", "world"]  # → Two records: line="hello", line="world"

# Structured fan-out  
[{"id": 1}, {"id": 2}]  # → Two records: data={"id": 1}, data={"id": 2}

# Mixed fan-out
["hello", {"id": 1}]  # → Two records: line="hello", data={"id": 1}
```

## Implementation Notes

### Record Flow
1. **Input**: Stage receives either `line` (string) or `data` (dict/list)
2. **Context**: `glob` always available
3. **Processing**: User expression executes with these variables
4. **Output**: Return value determines next stage's input type
5. **Fan-out**: Lists automatically create multiple downstream records

### Variable Precedence
- Return values override variable assignments
- `glob` modifications persist across stages and files


### Error Handling
```python
# Safe data access
data.get("field", "default_value") if data else "no_data"

# Type checking
if data and "field" in data:
    data["field"]
else:
    "field_missing"

# Defensive parsing (functions should handle errors gracefully)
parsed = parse_json(line)  # Should return error dict on parse failure
if "error" in parsed:
    f"Parse failed: {parsed['error']}"
else:
    parsed["actual_field"]
```

## Backwards Compatibility

**Breaking Change**: This design removes backwards compatibility in favor of a cleaner API.

### Migration from Old Stelp
**Old approach:**
```bash
stelp --eval 'parse_csv(line)' --eval 'data[0]'
```

**New approach:**
```bash
stelp -f csv --eval 'data[0]'
```

**Old text processing:**
```bash
stelp --eval 'line.upper()'
```

**New text processing:**
```bash
stelp --eval 'line.upper()'  # No change for default text mode
```

## Migration Examples

### From String Processing
**Before:**
```bash
stelp --eval 'line.split(",")[0] + " is " + line.split(",")[1] + " years old"'
```

**After:**
```bash
stelp --eval 'parse_csv(line, headers=["name", "age"])' \
      --eval 'f"{data[\"name\"]} is {data[\"age\"]} years old"'
```

### From External Tools
**Replace jq:**
```bash
# Instead of: cat data.json | jq -r '.user.name'
stelp --eval 'parse_json(line)' --eval 'data["user"]["name"]'
```

**Replace awk:**
```bash
# Instead of: awk -F, '{print $1 " -> " $3}'
stelp --eval 'parse_csv(line)' --eval 'f"{data[0]} -> {data[2]}"'
```

## Performance Considerations

- Structured data adds minimal overhead to text processing
- JSON/CSV parsing is lazy (only when functions called)
- Global state (`glob`) uses efficient storage
- Fan-out creates records lazily

## Future Extensions

### Enhanced Parsers
- `parse_xml(line)` → dict from XML
- `parse_yaml(line)` → dict from YAML
- `parse_apache_log(line)` → dict from Apache log format

### Data Operations
- `merge(dict1, dict2)` → merge dictionaries
- `flatten(nested_dict)` → flatten nested structure
- `validate(data, schema)` → validate against schema

## Design Principles

1. **Unified Records** - Each record is either text or structured, never both
2. **Seamless Transitions** - Easy conversion between text and structured data
3. **Automatic Fan-out** - Lists naturally create multiple records
4. **Stateful Processing** - Global variables persist across records
5. **Composability** - Each stage does one thing well
6. **Performance** - Structured features don't slow down text processing

## Summary

This design enables Stelp to handle complex structured data while maintaining simplicity for text processing. The either/or record model (line XOR data) provides a clean foundation for building sophisticated data pipelines from simple command-line expressions.