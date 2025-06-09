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
stelp --input-format csv file.csv           # Auto-parse as CSV → data = {"col1": "value1", "col2": "value2", ...}
stelp --input-format jsonl file.jsonl       # Auto-parse as JSONL → data = {...}
stelp --input-format fields file.txt        # Auto-split on whitespace → data = ["field1", "field2", ...]
stelp --input-format kv file.conf           # Auto-parse key=value → data = {"key": "value", ...}
stelp file.txt                              # Default: line-based → line = "text content"

# Short form
stelp -f csv file.csv
stelp -f jsonl file.jsonl
stelp -f fields file.txt
stelp -f kv file.txt
```

### Default Parsing Behavior
- **csv**: Standard CSV parsing with comma delimiter, auto-generated headers → `{"col1": "value1", "col2": "value2", ...}`
- **jsonl**: JSONL (JSON Lines) object/array parsing → `{...}` or `[...]`
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
parse_csv(line, headers=None, delimiter=",")  # → dict with column headers
parse_json(line)                          # → dict/list
parse_kv(line, sep="=", delim=" ")       # → dict
dump_csv(data, delimiter=",")              # dict/list → CSV string
dump_json(data)                            # → JSON string
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
# CSV auto-parsed with generated headers (col1, col2, col3...)
col1 = data["col1"]
col3 = data["col3"]
stelp -f csv --eval 'f"{col1} lives in {col3}"' users.csv

# Custom headers
stelp -f csv --eval 'parse_csv(data, ["name", "age", "city"])' \
      --eval 'name = data["name"]; city = data["city"]; f"{name} lives in {city}"' users.csv
```

### Automatic JSONL Processing
```bash
# Process JSONL objects
user_id = data["id"]
status = data["status"]
stelp -f jsonl --eval 'f"User {user_id}: {status}"' events.jsonl

# Extract specific fields - need to assign to variables first
user_email = data["user"]["email"]
stelp -f jsonl --eval 'user_email' nested.jsonl
```

### Automatic Field Splitting
```bash
# Process whitespace-separated fields (like awk)
field1 = data[0]
field3 = data[2]
stelp -f fields --eval 'f"{field1} -> {field3}"' logfile.txt

# Need custom delimiter? Parse manually
stelp --eval 'line.split(":")' --eval 'data[0]' /etc/passwd
```

### Automatic Key-Value Processing
```bash
# Process simple config files (key=value format)
stelp -f kv --eval 'f"Setting: {data}"' config.conf

# Extract specific settings
enabled = data.get("enabled")
name = data.get("name")
stelp -f kv --filter 'enabled == "true"' --eval 'name' settings.conf
```

### Mixed Processing Pipeline
```bash
# Parse JSONL, process, output as CSV
name = data["name"]
age = data["age"] 
city = data["city"]
stelp -f jsonl --eval 'dump_csv([name, age, city])' users.jsonl
```

### Fan-out Processing
```bash
# JSONL array auto-parsing with fan-out
events = data["events"]
stelp -f jsonl --eval 'events' \
      --eval 'event_type = data["type"]; timestamp = data["timestamp"]; f"Event: {event_type} at {timestamp}"' events.jsonl
# Stage 1: data={"events": [...]} → [data=event1, data=event2, ...] (fan-out)
```

### Global State Management
```bash
status = data["status"]
path = data["path"]
counter_key = f"status_{status}"
stelp -f jsonl --eval 'inc(counter_key)' \
      --eval 'total = glob[counter_key]; f"Request: {path} (total {status}: {total})"'
```

### In-place Data Modification
```bash
stelp -f csv --eval 'data["processed"] = True; data["col2"] = int(data["col2"])' \
      --eval 'dump_json(data)'
```

## Advanced Examples

### Multi-format Processing
```bash
# Process different file types automatically
user_name = data["user"]["name"]
stelp -f jsonl --eval 'user_name' users.jsonl
stelp -f csv --eval 'data["col1"]' users.csv  # Using auto-generated headers
stelp -f fields --eval 'data[0]' users.txt
```

### Data Validation Pipeline
```bash
stelp -f csv --eval '
# CSV already parsed as dict with col1, col2, col3 headers
errors = []
email = data["col1"]
age_str = data["col2"]
if not regex_match(r".+@.+", email):
    errors.append("invalid_email")
if int(age_str) < 0:
    errors.append("invalid_age")

if errors:
    emit(f"INVALID line {LINENUM}: {errors}")
    skip()
else:
    data["validated"] = True
    data
' --eval 'col1 = data["col1"]; col2 = data["col2"]; f"✓ {col1} ({col2})"' users.csv
```

### Format Conversion
```bash
# CSV to JSON (using auto-generated headers)
stelp -f csv --eval 'dump_json(data)' input.csv > output.json

# CSV to JSON with custom field names
col1 = data["col1"]
col2 = data["col2"] 
col3 = data["col3"]
stelp -f csv --eval '{"name": col1, "age": col2, "city": col3}' \
      --eval 'dump_json(data)' input.csv > output.json

# JSONL to CSV  
name = data["name"]
age = data["age"]
city = data["city"]
stelp -f jsonl --eval 'dump_csv([name, age, city])' input.jsonl > output.csv

# Fields to JSON
user = data[0]
user_id = data[1]
status = data[2]
stelp -f fields --eval '{"user": user, "id": user_id, "status": status}' \
      --eval 'dump_json(data)' users.txt > users.json
```

### Aggregation with Global State
```bash
stelp --eval 'parse_json(line)' \
      --eval '
user_id = data["user_id"]
action = data["action"]

# Track user activity
user_total_key = f"user_{user_id}_total"
action_total_key = f"action_{action}_total"
user_latest_key = f"user_{user_id}_latest"

inc(user_total_key)
inc(action_total_key)

# Store user's latest action
glob[user_latest_key] = action

total_actions = glob[user_total_key]
f"User {user_id}: {action} (total actions: {total_actions})"
'
```

### Mixed Processing with Emit
```bash
stelp --eval 'parse_json(line)' \
      --eval '
level = data["level"]
message = data["message"]

if level == "ERROR":
    inc("error_count")
    error_num = glob["error_count"]
    emit(f"🚨 ERROR #{error_num}: {message}")
    
if level == "WARN":
    inc("warning_count")

# Continue processing all records
f"[{level}] {message}"
'
```

## Data Type Conversions

### CSV Parsing Options
```python
parse_csv(line)                           # → {"col1": "value1", "col2": "value2", ...}
parse_csv(line, headers=["a", "b", "c"])  # → {"a": "value1", "b": "value2", "c": "value3"}
parse_csv(line, headers=True)             # → Use first row as headers
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

### CSV Header Generation
- Auto-generated headers follow pattern: `col1`, `col2`, `col3`, etc.
- First column is always `col1` (not `col0`)
- Headers can be overridden with explicit `headers` parameter

### Error Handling
```python
# Safe data access
col1 = data.get("col1", "default_value") if data else "no_data"

# Type checking
if data and "col1" in data:
    col1 = data["col1"]
else:
    col1 = "field_missing"

# Defensive parsing (functions should handle errors gracefully)
parsed = parse_json(line)  # Should return error dict on parse failure
if "error" in parsed:
    error_msg = parsed["error"]
    f"Parse failed: {error_msg}"
else:
    actual_field = parsed["actual_field"]
    actual_field
```

## Performance Considerations

- Structured data adds minimal overhead to text processing
- JSON/CSV parsing is lazy (only when functions called)
- Global state (`glob`) uses efficient storage
- Fan-out creates records lazily
- CSV header generation is efficient and consistent

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
7. **Consistent Headers** - CSV parsing always returns dict with predictable keys

## Summary

This design enables Stelp to handle complex structured data while maintaining simplicity for text processing. The either/or record model (line XOR data) provides a clean foundation for building sophisticated data pipelines from simple command-line expressions. CSV processing now consistently returns dictionaries with auto-generated headers, making field access predictable and intuitive.