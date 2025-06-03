# Stelp --include Feature Usage Examples

## Basic Usage

### Simple Function Library
Create `helpers.star`:
```python
def clean_line(text):
    return text.strip().replace('\t', ' ')

def is_error_line(text):
    return "ERROR" in text.upper()

def format_with_timestamp(text):
    return f"[{st_line_number():04d}] {text}"
```

Use it:
```bash
stelp --include helpers.star --eval 'format_with_timestamp(clean_line(line))' data.txt
```

### Shared Constants
Create `config.star`:
```python
MAX_LINE_LENGTH = 1000
LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
OUTPUT_FORMAT = "json"
```

Use it:
```bash
stelp --include config.star --filter 'len(line) <= MAX_LINE_LENGTH' --eval 'process(line)' logs.txt
```

## Advanced Examples

### Log Processing Pipeline
Create `log_utils.star`:
```python
def parse_log_line(line):
    """Parse standard log format: [LEVEL] timestamp message"""
    if not st_regex_match(r'\[(DEBUG|INFO|WARN|ERROR)\]', line):
        return None
    
    parts = line.split(' ', 2)
    if len(parts) < 3:
        return None
        
    level = parts[0][1:-1]  # Remove brackets
    timestamp = parts[1]
    message = parts[2]
    
    return {
        "level": level,
        "timestamp": timestamp, 
        "message": message,
        "line_number": st_line_number()
    }

def colorize_level(level):
    colors = {
        "ERROR": "\033[31m",    # Red
        "WARN": "\033[33m",     # Yellow  
        "INFO": "\033[32m",     # Green
        "DEBUG": "\033[90m"     # Gray
    }
    reset = "\033[0m"
    color = colors.get(level, "")
    return f"{color}{level}{reset}"

def track_error_count():
    if "ERROR" in line:
        count = st_get_global("error_count", 0) + 1
        st_set_global("error_count", count)
        return count
    return st_get_global("error_count", 0)
```

Process logs:
```bash
stelp --include log_utils.star --eval '
log_data = parse_log_line(line)
if log_data:
    error_count = track_error_count()
    colored_level = colorize_level(log_data["level"])
    f"[{log_data[\"line_number\"]:04d}] {colored_level} {log_data[\"message\"]} (errors: {error_count})"
else:
    skip()
' server.log
```

### Multiple Includes with Override
Create `base_functions.star`:
```python
def process_line(text):
    return "BASE: " + text.upper()

VERSION = "1.0"
```

Create `enhanced_functions.star`:
```python
def process_line(text):
    return "ENHANCED: " + text.lower() + f" (v{VERSION})"

def extra_function(text):
    return "Extra: " + text
```

Use both (later overrides earlier):
```bash
stelp --include base_functions.star --include enhanced_functions.star --eval '
result = process_line(line)
if "special" in line:
    result = extra_function(result)
result
' data.txt
```

### CSV Processing with Includes
Create `csv_helpers.star`:
```python
def validate_csv_row(fields, expected_count):
    if len(fields) != expected_count:
        return False, f"Expected {expected_count} fields, got {len(fields)}"
    return True, "OK"

def format_csv_output(fields):
    return st_to_csv(fields)

def clean_csv_field(field):
    return field.strip().replace('"', '')

REQUIRED_FIELDS = 4
```

Process CSV:
```bash
stelp --include csv_helpers.star --eval '
fields = st_parse_csv(line)
valid, message = validate_csv_row(fields, REQUIRED_FIELDS)

if not valid:
    emit(f"INVALID LINE {st_line_number()}: {message}")
    skip()

# Clean and process fields
cleaned = [clean_csv_field(f) for f in fields]
cleaned.append("processed")  # Add status column

format_csv_output(cleaned)
' data.csv
```

### JSON Processing Pipeline  
Create `json_utils.star`:
```python
def safe_parse_json(text):
    try:
        return st_parse_json(text), None
    except Exception as e:
        return None, str(e)

def extract_nested_field(data, path):
    """Extract nested field using dot notation: 'user.profile.name'"""
    current = data
    for part in path.split('.'):
        if part in current:
            current = current[part]
        else:
            return None
    return current

def validate_required_fields(data, required):
    missing = []
    for field in required:
        if field not in data:
            missing.append(field)
    return missing

REQUIRED_FIELDS = ["id", "timestamp", "event"]
```

Process JSON logs:
```bash
stelp --include json_utils.star --eval '
data, error = safe_parse_json(line)

if error:
    emit(f"JSON_ERROR line {st_line_number()}: {error}")
    skip()

missing = validate_required_fields(data, REQUIRED_FIELDS)

if missing:
    emit(f"MISSING_FIELDS line {st_line_number()}: {missing}")
    skip()

# Extract and format relevant data
event_type = data["event"]
user_id = extract_nested_field(data, "user.id") or "unknown"
timestamp = data["timestamp"]

f"{timestamp} | {event_type} | user:{user_id}"
' events.json
```

## Real-World Scenarios

### Multi-format Log Processor
Create `format_detector.star`:
```python
def detect_log_format(line):
    """Detect common log formats"""
    if st_regex_match(r'\[(DEBUG|INFO|WARN|ERROR)\]', line):
        return "standard"
    elif st_regex_match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', line):
        return "iso_timestamp"
    elif line.startswith('{') and line.endswith('}'):
        return "json"
    else:
        return "unknown"

def parse_standard_log(line):
    """Parse [LEVEL] timestamp message format"""
    match = st_regex_find_all(r'\[([^\]]+)\]\s+(\S+)\s+(.*)', line)
    if match:
        return {"level": match[0], "timestamp": match[1], "message": match[2]}
    return None

def parse_iso_log(line):
    """Parse ISO timestamp format logs"""
    parts = line.split(' ', 2)
    if len(parts) >= 3:
        return {"timestamp": parts[0], "level": parts[1], "message": parts[2]}
    return None
```

Process mixed format logs:
```bash
stelp --include format_detector.star --eval '
format = detect_log_format(line)
result = ""

if format == "standard":
    data = parse_standard_log(line)
    if data:
        result = f"STD | {data[\"level\"]} | {data[\"message\"]}"
elif format == "iso_timestamp":
    data = parse_iso_log(line)
    if data:
        result = f"ISO | {data[\"level\"]} | {data[\"message\"]}"
elif format == "json":
    json_data, error = safe_parse_json(line)
    if json_data and "message" in json_data:
        result = f"JSON | {json_data.get(\"level\", \"INFO\")} | {json_data[\"message\"]}"
else:
    result = f"UNK | {line}"

result
' mixed_logs.txt
```

### Error Aggregation and Reporting
Create `error_tracker.star`:
```python
def track_error(error_type, message):
    """Track errors by type with counts and examples"""
    # Increment counter for this error type
    count_key = f"error_{error_type}_count"
    count = st_get_global(count_key, 0) + 1
    st_set_global(count_key, count)
    
    # Store first few examples
    examples_key = f"error_{error_type}_examples"
    examples = st_get_global(examples_key, [])
    if len(examples) < 3:  # Keep only first 3 examples
        examples.append(message)
        st_set_global(examples_key, examples)
    
    return count

def generate_error_report():
    """Generate summary of all tracked errors"""
    total_errors = st_get_global("total_errors", 0)
    if total_errors == 0:
        return "No errors found"
    
    report = [f"ERROR REPORT: {total_errors} total errors"]
    
    # This is simplified - in practice you'd iterate through known error types
    for error_type in ["parse", "validation", "network"]:
        count = st_get_global(f"error_{error_type}_count", 0)
        if count > 0:
            report.append(f"  {error_type}: {count} occurrences")
    
    return "\n".join(report)

def increment_total_errors():
    count = st_get_global("total_errors", 0) + 1
    st_set_global("total_errors", count)
    return count
```

Track and report errors:
```bash
stelp --include error_tracker.star --eval '
result = line

if "ERROR" in line:
    increment_total_errors()
    
    if "ParseError" in line:
        count = track_error("parse", line)
        result = f"PARSE_ERROR #{count}: {line}"
    elif "ValidationError" in line:
        count = track_error("validation", line)
        result = f"VALIDATION_ERROR #{count}: {line}"
    else:
        result = f"OTHER_ERROR: {line}"

# Generate report at end
if st_line_number() > 1000:  # After processing many lines
    emit("=" * 50)
    emit(generate_error_report())
    exit()

result
' application.log
```

## Best Practices

### 1. Organize by Functionality
```
includes/
├── constants.star       # Shared constants and configuration
├── text_utils.star     # String manipulation functions
├── json_utils.star     # JSON parsing and validation
├── csv_utils.star      # CSV processing helpers
├── log_utils.star      # Log parsing and formatting
└── validators.star     # Data validation functions
```

### 2. Use Descriptive Function Names
```python
# Good
def extract_email_from_log_line(line):
    return st_regex_find_all(PATTERNS["email"], line)

# Less clear
def extract(line):
    return st_regex_find_all(r"[^@]+@[^@]+", line)
```

### 3. Handle Errors Gracefully
```python
def safe_parse_json(text):
    try:
        return st_parse_json(text), None
    except Exception as e:
        return None, f"JSON parse error: {str(e)}"

def safe_extract_field(data, field, default=""):
    if data and field in data:
        return data[field]
    return default
```

### 4. Use Include Order for Overrides
```bash
# Base functionality first, then specializations
stelp --include base_processors.star \
      --include company_specific.star \
      --include project_overrides.star \
      --eval 'process_line(line)' data.txt
```

### 5. Document Include Dependencies
```python
# log_enhanced.star
# Requires: constants.star (for LOG_LEVELS)
# Requires: text_utils.star (for clean_line function)

def enhanced_log_processor(line):
    cleaned = clean_line(line)  # from text_utils.star
    # ... rest of processing
```