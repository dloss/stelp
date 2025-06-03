# Stelp -I/--include Feature Usage Examples

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
stelp -I helpers.star -e 'format_with_timestamp(clean_line(line))' data.txt
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
stelp -I config.star --filter 'len(line) <= MAX_LINE_LENGTH' -e 'process(line)' logs.txt
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
stelp -I log_utils.star -e '
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
stelp -I base_functions.star -I enhanced_functions.star -e '
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
stelp -I csv_helpers.star -e '
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
stelp -I json_utils.star -e '
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
        return "iso