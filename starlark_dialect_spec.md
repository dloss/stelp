# Stelp Starlark Dialect Specification

## Overview

Stelp implements a specialized dialect of Starlark optimized for line-by-line text processing. This dialect extends standard Starlark with stream processing primitives while maintaining core language compatibility.

## Standard Starlark Compliance

### Core Language Features ✓
- **Syntax**: Full compatibility with Starlark syntax
- **Data Types**: All standard types (int, float, string, list, dict, etc.)
- **Control Flow**: if/elif/else, for loops, comprehensions
- **Functions**: def statements, lambda expressions
- **Standard Built-ins**: Implements most standard Starlark built-ins

### Spec Deviations
- **Global State**: Adds persistent global variables across line processing
- **External Effects**: Adds I/O and control flow functions
- **Execution Model**: Line-by-line processing vs. single module execution

## Stream Processing Extensions

### Line Processing Context
Each line is processed in a fresh Starlark environment with these predefined variables:

- `line` (string): Current input line
- `LINE_NUMBER` (int): Current line number (1-based)
- `FILE_NAME` (string): Current filename (if available)

### Output Control Functions

#### `emit(text: string) -> None`
Outputs an additional line without affecting the main line transformation.

```python
# Split comma-separated values into separate lines
fields = line.split(",")
for field in fields:
    emit(field.strip())
skip()  # Don't output original line
```

#### `skip() -> None`
Prevents the current line from being output.

```python
# Filter out empty lines
if line.strip() == "":
    skip()
else:
    line.upper()
```

#### `terminate(message: string = None) -> None`
Stops processing the entire stream, optionally outputting a final message.

```python
# Stop processing after encountering an error
if "FATAL" in line:
    terminate("Processing stopped due to fatal error")
```

### Global State Management

#### `get_global(name: string, default: any = None) -> any`
Retrieves a value from persistent global storage.

#### `set_global(name: string, value: any) -> any`
Stores a value in persistent global storage, returns the value.

```python
# Count processed lines
count = get_global("line_count", 0) + 1
set_global("line_count", count)
f"[{count}] {line}"
```

### Text Processing Extensions

#### `regex_match(pattern: string, text: string) -> bool`
Tests if text matches a regular expression pattern.

#### `regex_replace(pattern: string, replacement: string, text: string) -> string`
Replaces matches of a pattern in text.

```python
# Replace all numbers with "NUM"
if regex_match("\\d+", line):
    regex_replace("\\d+", "NUM", line)
else:
    line
```

#### `parse_json(text: string) -> any`
Parses JSON text into Starlark values.

#### `to_json(value: any) -> string`
Converts Starlark values to JSON text.

#### `parse_csv(line: string, delimiter: string = ",") -> list`
Parses a CSV line into a list of fields.

#### `to_csv(values: list, delimiter: string = ",") -> string`
Converts a list to CSV format.

### Context Functions

#### `line_number() -> int`
Returns the current line number.

#### `file_name() -> string`
Returns the current filename being processed.

## Usage Patterns

### Simple Transformation
```python
# Convert to uppercase
line.upper()
```

### Filtering
```python
# Keep only lines containing "ERROR"
if "ERROR" in line:
    line
else:
    skip()
```

### Multi-line Output
```python
# Split each line into words on separate lines
words = line.split()
for word in words:
    emit(word)
skip()
```

### Stateful Processing
```python
# Number each non-empty line
if line.strip():
    count = get_global("line_count", 0) + 1
    set_global("line_count", count)
    f"{count:03d}: {line}"
else:
    line  # Keep empty lines as-is
```

### Conditional Termination
```python
# Stop after processing 1000 lines
processed = get_global("processed", 0) + 1
set_global("processed", processed)

if processed > 1000:
    terminate(f"Stopped after {processed} lines")
else:
    line.upper()
```

## Compatibility Guidelines

### Standard Starlark Code
Most standard Starlark code will run unchanged:

```python
# This works exactly as in standard Starlark
def process_fields(text):
    fields = text.split(",")
    return [field.strip().upper() for field in fields if field.strip()]

result = process_fields(line)
```

### Extension Usage
Extension functions should be used judiciously:

- Use `get_global`/`set_global` only when state must persist across lines
- Prefer standard transformations over `emit`/`skip` when possible
- Use `terminate` only for exceptional conditions, not normal end-of-input

### Error Handling
- Syntax errors fail at compile time
- Runtime errors can be handled via configuration (skip vs. fail-fast)
- Use the standard `fail()` function for intentional errors

## Migration from Standard Starlark

### Differences to Note
1. **Execution Model**: Scripts run once per line vs. once per module
2. **Variable Scope**: Local variables reset per line, globals persist
3. **I/O Model**: Implicit line input/output vs. explicit I/O
4. **Termination**: Can stop processing mid-stream

### Best Practices
1. Keep line processing scripts simple and focused
2. Use functions to organize complex logic
3. Minimize global state usage
4. Test with representative data sets
5. Handle edge cases (empty lines, malformed input)

## Performance Characteristics

- **Simple transformations**: 10K-50K lines/second
- **Complex scripts with globals**: 1K-10K lines/second  
- **Memory usage**: O(pipeline_complexity + global_state)
- **Startup time**: Sub-second for reasonable pipeline complexity