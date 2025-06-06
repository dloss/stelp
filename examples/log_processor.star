# log_processor.star
# Example Starlark script for processing log files

# Helper functions
def colorize_level(level):
    colors = {
        "ERROR": "31",    # Red
        "WARN": "33",     # Yellow  
        "INFO": "32",     # Green
        "DEBUG": "90"     # Gray
    }
    color = colors.get(level, "0")
    return f"\033[{color}m{level}\033[0m"

def increment_counter(name):
    count = glob.get(name, 0) + 1
    glob[name] = count
    return count

# Main processing logic

# Skip empty lines
line = line.strip()
if len(line) == 0:
    skip()

# Track total lines
total = increment_counter("total_lines")

# Parse log level if present
level = None
if regex_match(r'\[(ERROR|WARN|INFO|DEBUG)\]', line):
    matches = regex_find_all(r'\[(ERROR|WARN|INFO|DEBUG)\]', line)
    if len(matches) > 0:
        level = matches[0][1:-1]  # Remove brackets
        set_global("current_level", level)

# Count errors and warnings
if level == "ERROR":
    error_count = increment_counter("error_count")
    emit(f"🚨 Error #{error_count} at line {total}")
elif level == "WARN":
    increment_counter("warning_count")

# Filter out debug messages in production
if level == "DEBUG" and glob.get("production_mode", False):
    skip()

# Format timestamp (2024-01-15 10:30:45 -> 2024-01-15T10:30:45Z)
formatted_line = regex_replace(
    r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})',
    r'\1T\2Z',
    line
)

# Colorize log levels
if level:
    colored_level = colorize_level(level)
    formatted_line = regex_replace(
        f'\\[{level}\\]',
        f'[{colored_level}]',
        formatted_line
    )

# Add line number prefix
f"[{total:04d}] {formatted_line}"