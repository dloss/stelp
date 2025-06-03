# helpers.star
# Utility functions for text processing

def clean_line(text):
    """Remove leading/trailing whitespace and normalize internal spacing"""
    return st_regex_replace(r'\s+', ' ', text.strip())

def extract_timestamp(text):
    """Extract ISO timestamp from text"""
    matches = st_regex_find_all(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', text)
    if len(matches) > 0:
        return matches[0]
    return None

def is_log_level(text, level):
    """Check if line contains a specific log level"""
    pattern = r'\[' + level + r'\]'
    return st_regex_match(pattern, text.upper())

def colorize_log_level(text):
    """Add ANSI colors to log levels"""
    result = text
    result = st_regex_replace(r'\[ERROR\]', '\033[31m[ERROR]\033[0m', result)
    result = st_regex_replace(r'\[WARN\]', '\033[33m[WARN]\033[0m', result)  
    result = st_regex_replace(r'\[INFO\]', '\033[32m[INFO]\033[0m', result)
    result = st_regex_replace(r'\[DEBUG\]', '\033[90m[DEBUG]\033[0m', result)
    return result

def extract_json_field(text, field):
    """Extract a field from a JSON line"""
    try:
        data = st_parse_json(text)
        if field in data:
            return data[field]
        return None
    except:
        return None

def format_csv_output(fields):
    """Format a list as CSV with proper escaping"""
    return st_to_csv(fields)

def increment_counter(name):
    """Increment a global counter and return new value"""
    count = st_get_global(name, 0) + 1
    st_set_global(name, count)
    return count