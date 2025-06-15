# Example: Realistic log processing with timestamp parsing
# This script demonstrates parsing common log formats

# Apache Combined Log Format
# Example: 192.168.1.1 - - [25/Dec/2021:10:24:56 +0000] "GET /api/status HTTP/1.1" 200 1234
if regex_match(r'.*\[[^\]]+\].*', line):
    # Extract timestamp from brackets
    ts_str = regex_replace(r'.*\[([^\]]+)\].*', r'\1', line)
    epoch = guess_ts(ts_str)
    normalized_time = format_ts(epoch, "%Y-%m-%d %H:%M:%S")
    
    # Extract other fields for structured output
    ip = regex_replace(r'^([^\s]+).*', r'\1', line)
    method_path = regex_replace(r'.*"([^"]+)".*', r'\1', line)
    status_size = regex_replace(r'.*" (\d+ \d+).*', r'\1', line)
    
    f"{normalized_time} {ip} {method_path} {status_size}"

# Syslog format
# Example: Dec 25 10:24:56 server1 nginx: 192.168.1.1 - GET /api/status
elif regex_match(r'^\w+ \d+ \d+:\d+:\d+', line):
    # Extract timestamp (first 3 fields)
    ts_str = regex_replace(r'^(\w+ \d+ \d+:\d+:\d+).*', r'\1', line)
    epoch = guess_ts(ts_str)
    normalized_time = format_ts(epoch, "%Y-%m-%d %H:%M:%S")
    
    # Extract hostname and service
    hostname = regex_replace(r'^\w+ \d+ \d+:\d+:\d+ ([^\s]+).*', r'\1', line)
    service = regex_replace(r'^\w+ \d+ \d+:\d+:\d+ [^\s]+ ([^:]+):.*', r'\1', line)
    message = regex_replace(r'^\w+ \d+ \d+:\d+:\d+ [^\s]+ [^:]+: (.*)', r'\1', line)
    
    f"{normalized_time} {hostname} {service}: {message}"

# JSON logs with timestamp field
# Example: {"timestamp": "2021-05-01T01:17:02.604456Z", "level": "ERROR", "message": "Connection failed"}
elif regex_match(r'^\{.*"timestamp".*\}$', line):
    data = parse_json(line)
    epoch = guess_ts(data["timestamp"])
    normalized_time = format_ts(epoch, "%Y-%m-%d %H:%M:%S")
    level = data.get("level", "INFO")
    message = data.get("message", "")
    
    f"{normalized_time} [{level}] {message}"

# ISO timestamp logs
# Example: 2024-01-15T10:30:45.123Z INFO Server started on port 8080
elif regex_match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', line):
    ts_str = regex_replace(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^\s]*).*', r'\1', line)
    epoch = guess_ts(ts_str)
    normalized_time = format_ts(epoch, "%Y-%m-%d %H:%M:%S")
    rest = regex_replace(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^\s]*\s*(.*)', r'\1', line)
    
    f"{normalized_time} {rest}"

# Default: pass through unchanged
else:
    line