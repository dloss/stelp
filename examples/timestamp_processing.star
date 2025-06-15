# Example: Timestamp processing with Stelp
# This script demonstrates timestamp manipulation functions

# Parse different timestamp formats and convert to Unix epoch
timestamp = parse_ts("2024-01-15T10:30:45")
log_ts = parse_ts("2024-01-15 10:30:45")
rfc_ts = parse_ts("2015-03-26T01:27:38-04:00")

# Format timestamps in different ways
iso_format = format_ts(timestamp)
custom_format = format_ts(timestamp, "%Y-%m-%d %H:%M:%S")

# Calculate time differences
current_time = now()
age_seconds = ts_diff(current_time, timestamp)

# Add time to timestamps
future_time = ts_add(timestamp, 3600)  # Add 1 hour

# Example log processing with timestamps
if regex_match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', line):
    ts = parse_ts(regex_replace(r'.*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}).*', r'\1', line))
    formatted = format_ts(ts, "%Y-%m-%d %H:%M:%S")
    f"Processed at {formatted}: {line}"
else:
    line