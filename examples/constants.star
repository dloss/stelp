# constants.star
# Shared configuration and constants

# Processing thresholds
MAX_LINE_LENGTH = 1000
MIN_FIELD_COUNT = 3
ERROR_THRESHOLD = 10

# Valid file extensions
VALID_EXTENSIONS = [".txt", ".log", ".csv", ".json"]

# Log levels in priority order
LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

# API endpoints for different environments
API_ENDPOINTS = {
    "dev": "https://api-dev.example.com",
    "staging": "https://api-staging.example.com", 
    "prod": "https://api.example.com"
}

# Common regex patterns
PATTERNS = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "ip": r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b",
    "url": r"https?://[^\s]+",
    "timestamp": r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
    "uuid": r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
}

# Status code mappings
HTTP_STATUS_MEANINGS = {
    "200": "OK",
    "201": "Created", 
    "400": "Bad Request",
    "401": "Unauthorized",
    "403": "Forbidden",
    "404": "Not Found",
    "500": "Internal Server Error"
}

# Color codes for output
COLORS = {
    "red": "\033[31m",
    "green": "\033[32m", 
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "purple": "\033[35m",
    "cyan": "\033[36m",
    "gray": "\033[90m",
    "reset": "\033[0m"
}