#!/usr/bin/env python3
"""
Generate sample multiline log data for testing Stelp's chunking features.

This script creates sample logs that demonstrate different multiline patterns:
1. Java stack traces
2. Python tracebacks  
3. Multi-line SQL queries
4. Configuration blocks
"""

import datetime

def generate_sample_logs():
    """Generate various types of multiline log entries."""
    
    now = datetime.datetime.now()
    
    # Java application logs with stack traces
    java_logs = f"""2024-01-15 10:00:00 INFO  Starting UserService
2024-01-15 10:00:01 ERROR Exception in user registration
java.lang.RuntimeException: Database connection failed
    at com.example.UserService.registerUser(UserService.java:42)
    at com.example.UserController.signup(UserController.java:23)
    at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.sql.SQLException: Connection timeout
    at com.mysql.jdbc.Driver.connect(Driver.java:115)
    ... 3 more
2024-01-15 10:00:02 INFO  Retrying user registration
2024-01-15 10:00:03 INFO  User registration successful"""

    # Python application logs with tracebacks
    python_logs = f"""[2024-01-15 10:05:00] INFO: Processing user data
[2024-01-15 10:05:01] ERROR: Failed to process user data
Traceback (most recent call last):
  File "/app/user_processor.py", line 25, in process_user
    result = validate_email(user.email)
  File "/app/validators.py", line 15, in validate_email
    return regex.match(pattern, email)
AttributeError: 'NoneType' object has no attribute 'match'
[2024-01-15 10:05:02] INFO: Skipping invalid user record
[2024-01-15 10:05:03] INFO: Processing complete"""

    # Configuration sections
    config_logs = f"""=== DATABASE CONFIG ===
host: localhost
port: 5432
database: myapp
username: app_user
connection_pool: 10
---
=== REDIS CONFIG ===
host: redis.internal
port: 6379
database: 0
password: secret123
---
=== API CONFIG ===
base_url: https://api.example.com
timeout: 30
retries: 3"""

    return java_logs, python_logs, config_logs

if __name__ == "__main__":
    java_logs, python_logs, config_logs = generate_sample_logs()
    
    print("=== Java Application Logs ===")
    print(java_logs)
    print("\n=== Python Application Logs ===") 
    print(python_logs)
    print("\n=== Configuration Sections ===")
    print(config_logs)