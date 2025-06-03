#!/bin/bash

# Manual test script for --include feature

set -e

echo "=== Testing --include feature manually ==="

# Create test files
echo "Creating test files..."

cat > test_helpers.star << 'EOF'
def greet(name):
    return "Hello, " + name + "!"

def double_length(text):
    return len(text) * 2

PREFIX = ">>> "
EOF

cat > test_overrides.star << 'EOF'
def greet(name):
    return "Howdy, " + name + "!"
EOF

echo "Created test_helpers.star and test_overrides.star"

# Test 1: Basic include
echo ""
echo "=== Test 1: Basic include ==="
echo "Input: Alice"
echo "Expected: Hello, Alice!"
echo "Actual:"
echo "Alice" | cargo run -q -- --include test_helpers.star --eval 'greet(line)'

# Test 2: Multiple includes (override)
echo ""
echo "=== Test 2: Multiple includes with override ==="
echo "Input: Alice"
echo "Expected: Howdy, Alice!"
echo "Actual:"
echo "Alice" | cargo run -q -- --include test_helpers.star --include test_overrides.star --eval 'greet(line)'

# Test 3: Include with constants
echo ""
echo "=== Test 3: Include with constants ==="
echo "Input: Alice"
echo "Expected: >>> Hello, Alice! (10)"
echo "Actual:"
echo "Alice" | cargo run -q -- --include test_helpers.star --eval 'PREFIX + greet(line) + " (" + str(double_length(line)) + ")"'

# Test 4: Include with filter
echo ""
echo "=== Test 4: Include with filter ==="
echo "Input: hi\\nhello\\nbye\\nworld"
echo "Expected: (only names > 3 chars)"

cat > test_validators.star << 'EOF'
def is_valid(text):
    return len(text) > 3
EOF

echo -e "hi\nhello\nbye\nworld" | cargo run -q -- --include test_validators.star --filter 'is_valid(line)' --eval 'line.upper()'

# Test 5: Error case - missing file
echo ""
echo "=== Test 5: Error case - missing file ==="
echo "Expected: Error message about missing file"
echo "Actual:"
echo "test" | cargo run -- --include missing_file.star --eval 'line' 2>&1 || echo "(Error caught as expected)"

# Cleanup
echo ""
echo "Cleaning up test files..."
rm -f test_helpers.star test_overrides.star test_validators.star

echo "=== Manual tests complete ==="