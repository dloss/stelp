# Stelp Prelude - Automatically included helper functions
# This file provides convenient functions for common operations in Stelp pipelines

def inc(key, amount=1):
    """Increment a counter in the global dictionary
    
    Args:
        key: Counter name (string)
        amount: Amount to increment by (default: 1)
        
    Returns:
        New counter value (integer)
        
    Examples:
        count = inc("total")           # Increment total by 1
        score = inc("points", 10)      # Increment points by 10
        line_num = inc("lines")        # Count lines processed
    """
    current = glob.get(key, 0) + amount
    glob[key] = current
    return current

# End of Stelp Prelude