/// ANSI color codes for logfmt output formatting
#[derive(Debug, Clone)]
pub struct ColorScheme {
    pub key: &'static str,         // Cyan for field names
    pub equals: &'static str,      // White for = separator
    pub string: &'static str,      // Green for quoted strings
    pub number: &'static str,      // Yellow for numbers
    pub boolean: &'static str,     // Magenta for true/false
    pub timestamp: &'static str,   // Blue for timestamp fields
    pub level_error: &'static str, // Red for error/fatal levels
    pub level_warn: &'static str,  // Yellow for warn levels
    pub level_info: &'static str,  // White for info levels
    pub level_debug: &'static str, // Gray for debug/trace levels
    pub reset: &'static str,       // Reset to default color
}

impl ColorScheme {
    /// Create color scheme for readable logfmt output
    pub fn new(use_colors: bool) -> Self {
        if use_colors {
            Self {
                key: "\x1b[36m",           // Cyan for field names
                equals: "\x1b[37m",        // White for equals signs
                string: "\x1b[32m",        // Green for quoted values
                number: "\x1b[33m",        // Yellow for numeric values
                boolean: "\x1b[35m",       // Magenta for true/false
                timestamp: "\x1b[34m",     // Blue for timestamps
                level_error: "\x1b[31m",   // Red for error levels
                level_warn: "\x1b[33m",    // Yellow for warning levels
                level_info: "\x1b[37m",    // White for info levels
                level_debug: "\x1b[90m",   // Gray for debug levels
                reset: "\x1b[0m",          // Reset
            }
        } else {
            // All empty strings for no-color mode
            Self {
                key: "",
                equals: "",
                string: "",
                number: "",
                boolean: "",
                timestamp: "",
                level_error: "",
                level_warn: "",
                level_info: "",
                level_debug: "",
                reset: "",
            }
        }
    }
}