/// ANSI color codes for logfmt output formatting
#[derive(Debug, Clone)]
pub struct ColorScheme {
    pub key: &'static str,         // Green for field names
    pub equals: &'static str,      // No color for = separator
    pub string: &'static str,      // No color for quoted strings
    pub number: &'static str,      // No color for numbers
    pub boolean: &'static str,     // No color for true/false
    pub timestamp: &'static str,   // No color for timestamp fields
    pub level_trace: &'static str, // Cyan for trace levels
    pub level_debug: &'static str, // Bright cyan for debug levels
    pub level_info: &'static str,  // Bright green for info levels
    pub level_warn: &'static str,  // Bright yellow for warn levels
    pub level_error: &'static str, // Bright red for error levels
    pub reset: &'static str,       // Reset to default color
}

impl ColorScheme {
    /// Create color scheme for readable logfmt output
    pub fn new(use_colors: bool) -> Self {
        if use_colors {
            Self {
                key: "\x1b[32m",           // Green for field names
                equals: "",                // No color for equals signs
                string: "",                // No color for quoted values
                number: "",                // No color for numeric values
                boolean: "",               // No color for true/false
                timestamp: "",             // No color for timestamps
                level_trace: "\x1b[36m",   // Cyan for trace/finest
                level_debug: "\x1b[96m",   // Bright cyan for debug/finer/config
                level_info: "\x1b[92m",    // Bright green for info/informational/notice
                level_warn: "\x1b[93m",    // Bright yellow for warn/warning
                level_error: "\x1b[91m",   // Bright red for error/fatal/panic/etc
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
                level_trace: "",
                level_debug: "",
                level_info: "",
                level_warn: "",
                level_error: "",
                reset: "",
            }
        }
    }
}