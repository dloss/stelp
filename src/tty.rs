use std::io::IsTerminal;

/// Check if stdout is connected to a TTY
pub fn is_stdout_tty() -> bool {
    std::io::stdout().is_terminal()
}

/// Determine if colors should be used based on environment
pub fn should_use_colors() -> bool {
    // Don't use colors if not on TTY
    if !is_stdout_tty() {
        return false;
    }

    // Respect NO_COLOR environment variable (https://no-color.org/)
    if std::env::var("NO_COLOR").is_ok() {
        return false;
    }

    // Check FORCE_COLOR for CI environments that support colors
    if std::env::var("FORCE_COLOR").is_ok() {
        return true;
    }

    // Default: use colors for TTY
    true
}