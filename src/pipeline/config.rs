/// Configuration for pipeline behavior
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub error_strategy: ErrorStrategy,
    pub debug: bool,
    pub buffer_size: usize,
    pub max_line_length: usize,
    pub progress_interval: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            error_strategy: ErrorStrategy::Skip,
            debug: false,
            buffer_size: 65536,       // 64KB
            max_line_length: 1048576, // 1MB
            progress_interval: 0,     // Disabled
        }
    }
}

/// Simple error handling strategy
#[derive(Debug, Clone)]
pub enum ErrorStrategy {
    /// Skip problematic lines and continue processing
    Skip,
    /// Stop processing on first error
    FailFast,
}
