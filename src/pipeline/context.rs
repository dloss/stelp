use crate::error::ProcessingError;
use crate::variables::GlobalVariables;
use std::borrow::Cow;
use std::time::Duration;

/// Context passed to each processor
pub struct LineContext<'a> {
    pub line_number: usize,
    pub file_name: Option<&'a str>,
    pub global_vars: &'a GlobalVariables,
}

/// Result of processing a single line
#[derive(Debug)]
pub enum ProcessResult {
    /// Transform line (use Cow to avoid allocation if unchanged)
    Transform(Cow<'static, str>),
    /// Multiple output lines
    MultipleOutputs(Vec<String>),
    /// Transform with additional emitted lines
    TransformWithEmissions {
        primary: Option<Cow<'static, str>>,
        emissions: Vec<String>,
    },
    /// Skip this line (filter out)
    Skip,
    /// Stop processing entirely, with optional final output
    Terminate(Option<Cow<'static, str>>),
    /// Processing error
    Error(ProcessingError),
}

/// Runtime statistics
#[derive(Debug, Default, Clone)]
pub struct ProcessingStats {
    pub lines_processed: usize,
    pub lines_output: usize,
    pub lines_skipped: usize,
    pub errors: usize,
    pub processing_time: Duration,
}

/// Shared context across all processors
pub struct PipelineContext {
    pub global_vars: GlobalVariables,
    pub line_number: usize,
    pub total_processed: usize,
    pub file_name: Option<String>,
}

impl PipelineContext {
    pub fn new() -> Self {
        PipelineContext {
            global_vars: GlobalVariables::new(),
            line_number: 0,
            total_processed: 0,
            file_name: None,
        }
    }
}
