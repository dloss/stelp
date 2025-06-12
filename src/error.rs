// src/error.rs
use std::fmt;

#[derive(Debug)]
pub enum ProcessingError {
    /// I/O related errors (file reading, writing, etc.)
    IoError(std::io::Error),

    /// Script compilation or execution errors
    ScriptError {
        step: String,
        line: usize,
        source: anyhow::Error,
    },

    /// Output formatting errors
    OutputError(String),
}

impl fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProcessingError::IoError(e) => write!(f, "I/O error: {}", e),
            ProcessingError::ScriptError { step, line, source } => {
                write!(f, "Script error in {}, line {}: {}", step, line, source)
            }
            ProcessingError::OutputError(msg) => write!(f, "Output error: {}", msg),
        }
    }
}

impl std::error::Error for ProcessingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ProcessingError::IoError(e) => Some(e),
            ProcessingError::ScriptError { source, .. } => source.source(),
            ProcessingError::OutputError(_) => None,
        }
    }
}

impl From<std::io::Error> for ProcessingError {
    fn from(e: std::io::Error) -> Self {
        ProcessingError::IoError(e)
    }
}

// Compilation error type for script validation
#[derive(Debug)]
pub enum CompilationError {
    SyntaxError(starlark::Error),
    ValidationError(String),
}

impl fmt::Display for CompilationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompilationError::SyntaxError(e) => write!(f, "Syntax error: {}", e),
            CompilationError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for CompilationError {}

impl From<starlark::Error> for CompilationError {
    fn from(e: starlark::Error) -> Self {
        CompilationError::SyntaxError(e)
    }
}
