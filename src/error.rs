#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Script error in step '{step}' at line {line}: {source}")]
    ScriptError {
        step: String,
        line: usize,
        #[source]
        source: anyhow::Error,
    },
    
    #[error("Parse error in step '{step}': {message}")]
    ParseError {
        step: String,
        message: String,
    },
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Line too long: {length} > {max_length}")]
    LineTooLong { length: usize, max_length: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum CompilationError {
    #[error("Starlark syntax error: {0}")]
    SyntaxError(String),
    
    #[error("File not found: {0}")]
    FileNotFound(String),
    
    #[error("Invalid configuration: {0}")]
    ConfigError(String),
}

impl From<starlark::Error> for CompilationError {
    fn from(err: starlark::Error) -> Self {
        CompilationError::SyntaxError(format!("{}", err))
    }
}

impl From<std::io::Error> for CompilationError {
    fn from(err: std::io::Error) -> Self {
        CompilationError::FileNotFound(err.to_string())
    }
}