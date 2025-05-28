pub mod builtins;
pub mod error;
pub mod pipeline;
pub mod variables;

pub use error::*;
pub use pipeline::*;
pub use variables::*;

// Re-export key types for convenience
pub use pipeline::{FilterProcessor, LineProcessor, StarlarkProcessor, StreamPipeline};
