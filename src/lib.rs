// src/lib.rs
pub mod error;
pub mod pipeline;
pub mod variables;

pub use error::*;
pub use pipeline::*;

pub use pipeline::config::{ErrorStrategy, PipelineConfig};
pub use pipeline::context::{ProcessResult, ProcessingStats, RecordContext, RecordData};
pub use pipeline::processors::{FilterProcessor, StarlarkProcessor};
pub use pipeline::stream::{RecordProcessor, StreamPipeline};
