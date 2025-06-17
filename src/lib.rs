// src/lib.rs
pub mod chunking;
pub mod error;
pub mod input_format;
pub mod output_format;
pub mod pattern_extraction;
pub mod pipeline;
pub mod processors;
pub mod variables;

pub use error::*;
pub use pipeline::*;

pub use pipeline::config::{ErrorStrategy, PipelineConfig};
pub use pipeline::context::{ProcessResult, ProcessingStats, RecordContext, RecordData};
pub use pipeline::processors::{DeriveProcessor, ExtractProcessor, FilterProcessor, StarlarkProcessor};
pub use processors::{WindowProcessor};
pub use pipeline::stream::{RecordProcessor, StreamPipeline};
