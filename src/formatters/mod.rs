use crate::pipeline::context::RecordData;

/// Trait for formatting records to strings
pub trait RecordFormatter {
    fn format_record(&self, record: &RecordData) -> String;
}

pub mod logfmt;