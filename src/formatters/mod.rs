use crate::pipeline::context::RecordData;

/// Trait for formatting records to strings
pub trait RecordFormatter {
    fn format_record(&self, record: &RecordData) -> String;
    fn format_record_with_key_order(&self, record: &RecordData, _key_order: Option<&[String]>) -> String {
        // Default implementation ignores key order
        self.format_record(record)
    }
}

pub mod logfmt;
