// src/pipeline/global_functions.rs
use crate::processors::window::WINDOW_CONTEXT;
use crate::variables::GlobalVariables;
use chrono::{DateTime, Datelike, NaiveDateTime, TimeZone, Utc};
use dateparser;
use once_cell::sync::Lazy;
use regex::Regex;
use starlark::starlark_module;
use starlark::values::tuple::UnpackTuple;
use starlark::values::{Heap, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;

thread_local! {
    pub(crate) static EMIT_BUFFER: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    pub(crate) static SKIP_FLAG: Cell<bool> = const { Cell::new(false) };
    pub(crate) static EXIT_FLAG: Cell<bool> = const { Cell::new(false) };
    pub(crate) static EXIT_MESSAGE: RefCell<Option<String>> = const { RefCell::new(None) };
    pub(crate) static EXIT_CODE: Cell<i32> = const { Cell::new(0) };
    pub(crate) static CURRENT_CONTEXT: RefCell<Option<(*const GlobalVariables, usize, Option<String>)>> = const { RefCell::new(None) };
    pub(crate) static DEBUG_BUFFER: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    pub(crate) static IS_DATA_MODE: Cell<bool> = const { Cell::new(false) };
    pub(crate) static CURRENT_MODULE: RefCell<Option<*const starlark::environment::Module>> = const { RefCell::new(None) };
}

// Precompiled builtin regex patterns
pub static BUILTIN_REGEXES: Lazy<HashMap<&'static str, (Regex, &'static str)>> = Lazy::new(|| {
    let mut patterns: HashMap<&'static str, (Regex, &'static str)> = HashMap::new();

    patterns.insert(
        "duration",
        (
            Regex::new(r"(?:\d+\.?\d*(?:ns|us|μs|ms|s|m|h|d|w|y))+").unwrap(),
            "duration (e.g., '2h30m', '1.5s')",
        ),
    );
    patterns.insert(
        "email",
        (
            Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b").unwrap(),
            "email address",
        ),
    );
    patterns.insert(
        "fqdn",
        (
            Regex::new(r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}\b")
                .unwrap(),
            "fully qualified domain name (FQDN)",
        ),
    );
    patterns.insert(
        "function",
        (
            Regex::new(r"\b[a-zA-Z_][a-zA-Z0-9_]*\s*\(").unwrap(),
            "function call",
        ),
    );
    patterns.insert(
        "gitcommit",
        (
            Regex::new(r"\b[a-f0-9]{7,40}\b").unwrap(),
            "git commit hash",
        ),
    );
    patterns.insert(
        "hexnum",
        (
            Regex::new(r"\b0x[a-fA-F0-9]+\b").unwrap(),
            "hex number with 0x prefix",
        ),
    );
    patterns.insert(
        "hexcolor",
        (
            Regex::new(r"#[a-fA-F0-9]{3,8}\b").unwrap(),
            "hex color code",
        ),
    );
    patterns.insert(
        "ipv4",
        (
            Regex::new(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b").unwrap(),
            "IPv4 address",
        ),
    );
    patterns.insert(
        "ipv4_port",
        (
            Regex::new(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?):[0-9]+\b").unwrap(),
            "IPv4 address:port",
        ),
    );
    patterns.insert(
        "ipv6",
        (
            Regex::new(r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b|::1\b|\b::ffff:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b").unwrap(),
            "IPv6 address",
        ),
    );
    patterns.insert(
        "isotime",
        (
            Regex::new(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\b")
                .unwrap(),
            "ISO 8601 datetime string",
        ),
    );
    patterns.insert(
        "json",
        (Regex::new(r"\{.*\}|\[.*\]").unwrap(), "JSON string"),
    );
    patterns.insert(
        "jwt",
        (
            Regex::new(r"\b[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b").unwrap(),
            "JSON Web Token (JWT)",
        ),
    );
    patterns.insert(
        "mac",
        (
            Regex::new(r"\b(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}\b").unwrap(),
            "MAC address",
        ),
    );
    patterns.insert(
        "md5",
        (Regex::new(r"\b[a-f0-9]{32}\b").unwrap(), "MD5 hash"),
    );
    patterns.insert(
        "num",
        (
            Regex::new(r"[+-]?(?:\d*\.)?\d+").unwrap(),
            "number (integer or float)",
        ),
    );
    patterns.insert(
        "path",
        (Regex::new(r"(?:/[^/\s]*)+/?").unwrap(), "Unix file path"),
    );
    patterns.insert(
        "oauth",
        (
            Regex::new(r"\b[A-Za-z0-9_-]{20,}\b").unwrap(),
            "OAuth token",
        ),
    );
    patterns.insert(
        "sha1",
        (Regex::new(r"\b[a-f0-9]{40}\b").unwrap(), "SHA-1 hash"),
    );
    patterns.insert(
        "sha256",
        (Regex::new(r"\b[a-f0-9]{64}\b").unwrap(), "SHA-256 hash"),
    );
    patterns.insert(
        "sql",
        (
            Regex::new(
                r"(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|GRANT|REVOKE)\b.*?;?",
            )
            .unwrap(),
            "SQL query",
        ),
    );
    patterns.insert("url", (Regex::new(r"https?://[^\s<>]+").unwrap(), "URL"));
    patterns.insert(
        "uuid",
        (
            Regex::new(
                r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
            )
            .unwrap(),
            "UUID",
        ),
    );
    patterns.insert(
        "version",
        (
            Regex::new(r"\b\d+(?:\.\d+){1,3}(?:-[a-zA-Z0-9]+(?:\.[a-zA-Z0-9]+)*)?\b").unwrap(),
            "software version identifier",
        ),
    );
    patterns.insert(
        "winregistry",
        (
            Regex::new(r"\\(?:HKEY_[A-Z_]+\\)?(?:[^\\]+\\)*[^\\]*").unwrap(),
            "Windows registry key",
        ),
    );

    patterns
});

pub fn get_pattern_list() -> Vec<(&'static str, &'static str)> {
    let mut patterns: Vec<_> = BUILTIN_REGEXES
        .iter()
        .map(|(name, (_, desc))| (*name, *desc))
        .collect();
    patterns.sort_by_key(|(name, _)| *name);
    patterns
}

#[starlark_module]
pub(crate) fn global_functions(builder: &mut starlark::environment::GlobalsBuilder) {
    // Control flow functions
    fn emit(text: String) -> anyhow::Result<starlark::values::none::NoneType> {
        // Check if we're in data mode by looking at the current data variable
        let is_data_mode = CURRENT_MODULE.with(|module_ptr| {
            if let Some(module_ptr) = *module_ptr.borrow() {
                unsafe {
                    if let Some(data_value) = (*module_ptr).get("data") {
                        !data_value.is_none()
                    } else {
                        false
                    }
                }
            } else {
                IS_DATA_MODE.with(|flag| flag.get()) // fallback to thread-local flag
            }
        });

        if is_data_mode {
            return Err(anyhow::anyhow!(
                "emit() can only be used in line mode (when 'data' is None)"
            ));
        }
        EMIT_BUFFER.with(|buffer| {
            buffer.borrow_mut().push(text);
        });
        Ok(starlark::values::none::NoneType)
    }

    fn emit_all<'v>(
        heap: &'v Heap,
        items: Value<'v>,
    ) -> anyhow::Result<starlark::values::none::NoneType> {
        // Check if we're in data mode by looking at the current data variable
        let is_data_mode = CURRENT_MODULE.with(|module_ptr| {
            if let Some(module_ptr) = *module_ptr.borrow() {
                unsafe {
                    if let Some(data_value) = (*module_ptr).get("data") {
                        !data_value.is_none()
                    } else {
                        false
                    }
                }
            } else {
                IS_DATA_MODE.with(|flag| flag.get()) // fallback to thread-local flag
            }
        });

        if is_data_mode {
            return Err(anyhow::anyhow!(
                "emit_all() can only be used in line mode (when 'data' is None)"
            ));
        }
        match items.iterate(heap) {
            Ok(iterable) => {
                EMIT_BUFFER.with(|buffer| {
                    let mut buffer = buffer.borrow_mut();
                    for item in iterable {
                        buffer.push(item.to_string());
                    }
                });
                Ok(starlark::values::none::NoneType)
            }
            Err(_) => Err(anyhow::anyhow!("emit_all() requires an iterable argument")),
        }
    }

    fn skip() -> anyhow::Result<starlark::values::none::NoneType> {
        SKIP_FLAG.with(|flag| flag.set(true));
        Ok(starlark::values::none::NoneType)
    }

    fn set_data_mode() -> anyhow::Result<starlark::values::none::NoneType> {
        IS_DATA_MODE.with(|flag| flag.set(true));
        Ok(starlark::values::none::NoneType)
    }

    fn exit<'v>(
        arg1: Option<starlark::values::Value<'v>>,
        arg2: Option<String>,
    ) -> anyhow::Result<starlark::values::none::NoneType> {
        let (code, msg) = match (arg1, arg2) {
            // exit() - no arguments
            (None, None) => (0, None),
            // exit(3) - integer as first argument
            (Some(val), None) if val.unpack_i32().is_some() => (val.unpack_i32().unwrap(), None),
            // exit("message") - string as first argument (backward compatibility)
            (Some(val), None) if val.unpack_str().is_some() => {
                (0, Some(val.unpack_str().unwrap().to_string()))
            }
            // exit(3, "message") - both arguments
            (Some(val), Some(msg)) if val.unpack_i32().is_some() => {
                (val.unpack_i32().unwrap(), Some(msg))
            }
            // Invalid usage
            _ => {
                return Err(anyhow::anyhow!(
                    "exit() expects exit(code=0, msg=None) - code must be an integer"
                ))
            }
        };

        EXIT_FLAG.with(|flag| flag.set(true));
        EXIT_MESSAGE.with(|message| {
            *message.borrow_mut() = msg;
        });
        EXIT_CODE.with(|exit_code| {
            exit_code.set(code);
        });
        Ok(starlark::values::none::NoneType)
    }

    fn print(message: String) -> anyhow::Result<starlark::values::none::NoneType> {
        eprintln!("{}", message);
        Ok(starlark::values::none::NoneType)
    }

    // Text processing functions
    fn regex_match(pattern: String, text: String) -> anyhow::Result<bool> {
        match regex::Regex::new(&pattern) {
            Ok(regex) => Ok(regex.is_match(&text)),
            Err(_) => Ok(false),
        }
    }

    fn regex_replace(pattern: String, replacement: String, text: String) -> anyhow::Result<String> {
        let regex = regex::Regex::new(&pattern)?;
        Ok(regex.replace_all(&text, replacement.as_str()).into_owned())
    }

    fn regex_find_all<'v>(
        heap: &'v Heap,
        pattern: String,
        text: String,
    ) -> anyhow::Result<Value<'v>> {
        let regex = regex::Regex::new(&pattern)?;
        let matches: Vec<Value> = regex
            .find_iter(&text)
            .map(|m| heap.alloc(m.as_str().to_string()))
            .collect();
        Ok(heap.alloc(matches))
    }

    // Pattern extraction using precompiled regexes
    fn extract_pattern<'v>(
        heap: &'v Heap,
        pattern_name: String,
        text: String,
    ) -> anyhow::Result<Value<'v>> {
        if let Some((regex, _desc)) = BUILTIN_REGEXES.get(pattern_name.as_str()) {
            if let Some(m) = regex.find(&text) {
                Ok(heap.alloc(m.as_str().to_string()))
            } else {
                Ok(Value::new_none())
            }
        } else {
            Ok(Value::new_none())
        }
    }

    // JSON functions
    fn parse_json<'v>(heap: &'v Heap, text: String) -> anyhow::Result<Value<'v>> {
        let json_value: serde_json::Value = serde_json::from_str(&text)?;
        json_to_starlark_value(heap, json_value)
    }

    fn dump_json(value: Value) -> anyhow::Result<String> {
        let json_value = starlark_to_json_value(value)?;
        Ok(serde_json::to_string(&json_value)?)
    }

    // CSV functions
    fn parse_csv<'v>(
        heap: &'v Heap,
        line: String,
        headers: Option<Value>,
        sep: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        use starlark::collections::SmallMap;
        use starlark::values::{dict::Dict, list::ListRef};

        let separator = sep.unwrap_or_else(|| ",".to_string());
        let sep_char = separator.chars().next().unwrap_or(',');

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(sep_char as u8)
            .has_headers(false)
            .from_reader(line.as_bytes());

        if let Some(record) = reader.records().next() {
            let record = record?;
            let fields: Vec<String> = record.iter().map(|field| field.to_string()).collect();

            // Handle headers
            let column_names = if let Some(headers_val) = headers {
                // Extract headers from list value
                if let Some(list) = ListRef::from_value(headers_val) {
                    list.iter()
                        .map(|v| {
                            // Extract the actual string content, not the debug representation
                            if let Some(s) = v.unpack_str() {
                                s.to_string()
                            } else {
                                v.to_string() // Fallback to debug representation
                            }
                        })
                        .collect::<Vec<String>>()
                } else {
                    return Err(anyhow::anyhow!("headers must be a list"));
                }
            } else {
                // Auto-generate col1, col2, col3...
                (1..=fields.len()).map(|i| format!("col{}", i)).collect()
            };

            // Create dict from headers and fields
            let mut dict_map = SmallMap::new();

            for (i, header) in column_names.iter().enumerate() {
                let value = if i < fields.len() {
                    fields[i].clone()
                } else {
                    // More headers than fields - fill with empty string
                    String::new()
                };

                // Allocate key and value as strings directly
                let key = heap.alloc(header.as_str()); // Use as_str() to avoid extra quotes
                let val = heap.alloc(value);
                dict_map.insert_hashed(key.get_hashed().unwrap(), val);
            }

            // Note: if there are more fields than headers, we ignore the extra fields

            let dict = Dict::new(dict_map);
            Ok(heap.alloc(dict))
        } else {
            // Empty line - return empty dict
            let dict = Dict::new(SmallMap::new());
            Ok(heap.alloc(dict))
        }
    }

    fn dump_csv(values: Value, delimiter: Option<String>) -> anyhow::Result<String> {
        use starlark::values::list::ListRef;

        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let delim_char = delim.chars().next().unwrap_or(',');

        let list = ListRef::from_value(values)
            .ok_or_else(|| anyhow::anyhow!("Expected list for dump_csv"))?;

        let mut writer = csv::WriterBuilder::new()
            .delimiter(delim_char as u8)
            .has_headers(false)
            .quote_style(csv::QuoteStyle::Never)
            .from_writer(Vec::new());

        let fields: Vec<String> = list
            .iter()
            .map(|v| {
                let s = v.to_string();
                if s.starts_with('"') && s.ends_with('"') && s.len() > 1 {
                    s[1..s.len() - 1].to_string()
                } else {
                    s
                }
            })
            .collect();

        writer.write_record(&fields)?;
        let data = writer.into_inner()?;
        let result = String::from_utf8(data)?;
        Ok(result.trim_end().to_string())
    }

    // Key-value parsing
    fn parse_kv<'v>(
        heap: &'v Heap,
        line: String,
        sep: Option<String>,
        delim: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let separator = sep.unwrap_or_else(|| "=".to_string());
        let delimiter = delim.unwrap_or_else(|| " ".to_string());

        let mut items = Vec::new();
        for pair in line.split(&delimiter) {
            if let Some((key, value)) = pair.split_once(&separator) {
                let k = key.trim();
                let v = value.trim();
                items.push(format!("{}: {}", k, v));
            }
        }

        let dict_str = format!("{{{}}}", items.join(", "));
        Ok(heap.alloc(dict_str))
    }

    // Standard functions
    fn str<'v>(heap: &'v Heap, value: Value<'v>) -> anyhow::Result<Value<'v>> {
        Ok(heap.alloc(value.to_string()))
    }

    fn len<'v>(value: Value<'v>) -> anyhow::Result<i32> {
        use starlark::values::{dict::DictRef, list::ListRef};

        if let Some(s) = value.unpack_str() {
            Ok(s.len() as i32)
        } else if let Some(list) = ListRef::from_value(value) {
            Ok(list.len() as i32)
        } else if let Some(dict) = DictRef::from_value(value) {
            Ok(dict.len() as i32)
        } else {
            Err(anyhow::anyhow!(
                "object of type '{}' has no len()",
                value.get_type()
            ))
        }
    }

    // Timestamp functions
    fn parse_ts(text: String, format: Option<String>) -> anyhow::Result<i64> {
        if let Some(fmt) = format {
            // Parse with custom format
            let dt = NaiveDateTime::parse_from_str(&text, &fmt).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to parse timestamp '{}' with format '{}': {}",
                    text,
                    fmt,
                    e
                )
            })?;
            Ok(dt.and_utc().timestamp())
        } else {
            // Auto-detect common formats
            // Try RFC3339/ISO 8601 first
            if let Ok(dt) = DateTime::parse_from_rfc3339(&text) {
                return Ok(dt.timestamp());
            }

            // Try ISO 8601 without timezone (assume UTC)
            if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S") {
                return Ok(dt.and_utc().timestamp());
            }

            // Try common log format
            if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S") {
                return Ok(dt.and_utc().timestamp());
            }

            // Try date only
            if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d") {
                return Ok(dt.and_utc().timestamp());
            }

            Err(anyhow::anyhow!(
                "Failed to parse timestamp '{}' - unsupported format",
                text
            ))
        }
    }

    fn format_ts<'v>(
        heap: &'v Heap,
        timestamp: i64,
        format: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let dt = Utc
            .timestamp_opt(timestamp, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp: {}", timestamp))?;

        let formatted = if let Some(fmt) = format {
            dt.format(&fmt).to_string()
        } else {
            // Default to ISO 8601
            dt.to_rfc3339()
        };

        Ok(heap.alloc(formatted))
    }

    fn now() -> anyhow::Result<i64> {
        Ok(Utc::now().timestamp())
    }

    fn ts_diff(ts1: i64, ts2: i64) -> anyhow::Result<i64> {
        Ok(ts1 - ts2)
    }

    fn ts_add(timestamp: i64, seconds: i64) -> anyhow::Result<i64> {
        Ok(timestamp + seconds)
    }

    fn guess_ts(text: String) -> anyhow::Result<i64> {
        // Try dateparser first - handles most common formats automatically
        match dateparser::parse(&text) {
            Ok(dt) => Ok(dt.timestamp()),
            Err(_) => {
                // Fallback to our manual format attempts for edge cases
                // Try RFC3339/ISO 8601 first
                if let Ok(dt) = DateTime::parse_from_rfc3339(&text) {
                    return Ok(dt.timestamp());
                }

                // Try ISO 8601 without timezone (assume UTC)
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try common log format
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try date only
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try Apache/Nginx log format (e.g., "25/Dec/2021:10:24:56 +0000")
                if let Ok(dt) = DateTime::parse_from_str(&text, "%d/%b/%Y:%H:%M:%S %z") {
                    return Ok(dt.timestamp());
                }

                // Try syslog format (e.g., "Dec 25 10:24:56")
                // Note: This assumes current year since syslog doesn't include year
                let current_year = Utc::now().year();
                let text_with_year = format!("{} {}", current_year, text);
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text_with_year, "%Y %b %d %H:%M:%S")
                {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try compact format YYYYMMDDTHHMMSS (e.g., "20030925T104941")
                if text.len() == 15 && text.chars().nth(8) == Some('T') {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y%m%dT%H%M%S") {
                        return Ok(dt.and_utc().timestamp());
                    }
                }

                // Try compact format YYYYMMDDHHMM (e.g., "199709020900")
                if text.len() == 12 && text.chars().all(|c| c.is_ascii_digit()) {
                    if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y%m%d%H%M") {
                        return Ok(dt.and_utc().timestamp());
                    }
                }

                // Try German format DD.MM.YYYY HH:MM:SS (e.g., "27.01.2025 14:30:45")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%d.%m.%Y %H:%M:%S") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try German date only DD.MM.YYYY (e.g., "27.01.2025")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%d.%m.%Y") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try BGL format YYYY-MM-DD-HH.MM.SS.ffffff (e.g., "2025-01-27-14.30.45.123456")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%Y-%m-%d-%H.%M.%S.%f") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try DD-MM-YYYY format (e.g., "27-01-2025")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%d-%m-%Y") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try DD-MM-YYYY HH:MM:SS format (e.g., "27-01-2025 14:30:45")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%d-%m-%Y %H:%M:%S") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try Spark format YY/MM/DD HH:MM:SS (e.g., "25/01/27 14:30:45")
                if let Ok(dt) = NaiveDateTime::parse_from_str(&text, "%y/%m/%d %H:%M:%S") {
                    return Ok(dt.and_utc().timestamp());
                }

                // Try Apache bracket format [Day Mon DD HH:MM:SS YYYY] (e.g., "[Mon Jan 27 14:30:45 2025]")
                if text.starts_with('[') && text.ends_with(']') {
                    let inner = &text[1..text.len() - 1];
                    if let Ok(dt) = NaiveDateTime::parse_from_str(inner, "%a %b %d %H:%M:%S %Y") {
                        return Ok(dt.and_utc().timestamp());
                    }
                }

                // Try Zookeeper format with comma separator (e.g., "2025-01-27 14:30:45,123")
                if text.contains(',') {
                    let comma_replaced = text.replace(',', ".");
                    if let Ok(dt) =
                        NaiveDateTime::parse_from_str(&comma_replaced, "%Y-%m-%d %H:%M:%S.%f")
                    {
                        return Ok(dt.and_utc().timestamp());
                    }
                }

                // Try nanosecond precision handling (truncate to microseconds)
                // Handle formats like "2024-01-15T10:30:45.123456789Z" or "2024-01-15T10:30:45.123456789+01:00"
                if text.contains('.')
                    && (text.ends_with('Z') || text.contains('+') || text.contains('-'))
                {
                    // Find the fractional seconds part
                    if let Some(dot_pos) = text.rfind('.') {
                        let before_dot = &text[..dot_pos];
                        let after_dot = &text[dot_pos + 1..];

                        // Find where timezone info starts
                        let mut tz_start = after_dot.len();
                        for (i, c) in after_dot.chars().enumerate() {
                            if c == 'Z' || c == '+' || c == '-' {
                                tz_start = i;
                                break;
                            }
                        }

                        let fractional = &after_dot[..tz_start];
                        let tz_part = &after_dot[tz_start..];

                        // Truncate fractional seconds to 6 digits (microseconds)
                        let truncated_fractional = if fractional.len() > 6 {
                            &fractional[..6]
                        } else {
                            fractional
                        };

                        let reconstructed =
                            format!("{}.{}{}", before_dot, truncated_fractional, tz_part);

                        // Try parsing the reconstructed timestamp
                        if let Ok(dt) = DateTime::parse_from_rfc3339(&reconstructed) {
                            return Ok(dt.timestamp());
                        }

                        // Try without timezone (assume UTC)
                        if tz_part == "Z" || tz_part.is_empty() {
                            let utc_format = format!("{}.{}", before_dot, truncated_fractional);
                            if let Ok(dt) =
                                NaiveDateTime::parse_from_str(&utc_format, "%Y-%m-%dT%H:%M:%S.%f")
                            {
                                return Ok(dt.and_utc().timestamp());
                            }
                        }
                    }
                }

                Err(anyhow::anyhow!(
                    "Failed to parse timestamp '{}' - no recognized format",
                    text
                ))
            }
        }
    }

    fn parse_duration(duration: String) -> anyhow::Result<f64> {
        parse_duration_hybrid(&duration)
    }

    // Window functions
    fn window_values<'v>(heap: &'v Heap, field_name: String) -> anyhow::Result<Value<'v>> {
        WINDOW_CONTEXT.with(|ctx| {
            if let Some(window_buffer) = ctx.borrow().as_ref() {
                let values: Vec<Value> = window_buffer
                    .iter()
                    .filter_map(|record| {
                        // Extract field from either line or structured data
                        match (&record.line, &record.data) {
                            // For text records, only support "line" field
                            (Some(line), None) if field_name == "line" => {
                                Some(heap.alloc(line.clone()))
                            }
                            // For structured records, extract named field
                            (None, Some(data)) => data
                                .get(&field_name)
                                .and_then(|v| v.as_str())
                                .map(|s| heap.alloc(s.to_string())),
                            _ => None,
                        }
                    })
                    .collect();
                Ok(heap.alloc(values))
            } else {
                Ok(heap.alloc(Vec::<Value>::new()))
            }
        })
    }

    fn window_numbers<'v>(heap: &'v Heap, field_name: String) -> anyhow::Result<Value<'v>> {
        WINDOW_CONTEXT.with(|ctx| {
            if let Some(window_buffer) = ctx.borrow().as_ref() {
                let values: Vec<Value> = window_buffer
                    .iter()
                    .filter_map(|record| {
                        match (&record.line, &record.data) {
                            // For structured records, extract and convert to number
                            (None, Some(data)) => data
                                .get(&field_name)
                                .and_then(|v| v.as_f64())
                                .map(|f| heap.alloc(f)),
                            // For text records, try to parse as number
                            (Some(line), None) if field_name == "line" => {
                                line.parse::<f64>().ok().map(|f| heap.alloc(f))
                            }
                            _ => None,
                        }
                    })
                    .collect();
                Ok(heap.alloc(values))
            } else {
                Ok(heap.alloc(Vec::<Value>::new()))
            }
        })
    }

    fn window_size() -> anyhow::Result<i32> {
        WINDOW_CONTEXT.with(|ctx| {
            if let Some(window_buffer) = ctx.borrow().as_ref() {
                Ok(window_buffer.len() as i32)
            } else {
                Ok(0)
            }
        })
    }

    // Column extraction function
    fn cols<'v>(
        heap: &'v Heap,
        text: String,
        #[starlark(args)] args: UnpackTuple<Value<'v>>,
        sep: Option<String>,
        outsep: Option<String>,
    ) -> anyhow::Result<Value<'v>> {
        let separator = sep.as_deref().unwrap_or_default(); // Empty string = whitespace split
        let output_sep = outsep.as_deref().unwrap_or(" ");

        // Split input text
        let columns: Vec<String> = if separator.is_empty() {
            text.split_whitespace().map(|s| s.to_string()).collect()
        } else {
            text.split(separator).map(|s| s.to_string()).collect()
        };

        // Parse column specs from arguments
        let mut results = Vec::new();
        for arg in args.items {
            let spec_str = if let Some(s) = arg.unpack_str() {
                s.to_string()
            } else if let Some(i) = arg.unpack_i32() {
                i.to_string()
            } else {
                return Err(anyhow::anyhow!("Column spec must be string or integer"));
            };

            let spec = parse_column_spec(&spec_str)?;
            let result = extract_columns(&columns, &spec, output_sep);
            results.push(result);
        }

        // Return single value or list
        if results.len() == 1 {
            Ok(heap.alloc(results.into_iter().next().unwrap()))
        } else {
            let starlark_list: Vec<Value> = results.into_iter().map(|s| heap.alloc(s)).collect();
            Ok(heap.alloc(starlark_list))
        }
    }

    /// Navigate nested data structures using dot notation with array indexing
    fn get_path<'v>(
        pathspec: &str,
        data: Value<'v>,
        default: Option<Value<'v>>,
    ) -> anyhow::Result<Value<'v>> {
        use starlark::values::{dict::DictRef, list::ListRef};
        
        if pathspec.is_empty() {
            return Ok(data);
        }

        let segments: Vec<&str> = pathspec.split('.').collect();
        let mut current = data;
        let default_value = default.unwrap_or(Value::new_none());

        for segment in segments {
            current = if let Some(dict) = DictRef::from_value(current) {
                // Dictionary access
                // Use iteration to find the key (since dict.get() may not have the API we expect)
                let mut found = None;
                for (k, v) in dict.iter() {
                    if let Some(key_str) = k.unpack_str() {
                        if key_str == segment {
                            found = Some(v);
                            break;
                        }
                    }
                }
                match found {
                    Some(value) => value,
                    None => return Ok(default_value),
                }
            } else if let Some(list) = ListRef::from_value(current) {
                // Array access - parse segment as index
                let index = segment.parse::<i32>().map_err(|_| {
                    anyhow::anyhow!("Invalid array index '{}' - must be a number", segment)
                })?;
                
                if index < 0 || index as usize >= list.len() {
                    return Ok(default_value);
                }
                
                // Use iterator to access the element
                match list.iter().nth(index as usize) {
                    Some(value) => value,
                    None => return Ok(default_value),
                }
            } else {
                // Can't navigate further into non-dict/non-list
                return Ok(default_value);
            };
        }

        Ok(current)
    }

    // Statistical functions
    fn avg<'v>(heap: &'v Heap, sequence: Value<'v>) -> anyhow::Result<Value<'v>> {
        use starlark::values::list::ListRef;
        
        let list = ListRef::from_value(sequence)
            .ok_or_else(|| anyhow::anyhow!("avg() argument must be a list"))?;
        
        if list.is_empty() {
            return Err(anyhow::anyhow!("avg() of empty sequence"));
        }
        
        let mut sum = 0.0;
        let mut count = 0;
        
        for item in list.iter() {
            // Try to convert to number using available methods
            let num_val = if let Some(i) = item.unpack_i32() {
                i as f64
            } else if let Some(s) = item.unpack_str() {
                s.parse::<f64>().map_err(|_| anyhow::anyhow!("avg() requires numeric values, got non-numeric string: {}", s))?
            } else {
                // Try to convert via string representation
                item.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("avg() requires numeric values, got: {}", item.get_type()))?
            };
            
            sum += num_val;
            count += 1;
        }
        
        Ok(heap.alloc(sum / count as f64))
    }

    fn sum<'v>(heap: &'v Heap, sequence: Value<'v>) -> anyhow::Result<Value<'v>> {
        use starlark::values::list::ListRef;
        
        let list = ListRef::from_value(sequence)
            .ok_or_else(|| anyhow::anyhow!("sum() argument must be a list"))?;
        
        if list.is_empty() {
            return Ok(heap.alloc(0.0));
        }
        
        let mut sum = 0.0;
        
        for item in list.iter() {
            // Try to convert to number using available methods
            let num_val = if let Some(i) = item.unpack_i32() {
                i as f64
            } else if let Some(s) = item.unpack_str() {
                s.parse::<f64>().map_err(|_| anyhow::anyhow!("sum() requires numeric values, got non-numeric string: {}", s))?
            } else {
                // Try to convert via string representation
                item.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("sum() requires numeric values, got: {}", item.get_type()))?
            };
            
            sum += num_val;
        }
        
        Ok(heap.alloc(sum))
    }

    fn percentile<'v>(heap: &'v Heap, sequence: Value<'v>, p: Value<'v>) -> anyhow::Result<Value<'v>> {
        use starlark::values::list::ListRef;
        
        let list = ListRef::from_value(sequence)
            .ok_or_else(|| anyhow::anyhow!("percentile() first argument must be a list"))?;
        
        if list.is_empty() {
            return Err(anyhow::anyhow!("percentile() of empty sequence"));
        }
        
        // Extract percentile value
        let p_val = if let Some(i) = p.unpack_i32() {
            i as f64
        } else if let Some(s) = p.unpack_str() {
            s.parse::<f64>().map_err(|_| anyhow::anyhow!("percentile value must be numeric, got non-numeric string: {}", s))?
        } else {
            // Try to convert via string representation
            p.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("percentile value must be numeric, got: {}", p.get_type()))?
        };
        
        if !(0.0..=100.0).contains(&p_val) {
            return Err(anyhow::anyhow!("percentile must be between 0 and 100, got: {}", p_val));
        }
        
        // Extract and sort numeric values
        let mut values: Vec<f64> = Vec::new();
        for item in list.iter() {
            // Try to convert to number using available methods
            let num_val = if let Some(i) = item.unpack_i32() {
                i as f64
            } else if let Some(s) = item.unpack_str() {
                s.parse::<f64>().map_err(|_| anyhow::anyhow!("percentile() requires numeric values, got non-numeric string: {}", s))?
            } else {
                // Try to convert via string representation
                item.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("percentile() requires numeric values, got: {}", item.get_type()))?
            };
            
            values.push(num_val);
        }
        
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        // Calculate percentile using linear interpolation method
        let n = values.len();
        let index = (p_val / 100.0) * (n - 1) as f64;
        
        if index.fract() == 0.0 {
            // Exact index
            let idx = index as usize;
            Ok(heap.alloc(values[idx]))
        } else {
            // Interpolate between two values
            let lower_idx = index.floor() as usize;
            let upper_idx = index.ceil() as usize;
            let fraction = index.fract();
            
            let lower_val = values[lower_idx];
            let upper_val = values[upper_idx];
            let result = lower_val + fraction * (upper_val - lower_val);
            
            Ok(heap.alloc(result))
        }
    }

    fn round<'v>(heap: &'v Heap, number: Value<'v>, digits: Option<Value<'v>>) -> anyhow::Result<Value<'v>> {
        // Extract number value
        let num_val = if let Some(i) = number.unpack_i32() {
            i as f64
        } else if let Some(s) = number.unpack_str() {
            s.parse::<f64>().map_err(|_| anyhow::anyhow!("round() first argument must be numeric, got non-numeric string: {}", s))?
        } else {
            // Try to convert via string representation
            number.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("round() first argument must be numeric, got: {}", number.get_type()))?
        };

        // Extract digits (default to 0 if not provided)
        let digits_val = if let Some(d) = digits {
            if let Some(i) = d.unpack_i32() {
                i
            } else if let Some(s) = d.unpack_str() {
                s.parse::<i32>().map_err(|_| anyhow::anyhow!("round() digits argument must be an integer, got non-numeric string: {}", s))?
            } else {
                return Err(anyhow::anyhow!("round() digits argument must be an integer, got: {}", d.get_type()));
            }
        } else {
            0
        };

        // Perform rounding
        let multiplier = 10_f64.powi(digits_val);
        let result = (num_val * multiplier).round() / multiplier;
        
        Ok(heap.alloc(result))
    }

    fn pow<'v>(heap: &'v Heap, base: Value<'v>, exponent: Value<'v>) -> anyhow::Result<Value<'v>> {
        // Extract base value
        let base_val = if let Some(i) = base.unpack_i32() {
            i as f64
        } else if let Some(s) = base.unpack_str() {
            s.parse::<f64>().map_err(|_| anyhow::anyhow!("pow() base must be numeric, got non-numeric string: {}", s))?
        } else {
            // Try to convert via string representation
            base.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("pow() base must be numeric, got: {}", base.get_type()))?
        };

        // Extract exponent value
        let exp_val = if let Some(i) = exponent.unpack_i32() {
            i as f64
        } else if let Some(s) = exponent.unpack_str() {
            s.parse::<f64>().map_err(|_| anyhow::anyhow!("pow() exponent must be numeric, got non-numeric string: {}", s))?
        } else {
            // Try to convert via string representation
            exponent.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("pow() exponent must be numeric, got: {}", exponent.get_type()))?
        };

        // Calculate power
        let result = base_val.powf(exp_val);
        
        Ok(heap.alloc(result))
    }

    fn abs<'v>(heap: &'v Heap, number: Value<'v>) -> anyhow::Result<Value<'v>> {
        // Extract number value
        let num_val = if let Some(i) = number.unpack_i32() {
            i as f64
        } else if let Some(s) = number.unpack_str() {
            s.parse::<f64>().map_err(|_| anyhow::anyhow!("abs() argument must be numeric, got non-numeric string: {}", s))?
        } else {
            // Try to convert via string representation
            number.to_string().parse::<f64>().map_err(|_| anyhow::anyhow!("abs() argument must be numeric, got: {}", number.get_type()))?
        };

        // Calculate absolute value
        let result = num_val.abs();
        
        Ok(heap.alloc(result))
    }
}

// Helper function for duration parsing with hybrid approach
fn parse_duration_hybrid(duration: &str) -> anyhow::Result<f64> {
    // Try our compact implementation first (supports fractional numbers and compact format)
    let pattern = regex::Regex::new(r"([-\d.]+)([a-z]+)").unwrap();
    let matches: Vec<_> = pattern.find_iter(duration).collect();
    
    if !matches.is_empty() {
        let mut total_seconds = 0.0;
        let mut all_valid = true;
        
        for m in matches {
            let full_match = m.as_str();
            if let Some(caps) = pattern.captures(full_match) {
                let value_str = caps.get(1).unwrap().as_str();
                let unit = caps.get(2).unwrap().as_str();
                
                if let Ok(value) = value_str.parse::<f64>() {
                    if value < 0.0 {
                        return Err(anyhow::anyhow!("Durations cannot be negative: {}", duration));
                    }
                    
                    let seconds = match unit {
                        "us" => value / 1_000_000.0,
                        "ms" => value / 1_000.0,
                        "s" => value,
                        "m" => value * 60.0,
                        "h" => value * 3_600.0,
                        "d" => value * 86_400.0,
                        "w" => value * 604_800.0,
                        _ => {
                            all_valid = false;
                            break;
                        }
                    };
                    
                    total_seconds += seconds;
                } else {
                    all_valid = false;
                    break;
                }
            } else {
                all_valid = false;
                break;
            }
        }
        
        if all_valid {
            return Ok(total_seconds);
        }
    }
    
    // If our implementation fails, try humantime as fallback
    // (supports spaces, full words, years, months)
    match humantime::parse_duration(duration) {
        Ok(std_duration) => Ok(std_duration.as_secs_f64()),
        Err(_) => Err(anyhow::anyhow!(
            "Invalid duration format: '{}'. Supported formats include '5d', '3h30m', '2.5s', '2h 30m', '1 hour 30 minutes'", 
            duration
        )),
    }
}

// Helper functions for JSON conversion
fn json_to_starlark_value(heap: &Heap, json: serde_json::Value) -> anyhow::Result<Value<'_>> {
    match json {
        serde_json::Value::Null => Ok(Value::new_none()),
        serde_json::Value::Bool(b) => Ok(Value::new_bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(heap.alloc(i as i32))
            } else if let Some(f) = n.as_f64() {
                Ok(heap.alloc(f))
            } else {
                Ok(heap.alloc(n.to_string()))
            }
        }
        serde_json::Value::String(s) => Ok(heap.alloc(s)),
        serde_json::Value::Array(arr) => {
            let values: Result<Vec<Value>, _> = arr
                .into_iter()
                .map(|v| json_to_starlark_value(heap, v))
                .collect();
            Ok(heap.alloc(values?))
        }
        serde_json::Value::Object(obj) => {
            use starlark::collections::SmallMap;
            use starlark::values::dict::Dict;

            let mut dict_map = SmallMap::new();
            for (k, v) in obj {
                let key = heap.alloc(k.as_str());
                let val = json_to_starlark_value(heap, v)?;
                dict_map.insert_hashed(key.get_hashed().unwrap(), val);
            }
            let dict = Dict::new(dict_map);
            Ok(heap.alloc(dict))
        }
    }
}

fn starlark_to_json_value(value: Value) -> anyhow::Result<serde_json::Value> {
    use starlark::values::{dict::DictRef, list::ListRef};

    if value.is_none() {
        Ok(serde_json::Value::Null)
    } else if let Some(b) = value.unpack_bool() {
        Ok(serde_json::Value::Bool(b))
    } else if let Some(i) = value.unpack_i32() {
        Ok(serde_json::Value::Number(serde_json::Number::from(i)))
    } else if let Some(s) = value.unpack_str() {
        Ok(serde_json::Value::String(s.to_string()))
    } else if let Some(list) = ListRef::from_value(value) {
        let arr: Result<Vec<serde_json::Value>, _> =
            list.iter().map(starlark_to_json_value).collect();
        Ok(serde_json::Value::Array(arr?))
    } else if let Some(dict) = DictRef::from_value(value) {
        let mut obj = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = if let Some(s) = k.unpack_str() {
                s.to_string()
            } else {
                k.to_string()
            };
            obj.insert(key, starlark_to_json_value(v)?);
        }
        Ok(serde_json::Value::Object(obj))
    } else {
        Ok(serde_json::Value::String(value.to_string()))
    }
}

// Column specification parsing and extraction for cols() function
#[derive(Debug, PartialEq)]
enum ColumnSpec {
    Index(i32),                      // 0, 1, -1, -2
    MultiIndex(Vec<i32>),            // "0,2,4"
    Slice(Option<i32>, Option<i32>), // "1:3", "2:", ":5"
}

fn parse_column_spec(spec: &str) -> anyhow::Result<ColumnSpec> {
    if spec.contains(':') {
        // Slice notation: "1:3", "2:", ":5"
        let parts: Vec<&str> = spec.split(':').collect();
        if parts.len() > 2 {
            return Err(anyhow::anyhow!("Invalid slice format: {}", spec));
        }

        let start = if parts[0].is_empty() {
            None
        } else {
            Some(parts[0].parse::<i32>()?)
        };

        let end = if parts.len() > 1 && !parts[1].is_empty() {
            Some(parts[1].parse::<i32>()?)
        } else {
            None
        };

        Ok(ColumnSpec::Slice(start, end))
    } else if spec.contains(',') {
        // Multiple indices: "0,2,4"
        let indices: Result<Vec<i32>, _> =
            spec.split(',').map(|s| s.trim().parse::<i32>()).collect();
        Ok(ColumnSpec::MultiIndex(indices?))
    } else {
        // Single index: "0", "-1"
        Ok(ColumnSpec::Index(spec.parse::<i32>()?))
    }
}

fn extract_columns(columns: &[String], spec: &ColumnSpec, outsep: &str) -> String {
    match spec {
        ColumnSpec::Index(idx) => {
            let index = if *idx < 0 {
                columns.len() as i32 + idx
            } else {
                *idx
            };

            if index >= 0 && (index as usize) < columns.len() {
                columns[index as usize].clone()
            } else {
                String::new() // Out of bounds returns empty string
            }
        }
        ColumnSpec::MultiIndex(indices) => {
            indices
                .iter()
                .filter_map(|&idx| {
                    let index = if idx < 0 {
                        columns.len() as i32 + idx
                    } else {
                        idx
                    };

                    if index >= 0 && (index as usize) < columns.len() {
                        Some(columns[index as usize].clone())
                    } else {
                        None // Skip out of bounds indices
                    }
                })
                .collect::<Vec<_>>()
                .join(outsep)
        }
        ColumnSpec::Slice(start, end) => {
            let len = columns.len() as i32;

            // Handle negative indices and defaults
            let start_idx = match start {
                Some(s) if *s < 0 => (len + s).max(0) as usize,
                Some(s) => (*s).max(0) as usize,
                None => 0,
            };

            let end_idx = match end {
                Some(e) if *e < 0 => (len + e).max(0) as usize,
                Some(e) => (*e).min(len) as usize,
                None => len as usize,
            };

            if start_idx < columns.len() && start_idx < end_idx {
                columns[start_idx..end_idx.min(columns.len())].join(outsep)
            } else {
                String::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starlark::environment::{GlobalsBuilder, Module};
    use starlark::eval::Evaluator;
    use starlark::syntax::{AstModule, Dialect};
    use starlark::values::dict::DictRef;

    // Helper to run a test and extract values immediately
    fn test_parse_csv_script(script: &str) -> Result<(usize, Vec<(String, String)>), String> {
        // Create globals with our functions
        let globals = GlobalsBuilder::new().with(global_functions).build();

        // Create module and evaluator
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Parse and execute the script
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended)
            .map_err(|e| format!("Parse error: {}", e))?;
        let result = eval
            .eval_module(ast, &globals)
            .map_err(|e| format!("Eval error: {}", e))?;

        // Extract the dict data immediately while the module is still alive
        let dict = DictRef::from_value(result).ok_or_else(|| "Result is not a dict".to_string())?;

        let len = dict.len();
        let mut entries = Vec::new();

        for (k, v) in dict.iter() {
            let key = k
                .unpack_str()
                .ok_or_else(|| "Key is not a string".to_string())?
                .to_string();
            let value = v
                .unpack_str()
                .ok_or_else(|| "Value is not a string".to_string())?
                .to_string();
            entries.push((key, value));
        }

        entries.sort(); // Sort for consistent testing
        Ok((len, entries))
    }

    #[test]
    fn test_parse_csv_auto_headers() {
        let script = r#"parse_csv("alice,25,engineer")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("col1".to_string(), "alice".to_string()),
                ("col2".to_string(), "25".to_string()),
                ("col3".to_string(), "engineer".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_with_headers() {
        let script = r#"parse_csv("alice,25,engineer", headers=["name", "age", "job"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("job".to_string(), "engineer".to_string()),
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_more_headers_than_fields() {
        let script = r#"parse_csv("alice,25", headers=["name", "age", "job"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("job".to_string(), "".to_string()), // Empty string
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_more_fields_than_headers() {
        let script = r#"parse_csv("alice,25,engineer,bonus", headers=["name", "age"])"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 2); // Only 2 headers, extra fields ignored
        assert_eq!(
            entries,
            vec![
                ("age".to_string(), "25".to_string()),
                ("name".to_string(), "alice".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_custom_delimiter() {
        let script = r#"parse_csv("alice|25|engineer", sep="|")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 3);
        assert_eq!(
            entries,
            vec![
                ("col1".to_string(), "alice".to_string()),
                ("col2".to_string(), "25".to_string()),
                ("col3".to_string(), "engineer".to_string()),
            ]
        );
    }

    #[test]
    fn test_parse_csv_empty_line() {
        let script = r#"parse_csv("")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 0); // Empty dict for empty line
        assert_eq!(entries, vec![]);
    }

    #[test]
    fn test_parse_csv_single_field() {
        let script = r#"parse_csv("alice")"#;
        let (len, entries) = test_parse_csv_script(script).unwrap();

        assert_eq!(len, 1);
        assert_eq!(entries, vec![("col1".to_string(), "alice".to_string()),]);
    }

    // Timestamp function tests
    #[test]
    fn test_parse_ts_rfc3339() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"parse_ts("2015-03-26T01:27:38-04:00")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Expected timestamp for 2015-03-26T01:27:38-04:00 (UTC)
        let expected = 1427347658i64;
        assert_eq!(result.unpack_i32().unwrap() as i64, expected);
    }

    #[test]
    fn test_parse_ts_iso_no_tz() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"parse_ts("2024-01-15T10:30:45")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse as UTC
        let expected = 1705314645i64;
        assert_eq!(result.unpack_i32().unwrap() as i64, expected);
    }

    #[test]
    fn test_parse_ts_log_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"parse_ts("2024-01-15 10:30:45")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let expected = 1705314645i64;
        assert_eq!(result.unpack_i32().unwrap() as i64, expected);
    }

    #[test]
    fn test_format_ts_default() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"format_ts(1427347658)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "2015-03-26T05:27:38+00:00");
    }

    #[test]
    fn test_format_ts_custom() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"format_ts(1427347658, "%Y-%m-%d %H:%M:%S")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "2015-03-26 05:27:38");
    }

    #[test]
    fn test_ts_diff() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"ts_diff(1427354858, 1427354800)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_i32().unwrap(), 58);
    }

    #[test]
    fn test_ts_add() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"ts_add(1427354800, 58)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_i32().unwrap(), 1427354858);
    }

    #[test]
    fn test_now() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"now()"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Just check it's a reasonable timestamp (after 2020)
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1577836800); // 2020-01-01
    }

    // guess_ts() function tests
    #[test]
    fn test_guess_ts_unix_timestamp() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("1511648546")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_i32().unwrap() as i64, 1511648546);
    }

    #[test]
    fn test_guess_ts_rfc3339() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("2021-05-01T01:17:02.604456Z")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse successfully
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1619827022); // Around 2021-05-01
    }

    #[test]
    fn test_guess_ts_postgres_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("2019-11-29 08:08-08")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse successfully
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1574000000); // Around 2019-11-29
    }

    #[test]
    fn test_guess_ts_natural_language() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("May 25, 2021")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse successfully
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1621900000); // Around May 25, 2021
    }

    #[test]
    fn test_guess_ts_slash_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("4/8/2014 22:05")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse successfully
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1396900000); // Around April 2014
    }

    #[test]
    fn test_guess_ts_apache_log_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"guess_ts("25/Dec/2021:10:24:56 +0000")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        // Should parse successfully
        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1640420000); // Around Dec 25, 2021
    }

    #[test]
    fn test_guess_ts_fallback_formats() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test formats that might not be in dateparser but in our fallback
        let script = r#"guess_ts("2024-01-15")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1705276800); // Around Jan 15, 2024
    }

    #[test]
    fn test_guess_ts_compact_formats() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test compact YYYYMMDDTHHMMSS format
        let script = r#"guess_ts("20030925T104941")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1064000000); // Around Sept 25, 2003
    }

    #[test]
    fn test_guess_ts_german_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test German DD.MM.YYYY HH:MM:SS format
        let script = r#"guess_ts("27.01.2025 14:30:45")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1737900000); // Around Jan 27, 2025
    }

    #[test]
    fn test_guess_ts_bgl_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test BGL YYYY-MM-DD-HH.MM.SS.ffffff format
        let script = r#"guess_ts("2025-01-27-14.30.45.123456")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1737900000); // Around Jan 27, 2025
    }

    #[test]
    fn test_guess_ts_nanosecond_precision() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test nanosecond precision (should truncate to microseconds)
        let script = r#"guess_ts("2024-01-15T10:30:45.123456789Z")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert_eq!(timestamp, 1705314645); // Should parse correctly
    }

    #[test]
    fn test_guess_ts_zookeeper_comma_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test Zookeeper comma separator format
        let script = r#"guess_ts("2025-01-27 14:30:45,123")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1737900000); // Around Jan 27, 2025
    }

    #[test]
    fn test_guess_ts_apache_bracket_format() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test Apache bracket format
        let script = r#"guess_ts("[Mon Jan 27 14:30:45 2025]")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let timestamp = result.unpack_i32().unwrap() as i64;
        assert!(timestamp > 1737900000); // Around Jan 27, 2025
    }

    #[test]
    fn test_dump_json_simple_dict() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"dump_json({"name": "Alice", "age": 30})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let json_str = result.unpack_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(parsed["name"], "Alice");
        assert_eq!(parsed["age"], 30);
    }

    #[test]
    fn test_dump_json_roundtrip() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"original = {"name": "Bob", "items": [1, 2, 3], "count": 42}; json_str = dump_json(original); parsed_back = parse_json(json_str); dump_json(parsed_back)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let json_str = result.unpack_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(parsed["name"], "Bob");
        assert_eq!(parsed["items"].as_array().unwrap().len(), 3);
        assert_eq!(parsed["count"], 42);
    }

    #[test]
    fn test_dump_json_nested_structures() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script =
            r#"dump_json({"user": {"profile": {"name": "Charlie"}}, "scores": [{"test": 95}]})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        let json_str = result.unpack_str().unwrap();
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();

        assert_eq!(parsed["user"]["profile"]["name"], "Charlie");
        assert_eq!(parsed["scores"][0]["test"], 95);
    }

    // Pattern extraction tests
    #[test]
    fn test_extract_pattern_email() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("email", "Contact us at test@example.com for help")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "test@example.com");
    }

    #[test]
    fn test_extract_pattern_ipv4() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("ipv4", "Request from 192.168.1.100 failed")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "192.168.1.100");
    }

    #[test]
    fn test_extract_pattern_url() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("url", "Visit https://example.com/api/v1 for docs")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "https://example.com/api/v1");
    }

    #[test]
    fn test_extract_pattern_uuid() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script =
            r#"extract_pattern("uuid", "Session ID: 550e8400-e29b-41d4-a716-446655440000")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(
            result.unpack_str().unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_extract_pattern_no_match() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("email", "No email address here")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_extract_pattern_unknown_pattern() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("nonexistent", "some text")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_extract_pattern_gitcommit() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("gitcommit", "Fixed bug in commit a1b2c3d4e5f6789")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "a1b2c3d4e5f6789");
    }

    #[test]
    fn test_extract_pattern_version() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"extract_pattern("version", "Using stelp version 1.2.3-beta.1")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();

        assert_eq!(result.unpack_str().unwrap(), "1.2.3-beta.1");
    }

    // Column spec parsing tests
    #[test]
    fn test_parse_column_spec_single_index() {
        assert_eq!(parse_column_spec("0").unwrap(), ColumnSpec::Index(0));
        assert_eq!(parse_column_spec("5").unwrap(), ColumnSpec::Index(5));
        assert_eq!(parse_column_spec("-1").unwrap(), ColumnSpec::Index(-1));
        assert_eq!(parse_column_spec("-2").unwrap(), ColumnSpec::Index(-2));
    }

    #[test]
    fn test_parse_column_spec_multi_index() {
        assert_eq!(
            parse_column_spec("0,2").unwrap(),
            ColumnSpec::MultiIndex(vec![0, 2])
        );
        assert_eq!(
            parse_column_spec("1,3,5").unwrap(),
            ColumnSpec::MultiIndex(vec![1, 3, 5])
        );
        assert_eq!(
            parse_column_spec("-2,-1").unwrap(),
            ColumnSpec::MultiIndex(vec![-2, -1])
        );
        assert_eq!(
            parse_column_spec("0, 2, 4").unwrap(),
            ColumnSpec::MultiIndex(vec![0, 2, 4])
        );
    }

    #[test]
    fn test_parse_column_spec_slice() {
        assert_eq!(
            parse_column_spec("1:3").unwrap(),
            ColumnSpec::Slice(Some(1), Some(3))
        );
        assert_eq!(
            parse_column_spec("2:").unwrap(),
            ColumnSpec::Slice(Some(2), None)
        );
        assert_eq!(
            parse_column_spec(":3").unwrap(),
            ColumnSpec::Slice(None, Some(3))
        );
        assert_eq!(
            parse_column_spec(":").unwrap(),
            ColumnSpec::Slice(None, None)
        );
        assert_eq!(
            parse_column_spec("1:-1").unwrap(),
            ColumnSpec::Slice(Some(1), Some(-1))
        );
    }

    #[test]
    fn test_parse_column_spec_invalid() {
        assert!(parse_column_spec("1:2:3").is_err()); // Too many colons
        assert!(parse_column_spec("abc").is_err()); // Non-numeric
        assert!(parse_column_spec("1,abc").is_err()); // Mixed valid/invalid
    }

    // Column extraction tests
    #[test]
    fn test_extract_columns_single_index() {
        let columns = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(0), " "), "a");
        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(2), " "), "c");
        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(-1), " "), "d");
        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(-2), " "), "c");

        // Out of bounds
        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(10), " "), "");
        assert_eq!(extract_columns(&columns, &ColumnSpec::Index(-10), " "), "");
    }

    #[test]
    fn test_extract_columns_multi_index() {
        let columns = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];

        assert_eq!(
            extract_columns(&columns, &ColumnSpec::MultiIndex(vec![0, 2]), " "),
            "a c"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::MultiIndex(vec![1, 3]), ":"),
            "b:d"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::MultiIndex(vec![-2, -1]), " "),
            "c d"
        );

        // With some out of bounds (should skip them)
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::MultiIndex(vec![0, 10, 2]), " "),
            "a c"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::MultiIndex(vec![10, 20]), " "),
            ""
        );
    }

    #[test]
    fn test_extract_columns_slice() {
        let columns = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(Some(1), Some(3)), " "),
            "b c"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(Some(2), None), " "),
            "c d e"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(None, Some(3)), " "),
            "a b c"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(None, None), " "),
            "a b c d e"
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(Some(1), Some(-1)), " "),
            "b c d"
        );

        // Edge cases
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(Some(10), Some(20)), " "),
            ""
        );
        assert_eq!(
            extract_columns(&columns, &ColumnSpec::Slice(Some(3), Some(2)), " "),
            ""
        ); // start > end
    }

    // Integration tests for cols() function
    fn test_cols_script(script: &str) -> Result<String, String> {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended)
            .map_err(|e| format!("Parse error: {}", e))?;
        let result = eval
            .eval_module(ast, &globals)
            .map_err(|e| format!("Eval error: {}", e))?;

        if let Some(s) = result.unpack_str() {
            Ok(s.to_string())
        } else {
            Ok(result.to_string())
        }
    }

    fn test_cols_script_list(script: &str) -> Result<Vec<String>, String> {
        use starlark::values::list::ListRef;

        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended)
            .map_err(|e| format!("Parse error: {}", e))?;
        let result = eval
            .eval_module(ast, &globals)
            .map_err(|e| format!("Eval error: {}", e))?;

        if let Some(list) = ListRef::from_value(result) {
            Ok(list
                .iter()
                .map(|v| {
                    if let Some(s) = v.unpack_str() {
                        s.to_string()
                    } else {
                        v.to_string()
                    }
                })
                .collect())
        } else {
            Err("Result is not a list".to_string())
        }
    }

    #[test]
    fn test_cols_basic_usage() {
        // Single column access
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma", 0)"#).unwrap(),
            "alpha"
        );
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma", 1)"#).unwrap(),
            "beta"
        );
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma", -1)"#).unwrap(),
            "gamma"
        );
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma", -2)"#).unwrap(),
            "beta"
        );
    }

    #[test]
    fn test_cols_multiple_columns() {
        // Multiple columns should return a list
        let result = test_cols_script_list(r#"cols("alpha beta gamma", 0, 2)"#).unwrap();
        assert_eq!(result, vec!["alpha", "gamma"]);

        let result = test_cols_script_list(r#"cols("alpha beta gamma delta", 1, -1)"#).unwrap();
        assert_eq!(result, vec!["beta", "delta"]);
    }

    #[test]
    fn test_cols_multi_index_string() {
        // Multiple indices as string
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma delta", "0,2")"#).unwrap(),
            "alpha gamma"
        );
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma delta", "1,3")"#).unwrap(),
            "beta delta"
        );
        assert_eq!(
            test_cols_script(r#"cols("alpha beta gamma delta", "-2,-1")"#).unwrap(),
            "gamma delta"
        );
    }

    #[test]
    fn test_cols_slices() {
        // Slice notation
        assert_eq!(
            test_cols_script(r#"cols("a b c d e", "1:3")"#).unwrap(),
            "b c"
        );
        assert_eq!(
            test_cols_script(r#"cols("a b c d e", "2:")"#).unwrap(),
            "c d e"
        );
        assert_eq!(
            test_cols_script(r#"cols("a b c d e", ":3")"#).unwrap(),
            "a b c"
        );
        assert_eq!(
            test_cols_script(r#"cols("a b c d e", "1:-1")"#).unwrap(),
            "b c d"
        );
    }

    #[test]
    fn test_cols_custom_separators() {
        // Custom input separator
        assert_eq!(
            test_cols_script(r#"cols("alice,bob,charlie", 1, sep=",")"#).unwrap(),
            "bob"
        );
        assert_eq!(
            test_cols_script(r#"cols("alice|bob|charlie", 0, sep="|")"#).unwrap(),
            "alice"
        );

        // Custom output separator
        assert_eq!(
            test_cols_script(r#"cols("a b c d", "0,2", outsep=":")"#).unwrap(),
            "a:c"
        );
        assert_eq!(
            test_cols_script(r#"cols("a b c d", "1:3", outsep="-")"#).unwrap(),
            "b-c"
        );
    }

    #[test]
    fn test_cols_mixed_selectors() {
        // Mix of different selector types
        let result = test_cols_script_list(r#"cols("a b c d e f", 0, "2:4", -1)"#).unwrap();
        assert_eq!(result, vec!["a", "c d", "f"]);

        let result = test_cols_script_list(r#"cols("a b c d e f", "0,1", "3:")"#).unwrap();
        assert_eq!(result, vec!["a b", "d e f"]);
    }

    #[test]
    fn test_cols_edge_cases() {
        // Empty string
        assert_eq!(test_cols_script(r#"cols("", 0)"#).unwrap(), "");

        // Out of bounds
        assert_eq!(test_cols_script(r#"cols("a b c", 10)"#).unwrap(), "");
        assert_eq!(test_cols_script(r#"cols("a b c", -10)"#).unwrap(), "");

        // Single word
        assert_eq!(test_cols_script(r#"cols("hello", 0)"#).unwrap(), "hello");
        assert_eq!(test_cols_script(r#"cols("hello", 1)"#).unwrap(), "");
    }

    #[test]
    fn test_cols_http_request_example() {
        // Real-world example: HTTP request parsing
        let result = test_cols_script_list(r#"cols("GET /api/users HTTP/1.1", 0, 1, 2)"#).unwrap();
        assert_eq!(result, vec!["GET", "/api/users", "HTTP/1.1"]);
    }

    #[test]
    fn test_cols_csv_example() {
        // CSV parsing with custom separator
        let result =
            test_cols_script_list(r#"cols("alice,25,engineer,remote", 0, 1, 2, sep=",")"#).unwrap();
        assert_eq!(result, vec!["alice", "25", "engineer"]);
    }

    #[test]
    fn test_cols_mixed_selector_types() {
        // Mix integers, comma-separated strings, and slices
        let result =
            test_cols_script_list(r#"cols("a b c d e f g h i j", 0, "1,3", "5:8", -1)"#).unwrap();
        assert_eq!(result, vec!["a", "b d", "f g h", "j"]);

        // Another complex mix
        let result = test_cols_script_list(
            r#"cols("alpha beta gamma delta epsilon zeta eta theta", 0, "2,4", "6:", -2)"#,
        )
        .unwrap();
        assert_eq!(result, vec!["alpha", "gamma epsilon", "eta theta", "eta"]);

        // With custom separators
        let result = test_cols_script_list(
            r#"cols("a,b,c,d,e,f,g", 0, "1,3", "4:6", -1, sep=",", outsep=":")"#,
        )
        .unwrap();
        assert_eq!(result, vec!["a", "b:d", "e:f", "g"]);
    }

    #[test]
    fn test_cols_error_cases() {
        // Invalid column specs should return errors
        assert!(test_cols_script(r#"cols("a b c", "invalid")"#).is_err());
        assert!(test_cols_script(r#"cols("a b c", "1:2:3")"#).is_err());
        assert!(test_cols_script(r#"cols("a b c", 1.5)"#).is_err()); // Float not allowed
    }

    // parse_duration() function tests - hybrid approach
    #[test]
    fn test_parse_duration_single_units() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test microseconds
        let script = r#"parse_duration("500us")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 0.0005).abs() < f64::EPSILON);

        // Test milliseconds
        let script = r#"parse_duration("250ms")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 0.25).abs() < f64::EPSILON);

        // Test seconds
        let script = r#"parse_duration("5s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 5.0).abs() < f64::EPSILON);

        // Test minutes
        let script = r#"parse_duration("3m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 180.0).abs() < f64::EPSILON);

        // Test hours
        let script = r#"parse_duration("2h")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 7200.0).abs() < f64::EPSILON);

        // Test days
        let script = r#"parse_duration("1d")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 86400.0).abs() < f64::EPSILON);

        // Test weeks
        let script = r#"parse_duration("1w")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 604800.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_duration_combined_units() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test 3h30m (3.5 hours = 12600 seconds)
        let script = r#"parse_duration("3h30m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 12600.0).abs() < f64::EPSILON);

        // Test 5d12h (5.5 days = 475200 seconds)
        let script = r#"parse_duration("5d12h")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 475200.0).abs() < f64::EPSILON);

        // Test 2h30m45s (9045 seconds)
        let script = r#"parse_duration("2h30m45s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 9045.0).abs() < f64::EPSILON);

        // Test 1d2h3m4s5ms (93784.005 seconds)
        let script = r#"parse_duration("1d2h3m4s5ms")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 93784.005).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_duration_fractional_values() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test 2.5s
        let script = r#"parse_duration("2.5s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 2.5).abs() < f64::EPSILON);

        // Test 1.5h (5400 seconds)
        let script = r#"parse_duration("1.5h")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 5400.0).abs() < f64::EPSILON);

        // Test 0.5m (30 seconds)
        let script = r#"parse_duration("0.5m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 30.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_duration_error_cases() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test empty string
        let script = r#"parse_duration("")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        assert!(eval.eval_module(ast, &globals).is_err());

        // Test invalid format (no units)
        let script = r#"parse_duration("123")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        assert!(eval.eval_module(ast, &globals).is_err());

        // Test truly unsupported unit (both parsers fail)
        let script = r#"parse_duration("5z")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        assert!(eval.eval_module(ast, &globals).is_err());

        // Test invalid numeric value
        let script = r#"parse_duration("abc s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        assert!(eval.eval_module(ast, &globals).is_err());

        // Test negative duration
        let script = r#"parse_duration("-5s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        assert!(eval.eval_module(ast, &globals).is_err());
    }

    #[test]
    fn test_parse_duration_edge_cases() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test zero duration
        let script = r#"parse_duration("0s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 0.0).abs() < f64::EPSILON);

        // Test very small duration
        let script = r#"parse_duration("1us")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 0.000001).abs() < f64::EPSILON);

        // Test multiple same units (should add up)
        let script = r#"parse_duration("1h1h")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 7200.0).abs() < f64::EPSILON); // 2 hours
    }

    #[test]
    fn test_parse_duration_humantime_fallback() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test space-separated format (humantime fallback)
        let script = r#"parse_duration("2h 30m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 9000.0).abs() < f64::EPSILON); // 2.5 hours

        // Test full word format (humantime fallback)
        let script = r#"parse_duration("1 hour 30 minutes")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 5400.0).abs() < f64::EPSILON); // 1.5 hours

        // Test years (humantime fallback)
        let script = r#"parse_duration("1y")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 31557600.0).abs() < f64::EPSILON); // 1 year = 365.25 days

        // Test months (humantime fallback)  
        let script = r#"parse_duration("1M")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 2630016.0).abs() < f64::EPSILON); // 1 month = 30.44 days

        // Test "day" vs "d" (humantime handles both)
        let script = r#"parse_duration("1 day")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 86400.0).abs() < f64::EPSILON); // 1 day

        // Test mixed humantime format
        let script = r#"parse_duration("1 hour 2 minutes 30 seconds")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 3750.0).abs() < f64::EPSILON); // 1h2m30s
    }

    #[test]
    fn test_parse_duration_hybrid_priority() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test that our implementation takes priority for formats both can handle
        let script = r#"parse_duration("5d")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 432000.0).abs() < f64::EPSILON); // 5 days

        // Test that our implementation handles fractional (humantime cannot)
        let script = r#"parse_duration("2.5s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 2.5).abs() < f64::EPSILON); // 2.5 seconds

        // Test that compact format without spaces uses our implementation
        let script = r#"parse_duration("3h30m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 12600.0).abs() < f64::EPSILON); // 3.5 hours

        // Test that spaces fall back to humantime
        let script = r#"parse_duration("3h 30m")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 12600.0).abs() < f64::EPSILON); // Same result, different parser
    }

    #[test]
    fn test_parse_duration_error_handling_hybrid() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test that completely invalid formats fail with helpful message
        let script = r#"parse_duration("invalid")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Invalid duration format"));
        assert!(error_msg.contains("Supported formats include"));

        // Test negative durations still fail
        let script = r#"parse_duration("-5s")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("cannot be negative"));

        // Test empty string
        let script = r#"parse_duration("")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_path_basic_dict_access() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("name", {"name": "Alice", "age": 30})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("Alice"));
    }

    #[test]
    fn test_get_path_nested_dict_access() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("user.name", {"user": {"name": "Bob", "id": 123}})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("Bob"));
    }

    #[test]
    fn test_get_path_array_access() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("items.1", {"items": ["first", "second", "third"]})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("second"));
    }

    #[test]
    fn test_get_path_nested_array_dict() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("users.0.name", {"users": [{"name": "Charlie", "id": 123}]})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("Charlie"));
    }

    #[test]
    fn test_get_path_with_default() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("missing.path", {"existing": "data"}, "fallback")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("fallback"));
    }

    #[test]
    fn test_get_path_out_of_bounds_array() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("items.10", {"items": ["a", "b"]}, "not_found")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("not_found"));
    }

    #[test]
    fn test_get_path_invalid_array_index() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("items.abc", {"items": ["a", "b"]}, "error")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_path_non_container_access() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("text.field", {"text": "not_a_dict"}, "default")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("default"));
    }

    #[test]
    fn test_get_path_empty_path() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        let script = r#"get_path("", {"key": "value"})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.get_type(), "dict");
    }

    #[test]
    fn test_get_path_mixed_types() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test accessing different data types
        let script = r#"get_path("number", {"number": 42, "text": "hello", "float": 3.14})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_i32(), Some(42));

        let script = r#"get_path("text", {"number": 42, "text": "hello", "float": 3.14})"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        assert_eq!(result.unpack_str(), Some("hello"));
    }

    // Statistical function tests
    #[test]
    fn test_avg_basic() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test with integers
        let script = r#"avg([1, 2, 3, 4, 5])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 3.0).abs() < f64::EPSILON);

        // Test with floats
        let script = r#"avg([1.5, 2.5, 3.5])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 2.5).abs() < f64::EPSILON);

        // Test with mixed numbers
        let script = r#"avg([10, 20.5, 15])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 15.166666666666666).abs() < 1e-10);
    }

    #[test]
    fn test_avg_edge_cases() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test single element
        let script = r#"avg([42])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 42.0).abs() < f64::EPSILON);

        // Test negative numbers
        let script = r#"avg([-1, -2, -3])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - (-2.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_error_cases() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test empty list
        let script = r#"avg([])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("avg() of empty sequence"));

        // Test non-list argument
        let script = r#"avg("not a list")"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("avg() argument must be a list"));
    }

    #[test]
    fn test_percentile_basic() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test median (50th percentile)
        let script = r#"percentile([1, 2, 3, 4, 5], 50)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 3.0).abs() < f64::EPSILON);

        // Test 0th percentile (minimum)
        let script = r#"percentile([5, 1, 3, 2, 4], 0)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 1.0).abs() < f64::EPSILON);

        // Test 100th percentile (maximum)
        let script = r#"percentile([5, 1, 3, 2, 4], 100)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentile_interpolation() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test case requiring interpolation
        let script = r#"percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 9.1).abs() < f64::EPSILON);

        // Test 25th percentile
        let script = r#"percentile([1, 2, 3, 4], 25)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 1.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_percentile_error_cases() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test empty list
        let script = r#"percentile([], 50)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("percentile() of empty sequence"));

        // Test invalid percentile value
        let script = r#"percentile([1, 2, 3], 150)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("percentile must be between 0 and 100"));

        // Test negative percentile
        let script = r#"percentile([1, 2, 3], -10)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("percentile must be between 0 and 100"));

        // Test non-list first argument
        let script = r#"percentile("not a list", 50)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("percentile() first argument must be a list"));
    }

    #[test]
    fn test_statistical_functions_with_floats() {
        let globals = GlobalsBuilder::new().with(global_functions).build();
        let module = Module::new();
        let mut eval = Evaluator::new(&module);

        // Test avg with decimal values
        let script = r#"avg([10.5, 20.3, 15.7, 8.2])"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        assert!((value - 13.675).abs() < f64::EPSILON);

        // Test percentile with decimal values and decimal percentile
        let script = r#"percentile([1.1, 2.2, 3.3, 4.4, 5.5], 75.5)"#;
        let ast = AstModule::parse("test", script.to_owned(), &Dialect::Extended).unwrap();
        let result = eval.eval_module(ast, &globals).unwrap();
        let value = result.to_string().parse::<f64>().unwrap();
        // Should interpolate between 3.3 and 4.4 at 75.5%
        assert!((value - 4.422).abs() < 0.001);
    }
}

// Additional globals for derive mode with stelp_ prefix
#[starlark_module]
pub(crate) fn derive_globals_with_prefix(builder: &mut starlark::environment::GlobalsBuilder) {
    fn stelp_emit(text: String) -> anyhow::Result<starlark::values::none::NoneType> {
        EMIT_BUFFER.with(|buffer| {
            buffer.borrow_mut().push(text);
        });
        Ok(starlark::values::none::NoneType)
    }

    fn stelp_skip() -> anyhow::Result<starlark::values::none::NoneType> {
        SKIP_FLAG.with(|flag| flag.set(true));
        Ok(starlark::values::none::NoneType)
    }

    fn stelp_exit<'v>(
        arg1: Option<starlark::values::Value<'v>>,
        arg2: Option<String>,
    ) -> anyhow::Result<starlark::values::none::NoneType> {
        let (code, msg) = match (arg1, arg2) {
            // stelp_exit() - no arguments
            (None, None) => (0, None),
            // stelp_exit(3) - integer as first argument
            (Some(val), None) if val.unpack_i32().is_some() => (val.unpack_i32().unwrap(), None),
            // stelp_exit("message") - string as first argument (backward compatibility)
            (Some(val), None) if val.unpack_str().is_some() => {
                (0, Some(val.unpack_str().unwrap().to_string()))
            }
            // stelp_exit(3, "message") - both arguments
            (Some(val), Some(msg)) if val.unpack_i32().is_some() => {
                (val.unpack_i32().unwrap(), Some(msg))
            }
            // Invalid usage
            _ => {
                return Err(anyhow::anyhow!(
                    "stelp_exit() expects stelp_exit(code=0, msg=None) - code must be an integer"
                ))
            }
        };

        EXIT_FLAG.with(|flag| flag.set(true));
        EXIT_MESSAGE.with(|message| {
            *message.borrow_mut() = msg;
        });
        EXIT_CODE.with(|exit_code| {
            exit_code.set(code);
        });
        Ok(starlark::values::none::NoneType)
    }

    fn stelp_inc(counter_name: String) -> anyhow::Result<i32> {
        CURRENT_CONTEXT.with(|ctx_cell| {
            if let Some((global_vars_ptr, _, _)) = *ctx_cell.borrow() {
                unsafe {
                    let global_vars = &*global_vars_ptr;
                    Ok(global_vars.increment_counter(&counter_name))
                }
            } else {
                Err(anyhow::anyhow!("No processing context available"))
            }
        })
    }

    fn stelp_parse_duration(duration: String) -> anyhow::Result<f64> {
        parse_duration_hybrid(&duration)
    }
}
