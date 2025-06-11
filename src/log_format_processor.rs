// src/log_format_processor.rs - Line-based log format processing for Stelp

use crate::input_format::{InputFormat, LineParser, prepare_csv_processor};
use std::io::{BufRead, BufReader, Read};
use std::cell::{Cell, RefCell};
use serde_json;
use starlark::environment::{Module, Globals};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};
use starlark::values::{Value, dict::Dict, list::List};

// Thread-local state management (integrate with your existing implementation)
thread_local! {
    static EMIT_BUFFER: RefCell<Vec<String>> = RefCell::new(Vec::new());
    static SKIP_FLAG: Cell<bool> = Cell::new(false);
    static TERMINATE_FLAG: Cell<bool> = Cell::new(false);
}

// Skylark built-in functions for log processing
#[starlark::function]
fn emit(s: String) {
    EMIT_BUFFER.with(|buffer| buffer.borrow_mut().push(s));
}

#[starlark::function]
fn skip() {
    SKIP_FLAG.with(|flag| flag.set(true));
}

#[starlark::function]
fn terminate() {
    TERMINATE_FLAG.with(|flag| flag.set(true));
}

// State management functions
pub fn clear_execution_state() {
    SKIP_FLAG.with(|flag| flag.set(false));
    TERMINATE_FLAG.with(|flag| flag.set(false));
    EMIT_BUFFER.with(|buffer| buffer.borrow_mut().clear());
}

pub fn get_emissions() -> Vec<String> {
    EMIT_BUFFER.with(|buffer| {
        let emissions = buffer.borrow().clone();
        buffer.borrow_mut().clear();
        emissions
    })
}

pub fn is_skipped() -> bool {
    SKIP_FLAG.with(|flag| flag.get())
}

pub fn is_terminated() -> bool {
    TERMINATE_FLAG.with(|flag| flag.get())
}

// Existing structures (adapt to your actual implementations)
pub struct LineContext<'a> {
    pub line_number: usize,
    pub file_name: Option<&'a str>,
    pub global_vars: &'a GlobalVariables,
    pub debug: bool,
}

pub struct GlobalVariables {
    // Your existing global variables structure
}

impl GlobalVariables {
    pub fn new() -> Self {
        Self {}
    }
}

// Configuration focused on log analysis
pub struct LogFormatConfig {
    pub input_format: Option<InputFormat>,
    pub eval_expr: Option<String>,
    pub filter_expr: Option<String>,
    pub debug: bool,
    pub no_multiline: bool,
}

// Line-based log processor with structured data support
pub struct LogFormatProcessor {
    config: LogFormatConfig,
    global_vars: GlobalVariables,
}

impl LogFormatProcessor {
    pub fn new(config: LogFormatConfig) -> Self {
        Self {
            config,
            global_vars: GlobalVariables::new(),
        }
    }
    
    pub fn process_input<R: Read>(&self, reader: R) -> Result<(), String> {
        match &self.config.input_format {
            Some(format) => {
                // Structured log processing (JSONL, CSV)
                self.process_structured_logs(BufReader::new(reader), format)
            }
            None => {
                // Raw text log processing (existing behavior)
                self.process_raw_logs(BufReader::new(reader))
            }
        }
    }
    
    fn process_raw_logs<R: BufRead>(&self, reader: R) -> Result<(), String> {
        for (line_num, line_result) in reader.lines().enumerate() {
            let line = line_result.map_err(|e| format!("Error reading line {}: {}", line_num + 1, e))?;
            
            let ctx = LineContext {
                line_number: line_num + 1,
                file_name: None,
                global_vars: &self.global_vars,
                debug: self.config.debug,
            };
            
            if self.config.debug {
                eprintln!("Line {}: {:?}", line_num + 1, line);
            }
            
            self.execute_log_pipeline(&line, None, &ctx)?;
        }
        Ok(())
    }
    
    fn process_structured_logs<R: BufRead>(&self, reader: R, format: &InputFormat) -> Result<(), String> {
        match format {
            InputFormat::Csv => self.process_csv_logs(reader),
            InputFormat::Jsonl => self.process_jsonl_logs(reader),
        }
    }
    
    fn process_csv_logs<R: BufRead>(&self, reader: R) -> Result<(), String> {
        let (parser, lines) = prepare_csv_processor(reader)?;
        
        for (line_num, line_result) in lines.enumerate() {
            let line = line_result.map_err(|e| format!("Error reading line {}: {}", line_num + 2, e))?;
            
            let ctx = LineContext {
                line_number: line_num + 2, // +2 because we skip header
                file_name: None,
                global_vars: &self.global_vars,
                debug: self.config.debug,
            };
            
            if self.config.debug {
                eprintln!("Line {}: {:?}", line_num + 2, line);
            }
            
            // Parse CSV line
            let data = parser.parse_line(&line)
                .map_err(|e| format!("Parse error on line {}: {}", line_num + 2, e))?;
            
            self.execute_log_pipeline(&line, Some(data), &ctx)?;
        }
        Ok(())
    }
    
    fn process_jsonl_logs<R: BufRead>(&self, reader: R) -> Result<(), String> {
        let parser = crate::input_format::JsonlParser::new();
        
        for (line_num, line_result) in reader.lines().enumerate() {
            let line = line_result.map_err(|e| format!("Error reading line {}: {}", line_num + 1, e))?;
            
            let ctx = LineContext {
                line_number: line_num + 1,
                file_name: None,
                global_vars: &self.global_vars,
                debug: self.config.debug,
            };
            
            if self.config.debug {
                eprintln!("Line {}: {:?}", line_num + 1, line);
            }
            
            // Parse JSONL line
            let data = parser.parse_line(&line)
                .map_err(|e| format!("Parse error on line {}: {}", line_num + 1, e))?;
            
            self.execute_log_pipeline(&line, Some(data), &ctx)?;
        }
        Ok(())
    }
    
    fn execute_log_pipeline(
        &self, 
        line: &str, 
        data: Option<serde_json::Value>, 
        ctx: &LineContext
    ) -> Result<(), String> {
        // Execute filter first (if provided)
        if let Some(filter_expr) = &self.config.filter_expr {
            let passed = self.execute_filter(filter_expr, line, data.clone(), ctx)?;
            
            if !passed {
                if ctx.debug {
                    eprintln!("  filter: → SKIP");
                }
                return Ok(());
            }
            
            if ctx.debug {
                eprintln!("  filter: → PASS");
            }
        }
        
        // Execute eval (if provided)
        if let Some(eval_expr) = &self.config.eval_expr {
            self.execute_expression(eval_expr, line, data, ctx)?;
        }
        
        Ok(())
    }
    
    fn execute_filter(
        &self,
        expression: &str,
        line: &str,
        data: Option<serde_json::Value>,
        ctx: &LineContext
    ) -> Result<bool, String> {
        clear_execution_state();
        
        let result = self.execute_skylark(expression, line, data, ctx)?;
        
        // Convert result to boolean
        match result {
            Value::Bool(b) => Ok(b),
            Value::None => Ok(false),
            Value::String(ref s) => Ok(!s.is_empty()),
            Value::Int(i) => Ok(i != 0),
            Value::Float(f) => Ok(f != 0.0),
            _ => Ok(true), // Other values are truthy
        }
    }
    
    fn execute_expression(
        &self,
        expression: &str,
        line: &str,
        data: Option<serde_json::Value>,
        ctx: &LineContext
    ) -> Result<(), String> {
        clear_execution_state();
        
        let result = self.execute_skylark(expression, line, data, ctx)?;
        
        // Handle side effects
        let emissions = get_emissions();
        let skip_flag = is_skipped();
        let terminate_flag = is_terminated();
        
        // Output any emit() calls first
        for emission in emissions {
            println!("{}", emission);
        }
        
        if ctx.debug {
            eprintln!("  eval:");
            if !get_emissions().is_empty() {
                for emission in &get_emissions() {
                    eprintln!("    + emit: {:?}", emission);
                }
            }
            
            if skip_flag {
                eprintln!("    → SKIP");
            } else if terminate_flag {
                eprintln!("    → TERMINATE");
            } else {
                eprintln!("    → {:?}", self.value_to_string(result.clone())?);
            }
        }
        
        // Handle skip
        if skip_flag {
            return Ok(());
        }
        
        // Handle terminate
        if terminate_flag {
            return Err("Script requested termination".to_string());
        }
        
        // Output result
        println!("{}", self.value_to_string(result)?);
        
        Ok(())
    }
    
    fn execute_skylark(
        &self,
        expression: &str,
        line: &str,
        data: Option<serde_json::Value>,
        ctx: &LineContext
    ) -> Result<Value, String> {
        // Parse expression
        let ast = AstModule::parse("expression", expression.to_owned(), &Dialect::Standard)
            .map_err(|e| format!("Parse error: {}", e))?;
        
        // Set up Skylark environment
        let module = Module::new();
        let mut evaluator = Evaluator::new(&module);
        
        // Build globals with log processing functions
        let globals = self.build_log_globals()?;
        
        // Set up module variables for log analysis
        module.set("line", line.to_string());
        module.set("line_number", ctx.line_number as i32);
        
        if let Some(data_val) = data {
            let starlark_data = self.json_to_starlark_value(data_val, &module)?;
            module.set("data", starlark_data);
        }
        
        // Execute expression
        evaluator.eval_module(ast, &globals)
            .map_err(|e| format!("Execution error: {}", e))
    }
    
    fn build_log_globals(&self) -> Result<Globals, String> {
        let mut builder = starlark::environment::GlobalsBuilder::standard();
        
        // Add log processing functions
        builder.set("emit", emit);
        builder.set("skip", skip);
        builder.set("terminate", terminate);
        
        Ok(builder.build())
    }
    
    fn json_to_starlark_value(&self, json_val: serde_json::Value, module: &Module) -> Result<Value, String> {
        let heap = module.heap();
        
        match json_val {
            serde_json::Value::String(s) => Ok(heap.alloc(s)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(heap.alloc(i as i32))
                } else if let Some(f) = n.as_f64() {
                    Ok(heap.alloc(f))
                } else {
                    Err("Invalid number".to_string())
                }
            }
            serde_json::Value::Bool(b) => Ok(heap.alloc(b)),
            serde_json::Value::Null => Ok(Value::new_none()),
            serde_json::Value::Object(map) => {
                let dict = Dict::new();
                for (k, v) in map {
                    let key = heap.alloc(k);
                    let value = self.json_to_starlark_value(v, module)?;
                    dict.insert_hashed(key.get_hashed()?, value);
                }
                Ok(heap.alloc(dict))
            }
            serde_json::Value::Array(arr) => {
                let mut list = Vec::new();
                for item in arr {
                    list.push(self.json_to_starlark_value(item, module)?);
                }
                Ok(heap.alloc(List::from(list)))
            }
        }
    }
    
    fn value_to_string(&self, val: Value) -> Result<String, String> {
        Ok(val.to_str())
    }
}

// Integration function for main
pub fn create_log_format_processor(config: LogFormatConfig) -> LogFormatProcessor {
    LogFormatProcessor::new(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    
    #[test]
    fn test_raw_log_processing() {
        let config = LogFormatConfig {
            input_format: None,
            eval_expr: Some("line.upper()".to_string()),
            filter_expr: None,
            debug: false,
            no_multiline: false,
        };
        
        let processor = create_log_format_processor(config);
        let input = "2024-01-15 ERROR Database connection failed";
        let result = processor.process_input(Cursor::new(input));
        
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_jsonl_log_processing() {
        let config = LogFormatConfig {
            input_format: Some(InputFormat::Jsonl),
            eval_expr: Some("data[\"level\"]".to_string()),
            filter_expr: None,
            debug: false,
            no_multiline: false,
        };
        
        let processor = create_log_format_processor(config);
        let input = r#"{"timestamp": "2024-01-15T10:00:00Z", "level": "ERROR", "message": "Database failed"}"#;
        let result = processor.process_input(Cursor::new(input));
        
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_csv_log_processing() {
        let config = LogFormatConfig {
            input_format: Some(InputFormat::Csv),
            eval_expr: Some("data[\"level\"]".to_string()),
            filter_expr: Some("data[\"level\"] == \"ERROR\"".to_string()),
            debug: false,
            no_multiline: false,
        };
        
        let processor = create_log_format_processor(config);
        let input = "timestamp,level,message\n2024-01-15T10:00:00Z,ERROR,Database failed\n2024-01-15T10:01:00Z,INFO,Request processed";
        let result = processor.process_input(Cursor::new(input));
        
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_jsonl_with_filter() {
        let config = LogFormatConfig {
            input_format: Some(InputFormat::Jsonl),
            eval_expr: Some("data[\"message\"]".to_string()),
            filter_expr: Some("data[\"level\"] == \"ERROR\"".to_string()),
            debug: false,
            no_multiline: false,
        };
        
        let processor = create_log_format_processor(config);
        let input = r#"{"level": "INFO", "message": "All good"}
{"level": "ERROR", "message": "Something broke"}
{"level": "DEBUG", "message": "Debugging info"}"#;
        let result = processor.process_input(Cursor::new(input));
        
        assert!(result.is_ok());
    }
}