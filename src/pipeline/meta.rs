// src/pipeline/meta.rs
use crate::pipeline::context::RecordContext;
use starlark::values::{Heap, Value};
use starlark::environment::Module;
use starlark::starlark_module;

/// Transform meta.property calls to meta_property variables
pub fn preprocess_meta_namespace(script: &str) -> String {
    // Transform meta.linenum -> meta_linenum, etc.
    let re = regex::Regex::new(r"\bmeta\.([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    re.replace_all(script, "meta_$1").to_string()
}
/// Create meta variables as individual variables instead of a complex object
/// This approach works better with Starlark's f-string limitations
pub fn inject_meta_variables<'v>(module: &Module, ctx: &RecordContext) {
    let heap = module.heap();
    
    // Inject individual meta variables that can be used directly in f-strings
    module.set("meta_linenum", heap.alloc(ctx.line_number as i32));
    module.set("meta_line_number", heap.alloc(ctx.line_number as i32));
    module.set("meta_record_count", heap.alloc(ctx.record_count as i32));
    
    if let Some(name) = ctx.file_name {
        module.set("meta_filename", heap.alloc(name));
        module.set("meta_file_name", heap.alloc(name));
    } else {
        module.set("meta_filename", Value::new_none());
        module.set("meta_file_name", Value::new_none());
    }
}

#[starlark_module]
pub fn meta_functions(builder: &mut starlark::environment::GlobalsBuilder) {
    // Keep existing st_* functions for backward compatibility
    fn st_line_number() -> anyhow::Result<i32> {
        use crate::pipeline::simple_globals::CURRENT_CONTEXT;
        let line_num = CURRENT_CONTEXT.with(|ctx| {
            if let Some((_, line_number, _)) = *ctx.borrow() {
                line_number as i32
            } else {
                0
            }
        });
        Ok(line_num)
    }

    fn st_file_name<'v>(heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        use crate::pipeline::simple_globals::CURRENT_CONTEXT;
        let filename = CURRENT_CONTEXT.with(|ctx| {
            if let Some((_, _, ref filename)) = *ctx.borrow() {
                filename.clone()
            } else {
                None
            }
        });

        if let Some(name) = filename {
            Ok(heap.alloc(name))
        } else {
            Ok(Value::new_none())
        }
    }
}