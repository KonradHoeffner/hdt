//! Manual WebAssembly bindings without wasm-bindgen
//!
//! This module provides simple C-style exports that work with wasm64

use crate::Hdt;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Read};
use std::sync::Mutex;

/// Structured RDF term representation for efficient JavaScript parsing
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "termType", rename_all = "PascalCase")]
pub enum RdfTerm {
    NamedNode {
        value: String,
    },
    BlankNode {
        value: String,
    },
    Literal {
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        language: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        datatype: Option<String>,
    },
}

/// Structured triple representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredTriple {
    pub subject: RdfTerm,
    pub predicate: RdfTerm,
    pub object: RdfTerm,
}

/// Parse an HDT term string into a structured RdfTerm
fn parse_hdt_term(term: &str) -> RdfTerm {
    // Literal with language tag: "value"@lang
    if let Some(at_pos) = term.rfind("\"@") {
        if term.starts_with('"') {
            let value = &term[1..at_pos];
            let language = &term[at_pos + 2..];
            return RdfTerm::Literal {
                value: value.to_string(),
                language: Some(language.to_string()),
                datatype: None,
            };
        }
    }

    // Literal with datatype: "value"^^<datatype>
    if let Some(caret_pos) = term.find("\"^^<") {
        if term.starts_with('"') && term.ends_with('>') {
            let value = &term[1..caret_pos];
            let datatype = &term[caret_pos + 4..term.len() - 1];
            return RdfTerm::Literal {
                value: value.to_string(),
                language: None,
                datatype: Some(datatype.to_string()),
            };
        }
    }

    // Simple literal: "value"
    if term.starts_with('"') && term.ends_with('"') {
        return RdfTerm::Literal { value: term[1..term.len() - 1].to_string(), language: None, datatype: None };
    }

    // Blank node: _:id
    if term.starts_with("_:") {
        return RdfTerm::BlankNode { value: term.to_string() };
    }

    // Named node (IRI) - default case
    RdfTerm::NamedNode { value: term.to_string() }
}

// Global HDT instance storage
static HDT_INSTANCE: Mutex<Option<Hdt>> = Mutex::new(None);

// Global error message storage (for debugging)
static LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);

// Global debug log storage
static DEBUG_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

// Simple logger for WASM that stores messages
struct WasmLogger;

impl log::Log for WasmLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let msg = format!("[{}] {}", record.level(), record.args());
        if let Ok(mut log) = DEBUG_LOG.lock() {
            log.push(msg);
        }
    }

    fn flush(&self) {}
}

static LOGGER: WasmLogger = WasmLogger;

/// Initialize logging (call once at startup)
#[unsafe(no_mangle)]
pub extern "C" fn hdt_init_logging() -> i32 {
    match log::set_logger(&LOGGER).map(|()| log::set_max_level(log::LevelFilter::Debug)) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

/// Simple BufRead implementation for slices that doesn't use std::io::BufReader
/// This avoids WASM64 issues with BufReader
struct SliceBufReader<'a> {
    slice: &'a [u8],
    pos: usize,
}

impl<'a> SliceBufReader<'a> {
    fn new(slice: &'a [u8]) -> Self {
        Self { slice, pos: 0 }
    }
}

impl<'a> Read for SliceBufReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = &self.slice[self.pos..];
        let to_read = buf.len().min(remaining.len());
        buf[..to_read].copy_from_slice(&remaining[..to_read]);
        self.pos += to_read;
        Ok(to_read)
    }
}

impl<'a> BufRead for SliceBufReader<'a> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        Ok(&self.slice[self.pos..])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = (self.pos + amt).min(self.slice.len());
    }
}

/// Load an HDT file from bytes
/// Returns 0 on success, negative on error (error code = -bytes_read - 1)
///
/// WASM64 Note: Instead of using from_raw_parts which doesn't work in WASM64,
/// we directly access the linear memory through volatile reads
#[unsafe(no_mangle)]
pub extern "C" fn hdt_load(ptr: *const u8, len: usize) -> i32 {
    // Version logging to verify correct build
    const VERSION: &str = "WASM64-v2.0-NoThreading-2025-11-22";
    if let Ok(mut log) = DEBUG_LOG.lock() {
        log.push(format!("[VERSION] {}", VERSION));
        log.push(format!("[RUST-ENTRY] hdt_load called: ptr={:?}, len={}", ptr, len));
    }

    if len < 10 {
        if let Ok(mut log) = DEBUG_LOG.lock() {
            log.push(format!("[RUST-ERROR] Data too short: len={}", len));
        }
        return -9999; // Data too short
    }

    // Directly access linear memory through volatile reads to work around WASM64 pointer issues
    // This copies the data byte-by-byte from WASM linear memory into a Rust Vec
    let mut data_vec = Vec::with_capacity(len);
    unsafe {
        for i in 0..len {
            let byte_ptr = ptr.add(i);
            let byte = core::ptr::read_volatile(byte_ptr);
            data_vec.push(byte);
        }
    }

    // Debug: Check first bytes in Rust
    if let Ok(mut log) = DEBUG_LOG.lock() {
        let first_20: Vec<String> = data_vec.iter().take(20).map(|b| format!("{:02x}", b)).collect();
        log.push(format!("[RUST] data_vec length: {}", data_vec.len()));
        log.push(format!("[RUST] First 20 bytes: {}", first_20.join(" ")));
    }

    // Verify we read the correct data
    if data_vec.len() != len {
        if let Ok(mut log) = DEBUG_LOG.lock() {
            log.push(format!("[RUST-ERROR] Length mismatch: expected={}, got={}", len, data_vec.len()));
        }
        return -9999; // Length mismatch
    }

    // Verify the HDT cookie
    let cookie_check = &data_vec[0..4];
    if let Ok(mut log) = DEBUG_LOG.lock() {
        log.push(format!("[RUST] Cookie check: {:02x?} vs b\"$HDT\" ({:02x?})", cookie_check, b"$HDT"));
    }
    if cookie_check != b"$HDT" {
        // Encode what we actually read in the error
        let b0 = data_vec[0] as i32;
        let b1 = data_vec[1] as i32;
        let b2 = data_vec[2] as i32;
        let b3 = data_vec[3] as i32;
        if let Ok(mut log) = DEBUG_LOG.lock() {
            log.push(format!(
                "[RUST-ERROR] Cookie mismatch! Got bytes: [{:02x}, {:02x}, {:02x}, {:02x}]",
                b0, b1, b2, b3
            ));
            log.push(format!(
                "[RUST-ERROR] As string: '{}'",
                String::from_utf8_lossy(&[b0 as u8, b1 as u8, b2 as u8, b3 as u8])
            ));
        }
        // Return error with first 4 bytes encoded
        return -(9000 + b0 + (b1 << 8) + (b2 << 16) + (b3 << 24));
    }

    // Use our custom SliceBufReader instead of std::io::BufReader
    // This avoids WASM64 issues with BufReader::read_until
    let mut reader = SliceBufReader::new(&data_vec);

    match Hdt::read(&mut reader) {
        Ok(hdt) => {
            let mut instance = HDT_INSTANCE.lock().unwrap();
            *instance = Some(hdt);
            0 // Success
        }
        Err(_e) => {
            // Return negative value encoding the byte position where it failed
            // This works around WASM64 static Mutex issues
            // Error code = -(bytes_read + 1), so we can distinguish from success (0)
            -((reader.pos as i32) + 1)
        }
    }
}

/// Get the last error message
/// Returns the length of the error message written to the buffer, or 0 if no error
#[unsafe(no_mangle)]
pub extern "C" fn hdt_get_last_error(output_ptr: *mut u8, output_capacity: usize) -> i32 {
    let last_error = LAST_ERROR.lock().unwrap();
    match last_error.as_ref() {
        Some(error_msg) => {
            let error_bytes = error_msg.as_bytes();
            if error_bytes.len() > output_capacity {
                return -1; // Buffer too small
            }
            let output_slice = unsafe { std::slice::from_raw_parts_mut(output_ptr, error_bytes.len()) };
            output_slice.copy_from_slice(error_bytes);
            error_bytes.len() as i32
        }
        None => 0, // No error
    }
}

/// Get debug logs
/// Returns the length of the debug log written to the buffer as JSON array
#[unsafe(no_mangle)]
pub extern "C" fn hdt_get_debug_log(output_ptr: *mut u8, output_capacity: usize) -> i32 {
    let debug_log = DEBUG_LOG.lock().unwrap();
    let json = match serde_json::to_string(&*debug_log) {
        Ok(j) => j,
        Err(e) => {
            // If serialization fails, return an error array with the error message
            format!("[\"Serialization error: {}\"]", e)
        }
    };
    let json_bytes = json.as_bytes();

    if json_bytes.len() > output_capacity {
        return -2; // Buffer too small
    }

    let output_slice = unsafe { std::slice::from_raw_parts_mut(output_ptr, json_bytes.len()) };
    output_slice.copy_from_slice(json_bytes);
    json_bytes.len() as i32
}

/// Clear debug logs
#[unsafe(no_mangle)]
pub extern "C" fn hdt_clear_debug_log() {
    if let Ok(mut log) = DEBUG_LOG.lock() {
        log.clear();
    }
}

/// Count triples matching a pattern WITHOUT loading them into memory
/// Returns the count, or -1 on error
#[unsafe(no_mangle)]
pub extern "C" fn hdt_count_triples(
    subject_ptr: *const u8, subject_len: usize, predicate_ptr: *const u8, predicate_len: usize,
    object_ptr: *const u8, object_len: usize,
) -> i64 {
    let instance = HDT_INSTANCE.lock().unwrap();
    let hdt = match instance.as_ref() {
        Some(h) => h,
        None => return -1,
    };

    // Parse input strings
    let subject = if subject_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(subject_ptr, subject_len) };
        Some(std::str::from_utf8(bytes).unwrap())
    } else {
        None
    };

    let predicate = if predicate_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(predicate_ptr, predicate_len) };
        Some(std::str::from_utf8(bytes).unwrap())
    } else {
        None
    };

    let object = if object_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(object_ptr, object_len) };
        Some(std::str::from_utf8(bytes).unwrap())
    } else {
        None
    };

    // Count matching triples (doesn't load them into memory)
    let count = hdt.triples_with_pattern(subject, predicate, object).count();
    count as i64
}

/// Query triples and write results to a buffer
/// Returns the number of triples found, or -1 on error
#[unsafe(no_mangle)]
pub extern "C" fn hdt_query_triples(
    subject_ptr: *const u8, subject_len: usize, predicate_ptr: *const u8, predicate_len: usize,
    object_ptr: *const u8, object_len: usize, output_ptr: *mut u8, output_capacity: usize,
) -> i32 {
    let instance = HDT_INSTANCE.lock().unwrap();
    let hdt = match instance.as_ref() {
        Some(h) => h,
        None => return -1,
    };

    // Parse input strings
    let subject = if subject_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(subject_ptr, subject_len) };
        Some(std::str::from_utf8(bytes).unwrap_or(""))
    } else {
        None
    };

    let predicate = if predicate_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(predicate_ptr, predicate_len) };
        Some(std::str::from_utf8(bytes).unwrap_or(""))
    } else {
        None
    };

    let object = if object_len > 0 {
        let bytes = unsafe { std::slice::from_raw_parts(object_ptr, object_len) };
        Some(std::str::from_utf8(bytes).unwrap_or(""))
    } else {
        None
    };

    // Query and convert to structured triples
    let results: Vec<StructuredTriple> = hdt
        .triples_with_pattern(subject, predicate, object)
        .map(|triple| StructuredTriple {
            subject: parse_hdt_term(&triple[0]),
            predicate: parse_hdt_term(&triple[1]),
            object: parse_hdt_term(&triple[2]),
        })
        .collect();

    // Serialize results as JSON
    let json = serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string());
    let json_bytes = json.as_bytes();

    if json_bytes.len() > output_capacity {
        return -2; // Buffer too small
    }

    let output_slice = unsafe { std::slice::from_raw_parts_mut(output_ptr, json_bytes.len()) };
    output_slice.copy_from_slice(json_bytes);

    json_bytes.len() as i32
}

/// Get the size of the HDT instance in memory
#[unsafe(no_mangle)]
pub extern "C" fn hdt_size_in_bytes() -> usize {
    let instance = HDT_INSTANCE.lock().unwrap();
    instance.as_ref().map(|h| h.size_in_bytes()).unwrap_or(0)
}

/// Allocate memory (for passing data from JS)
#[unsafe(no_mangle)]
pub extern "C" fn hdt_alloc(size: usize) -> *mut u8 {
    let mut buf = Vec::with_capacity(size);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Free memory
#[unsafe(no_mangle)]
pub extern "C" fn hdt_free(ptr: *mut u8, size: usize) {
    unsafe {
        let _ = Vec::from_raw_parts(ptr, 0, size);
    }
}
