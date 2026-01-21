use crate::Hdt;
use crate::IdKind;
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "Hdt")]
pub struct HdtWasm {
    hdt: Hdt,
}

#[wasm_bindgen(js_class = "Hdt")]
impl HdtWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(data: Vec<u8>) -> Result<HdtWasm, JsError> {
        std::panic::set_hook(Box::new(console_error_panic_hook::hook));
        let cursor = Cursor::new(data);
        let hdt = Hdt::read(cursor).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { hdt })
    }

    /// Returns a flat Int32Array of IDs [s1, p1, o1, s2, p2, o2, ...].
    /// There is some duplication with constants in triple patterns but as we only return 32 bit integers this should only be a few MB even for millions of results.
    /// On the other hand this hopefully allows performant transitions between WASM and JavaScript.
    /// Also this is expected to often be used with pagination and should use CPU cache better when using a specific "window".
    #[allow(clippy::needless_pass_by_value)]
    pub fn triple_ids_with_pattern(
        &self, sp: Option<String>, pp: Option<String>, op: Option<String>,
    ) -> Box<[u32]> {
        let ids = self.hdt.triple_ids_with_pattern(sp.as_deref(), pp.as_deref(), op.as_deref());
        // flatten into a single array for higher JavaScript performance
        // prevents inefficient generation of JavaScript array of arrays
        let flat: Box<[u32]> = ids.flat_map(|[s, p, o]| [s as u32, p as u32, o as u32]).collect();
        flat
    }

    // --- Translation Functions ---

    /// ids: flat Int32Array of IDs [s1, p1, o1, s2, p2, o2, ...].
    /// Returns string triples as a flat array of strings [s1, p1, o1, s2, p2, o2, ...].
    /// WASM memory is limited, several million triple IDs may lead to OOM crashes reported as "RuntimeError: unreachable executed"
    pub fn ids_to_strings(&self, ids: &[u32]) -> Result<Vec<String>, JsError> {
        if !ids.len().is_multiple_of(3) {
            return Err(JsError::new("Input array length must be a multiple of 3"));
        }
        let mut strings = Vec::with_capacity(ids.len());
        for (i, id) in ids.iter().enumerate() {
            strings.push(
                self.hdt
                    .dict
                    .id_to_string(*id as usize, IdKind::KINDS[i % 3])
                    .map_err(|_| JsError::new(&format!("{:?} ID {id} does not exist", IdKind::KINDS[i % 3])))?,
            );
        }
        Ok(strings)
    }
}
