use crate::Hdt;
use crate::IdKind;
use std::io::Cursor;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "Hdt")]
pub struct HdtWasm {
    hdt: Hdt,
}

#[wasm_bindgen(js_name = "Hdt")]
impl HdtWasm {
    #[wasm_bindgen(constructor)]
    pub fn new(data: Vec<u8>) -> Result<HdtWasm, JsError> {
        let cursor = Cursor::new(data);
        let hdt = Hdt::read(cursor).map_err(|e| JsError::new(&e.to_string()))?;
        Ok(Self { hdt })
    }

    /// Returns a flat Int32Array of IDs [s1, p1, o1, s2, p2, o2, ...]
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

    pub fn subject_str(&self, id: u32) -> Result<String, JsError> {
        self.hdt.dict.id_to_string(id as usize, IdKind::Subject).map_err(|_| JsError::new("Subject ID not found"))
    }

    pub fn predicate_str(&self, id: u32) -> Result<String, JsError> {
        self.hdt
            .dict
            .id_to_string(id as usize, IdKind::Predicate)
            .map_err(|_| JsError::new("Predicate ID not found"))
    }

    pub fn object_str(&self, id: u32) -> Result<String, JsError> {
        self.hdt.dict.id_to_string(id as usize, IdKind::Object).map_err(|_| JsError::new("Object ID not found"))
    }
}
