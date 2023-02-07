use crate::triples::Id;
use crate::triples::TriplesBitmap;

/// Alternative iterator over all subject IDs with a given predicate and object ID, answering an (?S,P,O) query.
pub struct PredicateObjectIter2<'a> {
    triples: &'a TriplesBitmap,
    p: Id,
    pos_index: usize,
    max_index: usize,
}

impl<'a> PredicateObjectIter2<'a> {
    /// Create a new iterator over all triples with the given predicate and object ID.
    /// Panics if the predicate or object ID is 0.
    pub fn new(triples: &'a TriplesBitmap, p: Id, o: Id) -> Self {
        assert_ne!(0, p, "predicate 0 does not exist, cant iterate");
        assert_ne!(0, o, "object 0 does not exist, cant iterate");
        let pos_index = triples.op_index.find(o);
        //debug_assert_eq!(o, triples.adjlist_z.get_id(pos_z as usize));
        let max_index = triples.op_index.last(o);
        //println!("PredicateObjectIter2 o={} pos_index={} max_index={}", o, pos_index, max_index);
        PredicateObjectIter2 { triples, p, pos_index, max_index }
    }
}

impl<'a> Iterator for PredicateObjectIter2<'a> {
    type Item = Id;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index > self.max_index {
            return None;
        }
        let pos_z = self.triples.op_index.sequence.get(self.pos_index) as u64;
        let pos_y = self.triples.adjlist_z.bitmap.dict.rank(pos_z, true);
        let y = self.triples.wavelet_y.get(pos_y as usize) as Id;
        self.pos_index += 1;
        if y != self.p {
            return self.next();
        }
        let s = self.triples.bitmap_y.dict.rank(pos_y, true) as Id + 1;
        Some(s)
    }
}
