use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

/// Iterator over all triples with a given property ID, answering an (?S,P,?O) query.
/// TODO implement
pub struct PredicateIter<'a> {
    triples: &'a TriplesBitmap,
    p: usize,
    /*pos_index: usize,
    max_index: usize,*/
}

impl<'a> PredicateIter<'a> {
    /// Create a new iterator over all triples with the given property ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, p: usize) -> Self {
        if p == 0 {
            panic!("object 0 does not exist, cant iterate");
        }
        PredicateIter { triples, p }
    }
}

impl<'a> Iterator for PredicateIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        //if self.pos_index >= self.max_index {
        return None;
        //}
        //Some(self.triples.coord_to_triple(x as usize, self.p, z).unwrap())
    }
}
