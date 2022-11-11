use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

/// Iterator over all triples with a given property ID, answering an (?S,P,?O) query.
/// TODO implement
pub struct PredicateIter<'a> {
    triples: &'a TriplesBitmap,
    p: usize,
    i: usize,
    occs: usize, /*pos_index: usize,
                 max_index: usize,*/
}

impl<'a> PredicateIter<'a> {
    /// Create a new iterator over all triples with the given property ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, p: usize) -> Self {
        if p == 0 {
            panic!("object 0 does not exist, cant iterate");
        }
        let occs = triples.wavelet_y.rank(triples.wavelet_y.len(), p);
        println!("the predicate {} occurs {} times", p, occs);
        //Self::find_subj(triples, p);
        PredicateIter { triples, p, i: 1, occs }
    }

    fn find_subj(triples: &TriplesBitmap, p: usize) {
        let occs = triples.wavelet_y.rank(triples.wavelet_y.len(), p);

        println!("the predicate {} occurs {} times", p, occs);
    }
}

impl<'a> Iterator for PredicateIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.occs {
            // should be > but crashes with it
            return None;
        }
        //let pos = self.triples.wavelet_y.select(self.i ,self.p) as u64;
        let pos = self.triples.wavelet_y.select(self.i, self.p) as u64;
        let s = self.triples.adjlist_y.bitmap.dict.rank(pos, true);
        self.i += 1;
        //return None;
        //return Some(self.triples.coord_to_triple(s as usize, self.p, 99).unwrap());
        return Some(self.triples.coord_to_triple(s as usize, self.p, 99).unwrap());
        //Some(self.triples.coord_to_triple(x as usize, self.p, z).unwrap())
    }
}
