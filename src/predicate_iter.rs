use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

/// Iterator over all triples with a given property ID, answering an (?S,P,?O) query.
pub struct PredicateIter<'a> {
    triples: &'a TriplesBitmap,
    s: usize,
    p: usize,
    i: usize,
    os: usize,
    pos_z: usize,
    occs: usize,
}

impl<'a> PredicateIter<'a> {
    /// Create a new iterator over all triples with the given property ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, p: usize) -> Self {
        if p == 0 {
            panic!("object 0 does not exist, cant iterate");
        }
        let occs = triples.wavelet_y.rank(triples.wavelet_y.len(), p);
        //println!("the predicate {} is used by {} subjects in the index", p, occs);
        PredicateIter { triples, p, i: 0, pos_z: 0, os: 0, s: 0, occs }
    }
}

impl<'a> Iterator for PredicateIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.occs {
            return None;
        }
        if self.os == 0 {
            // Algorithm 1 findSubj from Martinez et al. 2012 ******
            let pos_y = self.triples.wavelet_y.select(self.i, self.p) as u64;
            self.s = self.triples.bitmap_y.dict.rank(pos_y, true) as usize + 1;
            // *****************************************************
            // SP can have multiple O
            self.pos_z = self.triples.adjlist_z.find(pos_y as usize);
            let pos_z_end = self.triples.adjlist_z.last(pos_y as usize);
            //println!("**** found predicate {} between {} and {} (inclusive)", self.p, self.pos_z, pos_z_end);
            self.os = pos_z_end - self.pos_z;
        } else {
            self.os -= 1;
            self.pos_z += 1;
        }

        let o = self.triples.adjlist_z.sequence.get(self.pos_z);
        if (self.os == 0) {
            self.i += 1;
        }
        Some(self.triples.coord_to_triple(self.s, self.p, o).unwrap())
    }
}
