use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

/// Iterator over all triples with a given property ID, answering an (?S,P,?O) query.
/// TODO implement
pub struct PredicateIter<'a> {
    triples: &'a TriplesBitmap,
    s: usize,
    p: usize,
    i: usize,
    os: usize,
    pos_z: usize,
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
        PredicateIter { triples, p, i: 1, pos_z: 0, os: 0, s: 0, occs }
    }
}

// TODO: simplify?
impl<'a> Iterator for PredicateIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i > self.occs {
            return None;
        }
        if self.os == 0 {
            // Algorithm 1 findSubj from Martinez et al. 2012 ******
            // in the paper i is used but i-1 works correctly
            let pos_y = self.triples.wavelet_y.select(self.i - 1, self.p) as u64;
            // add one to the formula from the paper because that works out
            self.s = self.triples.adjlist_y.bitmap.dict.rank(pos_y, true) as usize + 1;
            // *****************************************************
            // SP can have multiple O
            self.pos_z = self.triples.adjlist_z.bitmap.dict.select(pos_y, true).unwrap() as usize;
            let pos_z_end = self.triples.adjlist_z.bitmap.dict.select(pos_y + 1, true).unwrap() as usize;
            self.os = pos_z_end - self.pos_z - 1; // is this correct? it causes the test to fail. but i think the test is wrong instead.
                                                  //println!("os os {}",self.os);
        } else {
            self.os -= 1;
            self.pos_z += 1;
        }
        let o = self.triples.adjlist_z.sequence.get(self.pos_z);
        if (self.os == 0) {
            self.i += 1;
        }
        //return None;
        //return Some(self.triples.coord_to_triple(s as usize, self.p, 99).unwrap());
        return Some(self.triples.coord_to_triple(self.s, self.p, o).unwrap());
        //Some(self.triples.coord_to_triple(x as usize, self.p, z).unwrap())
    }
}
