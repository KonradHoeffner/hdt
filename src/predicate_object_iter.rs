use crate::triples::Id;
use crate::triples::TripleId;
use crate::triples::TriplesBitmap;
use std::cmp::Ordering;

// see filterPredSubj in "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36

/// Iterator over all triples with a given predicate and object ID, answering an (?S,P,O) query.
pub struct PredicateObjectIter<'a> {
    triples: &'a TriplesBitmap,
    p: Id,
    o: Id,
    pos_index: usize,
    max_index: usize,
}

impl<'a> PredicateObjectIter<'a> {
    /// Create a new iterator over all triples with the given object ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, p: Id, o: Id) -> Self {
        assert!(o != 0, "object 0 does not exist, cant iterate");
        let mut low = triples.op_index.find(o);
        let mut high = triples.op_index.last(o);
        let get_y = |pos_index| {
            let pos_z = triples.op_index.sequence.get(pos_index) as u64;
            let pos_y = triples.adjlist_z.bitmap.dict.rank(pos_z, true);
            triples.wavelet_y.get(pos_y as usize) as Id
        };
        while low <= high {
            let mut mid = (low + high) / 2;
            match get_y(mid).cmp(&p) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => {
                    // Each value may occur multiple times, so we search for the left border.
                    let mut border_high = high;
                    while low < border_high {
                        mid = (low + border_high) / 2;
                        match get_y(mid).cmp(&p) {
                            Ordering::Less => low = mid + 1,
                            _ => border_high = mid,
                        }
                    }
                    return PredicateObjectIter { triples, p, o, pos_index: low, max_index: high };
                }
            }
        }
        // not found
        return PredicateObjectIter { triples, p, o, pos_index: 999, max_index: 0 };
    }
}

impl<'a> Iterator for PredicateObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        while self.pos_index <= self.max_index {
            let pos_z = self.triples.op_index.sequence.get(self.pos_index) as u64;
            let pos_y = self.triples.adjlist_z.bitmap.dict.rank(pos_z, true);
            let y = self.triples.wavelet_y.get(pos_y as usize) as Id;
            //println!(" op p {y}");
            if y != self.p {
                return None;
            }
            let x = self.triples.bitmap_y.dict.rank(pos_y, true) as Id + 1;
            self.pos_index += 1;
            return Some(self.triples.coord_to_triple(x, y, self.o).unwrap());
        }
        return None;
    }
}
