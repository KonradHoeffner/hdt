use crate::triples::Id;
use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36
// actually only an object iterator when SPO order is used
// TODO test with other orders and fix if broken

/// Iterator over all triples with a given object ID, answering an (?S,?P,O) query.
pub struct ObjectIter<'a> {
    triples: &'a TriplesBitmap,
    o: Id,
    pos_index: usize,
    max_index: usize,
}

impl<'a> ObjectIter<'a> {
    /// Create a new iterator over all triples with the given object ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, o: Id) -> Self {
        assert!(o != 0, "object 0 does not exist, cant iterate");
        let pos_index = triples.op_index.find(o);
        //debug_assert_eq!(o, triples.adjlist_z.get_id(pos_z as usize));
        let max_index = triples.op_index.last(o);
        //println!("ObjectIter o={} pos_index={} max_index={}", o, pos_index, max_index);
        ObjectIter { triples, o, pos_index, max_index }
    }
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index > self.max_index {
            return None;
        }
        let pos_z = self.triples.op_index.sequence.get(self.pos_index) as u64;
        let pos_y = self.triples.adjlist_z.bitmap.dict.rank(pos_z, true);
        let y = self.triples.wavelet_y.get(pos_y as usize) as Id;
        let x = self.triples.bitmap_y.dict.rank(pos_y, true) as Id + 1;
        self.pos_index += 1;
        Some(self.triples.coord_to_triple(x, y, self.o).unwrap())
    }
}
