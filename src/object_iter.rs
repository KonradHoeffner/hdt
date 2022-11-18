use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36
// actually only an object iterator when SPO order is used
// TODO test with other orders and fix if broken

/// Iterator over all triples with a given object ID, answering an (?S,?P,O) query.
pub struct ObjectIter<'a> {
    triples: &'a TriplesBitmap,
    o: usize,
    pos_index: usize,
    max_index: usize,
}

impl<'a> ObjectIter<'a> {
    /// Create a new iterator over all triples with the given object ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, o: usize) -> Self {
        if o == 0 {
            panic!("object 0 does not exist, cant iterate");
        }
        let pos_index = triples.op_index.bitmap.dict.select1(o as u64 - 1).unwrap() as usize;
        // mathematically, a maximum is inclusive, but use exclusive like in the C++ code to reduce confusion
        let max_index = triples.op_index.bitmap.dict.select1(o as u64).unwrap() as usize;
        //println!("ObjectIter o={} pos_index={} max_index={}", o, pos_index, max_index);
        ObjectIter { pos_index, max_index, triples, o }
    }
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index >= self.max_index {
            return None;
        }
        let pos_z = self.triples.op_index.sequence[self.pos_index] as u64;
        let pos_y = self.triples.adjlist_z.bitmap.dict.rank(pos_z, true);
        let y = self.triples.adjlist_y.sequence.get(pos_y as usize);
        let x = self.triples.adjlist_y.bitmap.dict.rank(pos_y, true) + 1;
        self.pos_index += 1;
        Some(self.triples.coord_to_triple(x as usize, y, self.o).unwrap())
    }
}
