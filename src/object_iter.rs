use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

// TODO create and use object index
// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36
// actually only an object iterator when SPO order is used
// TODO test with other orders and fix if broken

struct ObjectIter<'a> {
    // triples data
    triples: &'a TriplesBitmap,
    pos_index: usize,
    max_index: usize,
}

impl<'a> ObjectIter<'a> {
    pub fn new(triples: &'a TriplesBitmap, o: usize) -> Self {
        let pos_index = triples.op_index.bitmap.dict.select1(o as u64);
        let max_index = triples.op_index.bitmap.dict.select1(o as u64 + 1) - 1;
        ObjectIter { pos_index, max_index, triples }
    }
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index >= max_index {
            return None;
        }
        let pos_z = self.triples.op_index.sequence.get(self.pos_index);
        let pos_y = self.triples.adjlist_z.bitmap.dict.select1(pos_z);
        let y = self.triples.adjlist_y.sequence.get(pos_y);
        let x = self.triples.adjlist_y.bitmap.rank1(pos_y);
        Some(self.triples.coord_to_triple(x, y, z))
    }
}
