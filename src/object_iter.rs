use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36
// actually only an object iterator when SPO order is used
// TODO test if it works at all
// TODO test with other orders and fix if broken

pub struct ObjectIter<'a> {
    // triples data
    triples: &'a TriplesBitmap,
    o: usize,
    pos_index: usize,
    max_index: usize,
}

impl<'a> ObjectIter<'a> {
    pub fn new(triples: &'a TriplesBitmap, o: usize) -> Self {
        let pos_index = triples.op_index.bitmap.dict.select1(o as u64).unwrap() as usize;
        let max_index = triples.op_index.bitmap.dict.select1(o as u64 + 1).unwrap() as usize - 1;
        println!("ObjectIter o={} pos_index={} max_index={}", o, pos_index, max_index);
        ObjectIter { pos_index, max_index, triples, o }
    }
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index >= self.max_index || self.o == 0 {
            if self.o == 0 {
                eprintln!("object is 0, cant iterate");
            }
            return None;
        }
        let pos_z = self.triples.op_index.sequence.get(self.pos_index) as u64;
        let pos_y = self.triples.adjlist_z.bitmap.dict.select1(pos_z).unwrap();
        let y = self.triples.adjlist_y.sequence.get(pos_y as usize);
        let x = self.triples.adjlist_y.bitmap.dict.rank(pos_y, true);
        Some(self.triples.coord_to_triple(x as usize, y as usize, self.o).unwrap())
    }
}
