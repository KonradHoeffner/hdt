use crate::triples::TripleId;
use crate::triples::TriplesBitmap;

// TODO create and use object index
// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36

struct ObjectIter<'a> {
    // triples data
    triples: &'a TriplesBitmap,
    // x-coordinate identifier
    x: usize,
    // current position
    pos_y: usize,
    pos_z: usize,
    max_y: usize,
    max_z: usize,
}

impl<'a> ObjectIter<'a> {
    pub fn new(triples: &'a TriplesBitmap) -> Self {
        ObjectIter {
            x: 1,
            pos_y: 0,
            pos_z: 0,
            max_y: triples.adjlist_y.get_max(),
            max_z: triples.adjlist_z.get_max(),
            triples,
        }
    }
}

impl<'a> Iterator for ObjectIter<'a> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
