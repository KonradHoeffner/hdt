use crate::containers::{AdjList, Bitmap, Sequence};
use crate::ControlInfo;
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub enum TripleSect {
    Bitmap(TriplesBitmap),
}

impl TripleSect {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        let triples_ci = ControlInfo::read(reader)?;

        match &triples_ci.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => {
                Ok(TripleSect::Bitmap(TriplesBitmap::read(reader, triples_ci)?))
            }
            "<http://purl.org/HDT/hdt#triplesList>" => {
                Err(Error::new(InvalidData, "Triples Lists are not supported yet."))
            }
            _ => Err(Error::new(InvalidData, "Unknown triples listing format.")),
        }
    }

    pub fn read_all_ids(self) -> TriplesBitmap {
        match self {
            TripleSect::Bitmap(bitmap) => bitmap,
        }
    }

    pub fn triples_with_s(self, subject_id: usize) -> BitmapIter {
        match self {
            TripleSect::Bitmap(bitmap) => {
                /*let start_pos = bitmap.adjlist_y.find(sid);
                let end_pos = bitmap.adjlist_y.find(sid + 1);*/
                BitmapIter::with_s(bitmap, subject_id)
            }
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Order {
    Unknown = 0,
    SPO = 1,
    SOP = 2,
    PSO = 3,
    POS = 4,
    OSP = 5,
    OPS = 6,
}

impl TryFrom<u32> for Order {
    type Error = std::io::Error;

    fn try_from(original: u32) -> Result<Self, Self::Error> {
        match original {
            0 => Ok(Order::Unknown),
            1 => Ok(Order::SPO),
            2 => Ok(Order::SOP),
            3 => Ok(Order::PSO),
            4 => Ok(Order::POS),
            5 => Ok(Order::OSP),
            6 => Ok(Order::OPS),
            _ => Err(Self::Error::new(io::ErrorKind::InvalidData, "Unrecognized order")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TriplesBitmap {
    order: Order,
    adjlist_y: AdjList,
    adjlist_z: AdjList,
}

impl TriplesBitmap {
    pub fn read<R: BufRead>(reader: &mut R, triples_ci: ControlInfo) -> io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind::InvalidData;

        // read order
        let order: Order;
        if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            order = Order::try_from(n)?;
        } else {
            return Err(Error::new(InvalidData, "Unrecognized order"));
        }

        // read bitmaps
        let bitmap_y = Bitmap::read(reader)?;
        let bitmap_z = Bitmap::read(reader)?;

        // read sequences
        let sequence_y = Sequence::read(reader)?;
        let sequence_z = Sequence::read(reader)?;

        // construct adjacency lists
        let adjlist_y = AdjList::new(sequence_y, bitmap_y);
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);

        Ok(TriplesBitmap { order, adjlist_y, adjlist_z })
    }
}

impl IntoIterator for TriplesBitmap {
    type Item = TripleId;
    type IntoIter = BitmapIter;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::new(self)
    }
}

pub struct BitmapIter {
    // triples data
    triples: TriplesBitmap,

    // x-coordinate identifier
    x: usize,

    // current position
    pos_y: usize,
    pos_z: usize,
    max_y: usize,
    max_z: usize,
}

impl BitmapIter {
    pub fn new(triples: TriplesBitmap) -> Self {
        BitmapIter {
            x: 1, // was 0 in the old code but it should start at 1
            pos_y: 0,
            pos_z: 0,
            max_y: triples.adjlist_y.get_max(),
            max_z: triples.adjlist_z.get_max(),
            triples,
        }
    }

    pub fn with_s(triples: TriplesBitmap, subject_id: usize) -> Self {
        let min_y = triples.adjlist_y.find(subject_id - 1);
        let min_z = triples.adjlist_z.find(min_y);
        let max_y = triples.adjlist_y.last(subject_id - 1) + 1;
        let max_z = triples.adjlist_z.find(max_y);
        println!(
            "BitMapIter::with_s subject_id={} min_y={} max_y={} min_z={} max_z={}",
            subject_id, min_y, max_y, min_z, max_z
        );
        BitmapIter { triples, x: subject_id, pos_y: min_y, pos_z: min_z, max_y, max_z }
    }

    fn coord_to_triple(&self, x: usize, y: usize, z: usize) -> io::Result<TripleId> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        if x == 0 || y == 0 || z == 0 {
            return Err(Error::new(
                InvalidData,
                format!("({},{},{}) none of the components of a triple may be 0.", x, y, z),
            ));
        }
        match self.triples.order {
            Order::SPO => Ok(TripleId::new(x, y, z)),
            Order::SOP => Ok(TripleId::new(x, z, y)),
            Order::PSO => Ok(TripleId::new(y, x, z)),
            Order::POS => Ok(TripleId::new(y, z, x)),
            Order::OSP => Ok(TripleId::new(z, x, y)),
            Order::OPS => Ok(TripleId::new(z, y, x)),
            Order::Unknown => Err(Error::new(InvalidData, "unknown triples order")),
        }
    }
}

impl Iterator for BitmapIter {
    type Item = TripleId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_y >= self.max_y {
            return None;
        }

        if self.pos_z >= self.max_z {
            return None;
        }

        //println!("{} pos {} {} ",self.x,self.pos_y,self.pos_z);
        let y = self.triples.adjlist_y.get_id(self.pos_y);
        let z = self.triples.adjlist_z.get_id(self.pos_z);
        //println!("x y z {} {} {}", self.x,y,z);
        //println!("{} {}", self.triples.adjlist_y.at_last_sibling(self.pos_y), self.triples.adjlist_z.at_last_sibling(self.pos_z));
        let triple_id = self.coord_to_triple(self.x, y, z).unwrap();

        // theoretically the second condition should only be true if the first is as well but in practise it wasn't, which screwed up the subject identifiers
        // fixed by moving the second condition inside the first one but there may be another reason for the bug occuring in the first place
        if self.triples.adjlist_z.at_last_sibling(self.pos_z) {
            if self.triples.adjlist_y.at_last_sibling(self.pos_y) {
                self.x += 1;
            }
            self.pos_y += 1;
        }
        //if ! self.triples.adjlist_y.at_last_sibling(self.pos_y) && (!self.triples.adjlist_z.at_last_sibling(self.pos_z)) {self.x-=1;}

        self.pos_z += 1;

        Some(triple_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TripleId {
    pub subject_id: usize,
    pub predicate_id: usize,
    pub object_id: usize,
}

impl TripleId {
    pub fn new(subject_id: usize, predicate_id: usize, object_id: usize) -> Self {
        TripleId { subject_id, predicate_id, object_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControlInfo, Dict, Header};
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_triples() {
        let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        let _header = Header::read(&mut reader).unwrap();
        Dict::read(&mut reader).unwrap();
        let triples = TripleSect::read(&mut reader).unwrap();
        let v: Vec<TripleId> = triples.read_all_ids().into_iter().collect::<Vec<TripleId>>();
        assert_eq!(v.len(), 242256);
        println!("{:#?}", &v[0..30]);
        assert_eq!(v[0].subject_id, 1);
        assert_eq!(v[2].subject_id, 1);
        assert_eq!(v[3].subject_id, 2);
    }
}
