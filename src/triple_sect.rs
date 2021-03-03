use crate::bitmap::Bitmap;
use crate::sequence::Sequence;
use crate::vbyte::read_vbyte;
use crate::ControlInfo;
use crc_any::{CRCu32, CRCu8};
use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub enum TripleSect {
    Bitmap(TriplesBitmap),
    // List(TriplesList),
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
            "<http://purl.org/HDT/hdt#triplesList>" => Err(Error::new(
                InvalidData,
                "Triples Lists are not supported yet.",
            )),
            _ => Err(Error::new(InvalidData, "Unknown triples listing format.")),
        }
    }

    pub fn read_all_ids(&mut self) -> BTreeSet<TripleId> {
        match self {
            TripleSect::Bitmap(bitmap) => BTreeSet::new(),
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
            _ => Err(Self::Error::new(
                io::ErrorKind::InvalidData,
                "Unrecognized order",
            )),
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
    fn read<R: BufRead>(reader: &mut R, triples_ci: ControlInfo) -> io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind::InvalidData;

        // read order
        let mut order: Order;
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

        Ok(TriplesBitmap {
            order,
            adjlist_y,
            adjlist_z,
        })
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
    // current position
    pos_y: usize,
    pos_z: usize,
    // maximum
    max_y: usize,
    max_z: usize,
}

impl BitmapIter {
    pub fn new(triples: TriplesBitmap) -> Self {
        let pos_z = 0;
        let pos_y = triples.adjlist_z.find_index(pos_z);
        let max_y = triples.adjlist_y.sequence.entries;
        let max_z = triples.adjlist_z.sequence.entries;

        BitmapIter {
            triples,
            pos_y,
            pos_z,
            max_y,
            max_z,
        }
    }
}

impl Iterator for BitmapIter {
    type Item = TripleId;

    fn next(&mut self) -> Option<Self::Item> {
        let subject_id = self.triples.adjlist_z.get(self.pos_z)?;
        let predicate_id = self.triples.adjlist_y.get(self.pos_y)?;
        let object_id = self.triples.adjlist_y.find_index(self.pos_y) + 1;

        self.pos_y = self.triples.adjlist_y.last(subject_id - 1) + 1;
        self.pos_z = self.triples.adjlist_z.last(self.pos_y) + 1;

        Some(TripleId::new(subject_id, predicate_id, object_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TripleId {
    subject_id: usize,
    predicate_id: usize,
    object_id: usize,
}

impl TripleId {
    pub fn new(subject_id: usize, predicate_id: usize, object_id: usize) -> Self {
        TripleId {
            subject_id,
            predicate_id,
            object_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdjList {
    sequence: Sequence,
    bitmap: Bitmap,
    subject_id: usize,
}

impl AdjList {
    fn new(sequence: Sequence, bitmap: Bitmap) -> Self {
        AdjList {
            sequence,
            bitmap,
            subject_id: 0,
        }
    }

    fn find_index(&self, global_pos: usize) -> usize {
        self.bitmap.rank1(global_pos - 1)
    }

    fn get(&self, pos: usize) -> Option<usize> {
        self.sequence.get(pos)
    }

    fn last(&self, pos: usize) -> usize {
        self.bitmap.select1(pos + 1)
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
        Header::read(&mut reader).unwrap();
        Dict::read(&mut reader).unwrap();
        let triples = TripleSect::read(&mut reader).unwrap();
    }
}
