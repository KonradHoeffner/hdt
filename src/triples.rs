use crate::containers::{AdjList, Bitmap, Sequence};
use crate::object_iter::ObjectIter;
use crate::predicate_iter::PredicateIter;
use crate::{ControlInfo, IdKind};
use bytesize::ByteSize;
use rsdict::RsDict;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::io::BufRead;
use sucds::{CompactVector, Searial, WaveletMatrix, WaveletMatrixBuilder};

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

pub struct OpIndex {
    pub sequence: CompactVector,
    pub bitmap: Bitmap,
}

impl fmt::Debug for OpIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {} {{", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(
            f,
            "    sequence: {} with {} bits,",
            ByteSize(self.sequence.len() as u64 * self.sequence.width() as u64 / 8),
            self.sequence.width()
        )?;
        write!(f, "    bitmap: {:#?}\n}}", self.bitmap)
    }
}

impl OpIndex {
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.len() * self.sequence.width() / 8 + self.bitmap.size_in_bytes()
    }
    pub fn find(&self, o: usize) -> usize {
        self.bitmap.dict.select1(o as u64 - 1).unwrap() as usize
    }
    pub fn last(&self, o: usize) -> usize {
        match self.bitmap.dict.select1(o as u64) {
            Some(index) => index as usize - 1,
            None => self.bitmap.dict.len() - 1,
        }
    }
}

//#[derive(Clone)]
pub struct TriplesBitmap {
    order: Order,
    pub bitmap_y: Bitmap,
    pub adjlist_z: AdjList,
    pub op_index: OpIndex,
    pub wavelet_y: WaveletMatrix,
}

impl fmt::Debug for TriplesBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {}", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(f, "adjlist_z {:#?}", self.adjlist_z)?;
        writeln!(f, "op_index {:#?}", self.op_index)?;
        write!(f, "wavelet_y {}", ByteSize(self.wavelet_y.size_in_bytes() as u64))
    }
}

impl TriplesBitmap {
    pub fn read_sect<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        let triples_ci = ControlInfo::read(reader)?;

        match &triples_ci.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => Ok(TriplesBitmap::read(reader, &triples_ci)?),
            "<http://purl.org/HDT/hdt#triplesList>" => {
                Err(Error::new(InvalidData, "Triples Lists are not supported yet."))
            }
            _ => Err(Error::new(InvalidData, "Unknown triples listing format.")),
        }
    }

    pub fn triples_with_id(&self, id: usize, id_kind: &IdKind) -> Box<dyn Iterator<Item = TripleId> + '_> {
        match id_kind {
            IdKind::Subject => Box::new(BitmapIter::with_s(self, id)),
            IdKind::Predicate => Box::new(PredicateIter::new(self, id)),
            IdKind::Object => Box::new(ObjectIter::new(self, id)),
        }
    }

    pub fn size_in_bytes(&self) -> usize {
        self.adjlist_z.size_in_bytes() + self.op_index.size_in_bytes() + self.wavelet_y.size_in_bytes()
    }

    pub fn find_y(&self, subject_id: usize) -> usize {
        if subject_id == 0 {
            return 0;
        }
        self.bitmap_y.dict.select1(subject_id as u64 - 1).unwrap() as usize + 1
    }

    pub fn last_y(&self, subject_id: usize) -> usize {
        self.find_y(subject_id + 1) - 1
    }

    /// binary search in the wavelet matrix
    fn bin_search_y(&self, element: usize, begin: usize, end: usize) -> Option<usize> {
        println!("searching for element {element} between {begin} and {end}");

        let mut low = begin;
        let mut high = end;

        for i in low..high {
            println!("{}", self.wavelet_y.get(i));
        }

        while low <= high {
            let mid = low + high / 2;
            match self.wavelet_y.get(mid).cmp(&element) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Some(mid),
            };
        }
        None
    }

    /// search the wavelet matrix for the position of a given subject, predicate pair
    pub fn search_y(&self, subject_id: usize, property_id: usize) -> Option<usize> {
        println!("searching for subject id {subject_id} and property_id {property_id}");
        self.bin_search_y(property_id, self.find_y(subject_id), self.last_y(subject_id))
    }

    fn read<R: BufRead>(reader: &mut R, triples_ci: &ControlInfo) -> io::Result<Self> {
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
        // generate wavelet matrix early to reduce memory peak consumption
        print!("Start constructing wavelet matrix");
        let mut wavelet_builder = WaveletMatrixBuilder::with_width(sequence_y.bits_per_entry);
        for x in &sequence_y {
            wavelet_builder.push(x);
        }
        drop(sequence_y);
        let wavelet_y = wavelet_builder.build().expect("Error building the wavelet matrix. Aborting.");
        println!("...finished constructing wavelet matrix with length {}", wavelet_y.len());
        let sequence_z = Sequence::read(reader)?;

        // construct adjacency lists
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);

        // construct object-based index to traverse from the leaves and support O?? queries
        print!("Constructing OPS index");
        let entries = adjlist_z.sequence.entries;
        // if it takes too long to calculate, can also pass in as parameter
        let max_object = adjlist_z.sequence.into_iter().max().unwrap().to_owned();
        // limited to < 2^32 objects
        let mut indicess = vec![Vec::<u32>::new(); max_object];

        // Count the indexes of appearance of each object
        for i in 0..entries {
            let object = adjlist_z.sequence.get(i);
            if object == 0 {
                eprintln!("ERROR: There is a zero value in the Z level.");
                continue;
            }
            indicess[object - 1].push(i as u32); // hdt index counts from 1 but we count from 0 for simplicity
        }
        // reduce memory consumption of index by using adjacency list
        let mut bitmap_index_dict = RsDict::new();
        let mut cv = CompactVector::with_capacity(entries, sucds::util::needed_bits(entries));
        for mut indices in indicess {
            let mut first = true;
            indices.sort_unstable();
            for index in indices {
                bitmap_index_dict.push(first);
                first = false;
                cv.push(index as usize);
            }
        }
        let bitmap_index = Bitmap { dict: bitmap_index_dict };
        let op_index = OpIndex { sequence: cv, bitmap: bitmap_index };
        println!("...finished constructing OPS index");

        Ok(TriplesBitmap { order, bitmap_y, adjlist_z, op_index, wavelet_y })
    }

    pub fn coord_to_triple(&self, x: usize, y: usize, z: usize) -> io::Result<TripleId> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        if x == 0 || y == 0 || z == 0 {
            return Err(Error::new(
                InvalidData,
                format!("({x},{y},{z}) none of the components of a triple may be 0."),
            ));
        }
        match self.order {
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

impl<'a> IntoIterator for &'a TriplesBitmap {
    type Item = TripleId;
    type IntoIter = BitmapIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BitmapIter::new(self)
    }
}

//#[derive(Debug)]
pub struct BitmapIter<'a> {
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

impl<'a> BitmapIter<'a> {
    pub const fn new(triples: &'a TriplesBitmap) -> Self {
        BitmapIter {
            triples,
            x: 1, // was 0 in the old code but it should start at 1
            pos_y: 0,
            pos_z: 0,
            max_y: triples.wavelet_y.len(), // exclusive
            max_z: triples.adjlist_z.len(), // exclusive
        }
    }

    pub const fn empty(triples: &'a TriplesBitmap) -> Self {
        BitmapIter { triples, x: 1, pos_y: 0, pos_z: 0, max_y: 0, max_z: 0 }
    }

    /// see <https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/triples/BitmapTriplesIterators.cpp>
    pub fn with_s(triples: &'a TriplesBitmap, subject_id: usize) -> Self {
        let min_y = triples.find_y(subject_id - 1);
        let min_z = triples.adjlist_z.find(min_y);
        let max_y = triples.find_y(subject_id);
        let max_z = triples.adjlist_z.find(max_y);
        BitmapIter { triples, x: subject_id, pos_y: min_y, pos_z: min_z, max_y, max_z }
    }

    /// Iterate over triples fitting the given SPO, SP? S?? or ??? triple pattern.
    /// Variable positions are signified with a 0 value.
    /// Undefined result if any other triple pattern is used.
    /// # Examples
    /// ```ignore
    /// // S?? pattern, all triples with subject ID 1
    /// BitmapIter::with_pattern(triples, TripleId::new(1, 0, 0);
    /// // SP? pattern, all triples with subject ID 1 and predicate ID 2
    /// BitmapIter::with_pattern(triples, TripleId::new(1, 2, 0);
    /// // match a specific triple, not useful in practice
    /// BitmapIter::with_pattern(triples, TripleId::new(1, 2, 3);
    /// ```
    // Translated from <https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/triples/BitmapTriplesIterators.cpp>.
    pub fn with_pattern(triples: &'a TriplesBitmap, pat: &TripleId) -> Self {
        let (pat_x, pat_y, pat_z) = (pat.subject_id, pat.predicate_id, pat.object_id);
        let (min_y, max_y, min_z, max_z);
        let mut x = 1;
        // only SPO order is supported currently
        if pat_x != 0 {
            // S X X
            if pat_y != 0 {
                // S P X
                match triples.search_y(pat_x, pat_y) {
                    Some(y) => min_y = y,
                    None => return BitmapIter::empty(triples),
                };
                max_y = min_y + 1;
                if pat_z != 0 {
                    // S P O
                    // simply with try block when they come to stable Rust
                    match triples.adjlist_z.search(min_y, pat_z) {
                        Some(z) => min_z = z,
                        None => return BitmapIter::empty(triples),
                    };
                    max_z = min_z + 1;
                } else {
                    println!("bingo!");
                    // S P ?
                    min_z = triples.adjlist_z.find(min_y);
                    max_z = triples.adjlist_z.last(min_y) + 1;
                    println!("adjlist_z min_z ele {}", triples.adjlist_z.sequence.get(min_z));
                    println!("adjlist_z max_z ele {}", triples.adjlist_z.sequence.get(max_z));
                }
            } else {
                // S ? X
                min_y = triples.find_y(pat_x - 1);
                min_z = triples.adjlist_z.find(min_y);
                max_y = triples.last_y(pat_x - 1) + 1;
                max_z = triples.adjlist_z.find(max_y);
            }
            x = pat_x;
        } else {
            // ? X X
            // assume ? ? ?, other triple patterns are not supported by this function
            min_y = 0;
            min_z = 0;
            max_y = triples.wavelet_y.len();
            max_z = triples.adjlist_z.len();
        }
        BitmapIter { triples, x, pos_y: min_y, pos_z: min_z, max_y, max_z }
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = TripleId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_y >= self.max_y {
            return None;
        }

        if self.pos_z >= self.max_z {
            return None;
        }

        let y = self.triples.wavelet_y.get(self.pos_y);
        let z = self.triples.adjlist_z.get_id(self.pos_z);
        let triple_id = self.triples.coord_to_triple(self.x, y, z).unwrap();

        // theoretically the second condition should only be true if the first is as well but in practise it wasn't, which screwed up the subject identifiers
        // fixed by moving the second condition inside the first one but there may be another reason for the bug occuring in the first place
        if self.triples.adjlist_z.at_last_sibling(self.pos_z) {
            if self.triples.bitmap_y.at_last_sibling(self.pos_y) {
                self.x += 1;
            }
            self.pos_y += 1;
        }
        self.pos_z += 1;
        println!("bitmap iter next {triple_id:?} ");
        Some(triple_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TripleId {
    pub subject_id: usize,
    pub predicate_id: usize,
    pub object_id: usize,
}

impl TripleId {
    pub const fn new(subject_id: usize, predicate_id: usize, object_id: usize) -> Self {
        TripleId { subject_id, predicate_id, object_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::{ControlInfo, FourSectDict, IdKind};
    use pretty_assertions::assert_eq;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_triples() {
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();
        let _dict = FourSectDict::read(&mut reader).unwrap();
        let triples = TriplesBitmap::read_sect(&mut reader).unwrap();
        let v: Vec<TripleId> = triples.into_iter().collect::<Vec<TripleId>>();
        assert_eq!(v.len(), 327);
        assert_eq!(v[0].subject_id, 1);
        assert_eq!(v[2].subject_id, 1);
        assert_eq!(v[3].subject_id, 2);
        let num_subjects = 48;
        let num_predicates = 23;
        let num_objects = 175;
        // theorectially order doesn't matter so should derive Hash for TripleId and use HashSet but not needed in practice
        let mut filtered: Vec<TripleId>;
        let kinds = [IdKind::Subject, IdKind::Predicate, IdKind::Object];
        let lens = [num_subjects, num_predicates, num_objects];
        let funs = [|t: TripleId| t.subject_id, |t: TripleId| t.predicate_id, |t: TripleId| t.object_id];
        for j in 0..kinds.len() {
            for i in 1..=lens[j] {
                filtered = v.iter().filter(|tid| funs[j](**tid) == i).copied().collect();
                assert_eq!(
                    filtered,
                    triples.triples_with_id(i, &kinds[j]).collect::<Vec<TripleId>>(),
                    "triples_with({},{:?})",
                    i,
                    kinds[j]
                );
            }
        }
        assert_eq!(0, BitmapIter::empty(&triples).count());
    }
}
