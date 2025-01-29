use crate::containers::{AdjList, Bitmap, Sequence};
use crate::ControlInfo;
use bytesize::ByteSize;
use eyre::{eyre, Result, WrapErr};
use log::{debug, error};
use serde::de::{self, Deserializer};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::io::BufRead;
use std::io::BufReader;
use sucds::{
    bit_vectors::{BitVector, Rank9Sel},
    char_sequences::WaveletMatrix,
    int_vectors::CompactVector,
    Serializable,
};

mod subject_iter;
pub use subject_iter::SubjectIter;
mod predicate_iter;
pub use predicate_iter::PredicateIter;
mod predicate_object_iter;
pub use predicate_object_iter::PredicateObjectIter;
mod object_iter;
pub use object_iter::ObjectIter;

/// Order of the triple sections.
/// Only SPO is tested, others probably don't work correctly.
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    type Error = eyre::Error;

    fn try_from(original: u32) -> Result<Self> {
        match original {
            0 => Ok(Order::Unknown),
            1 => Ok(Order::SPO),
            2 => Ok(Order::SOP),
            3 => Ok(Order::PSO),
            4 => Ok(Order::POS),
            5 => Ok(Order::OSP),
            6 => Ok(Order::OPS),
            _ => Err(eyre!("Unrecognized order")),
        }
    }
}

/// Inverse index from object id to positions in the object adjacency list.
/// Used for logarithmic (?) time access instead of linear time sequential search.
pub struct OpIndex {
    /// Compact integer vector of object positions.
    pub sequence: CompactVector,
    /// Bitmap with a one bit for every new object to allow finding the starting point for a given object id.
    pub bitmap: Bitmap,
}

impl Serialize for OpIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OpIndex", 2)?;

        // Serialize sequence using `sucds`
        let mut seq_buffer = Vec::new();
        self.sequence.serialize_into(&mut seq_buffer).map_err(serde::ser::Error::custom)?;
        state.serialize_field("sequence", &seq_buffer)?;

        state.serialize_field("bitmap", &self.bitmap)?;

        state.end()
    }
}

impl<'de> Deserialize<'de> for OpIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct OpIndexData {
            sequence: Vec<u8>,
            bitmap: Bitmap,
        }

        let data = OpIndexData::deserialize(deserializer)?;

        // Deserialize `sucds` structures
        let mut seq_reader = BufReader::new(&data.sequence[..]);

        let v = CompactVector::deserialize_from(&mut seq_reader).map_err(de::Error::custom)?;
        let index = OpIndex { sequence: v, bitmap: data.bitmap }; // Replace with proper reconstruction

        Ok(index)
    }
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
    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.len() * self.sequence.width() / 8 + self.bitmap.size_in_bytes()
    }
    /// Find the first position in the OP index of the given object ID.
    pub fn find(&self, o: Id) -> usize {
        self.bitmap.select1(o - 1).unwrap() as usize
    }
    /// Find the last position in the object index of the given object ID.
    pub fn last(&self, o: Id) -> usize {
        match self.bitmap.select1(o) {
            Some(index) => index as usize - 1,
            None => self.bitmap.len() - 1,
        }
    }
}

/// `BitmapTriples` variant of the triples section.
//#[derive(Clone)]
pub struct TriplesBitmap {
    order: Order,
    /// bitmap to find positions in the wavelet matrix
    pub bitmap_y: Bitmap,
    /// adjacency list storing the object IDs
    pub adjlist_z: AdjList,
    /// Index for object-based access. Points to the predicate layer.
    pub op_index: OpIndex,
    /// wavelet matrix for predicate-based access
    pub wavelet_y: WaveletMatrix<Rank9Sel>,
}

impl fmt::Debug for TriplesBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {}", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(f, "adjlist_z {:#?}", self.adjlist_z)?;
        writeln!(f, "op_index {:#?}", self.op_index)?;
        write!(f, "wavelet_y {}", ByteSize(self.wavelet_y.size_in_bytes() as u64))
    }
}

impl Serialize for TriplesBitmap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TriplesBitmap", 5)?;

        // Extract the number of triples
        state.serialize_field("order", &self.order)?;

        //bitmap_y
        state.serialize_field("bitmap_y", &self.bitmap_y)?;

        // adjlist_z
        state.serialize_field("adjlist_z", &self.adjlist_z)?;

        // op_index
        state.serialize_field("op_index", &self.op_index)?;

        // wavelet_y
        let mut wavelet_y_buffer = Vec::new();
        self.wavelet_y.serialize_into(&mut wavelet_y_buffer).map_err(serde::ser::Error::custom)?;
        state.serialize_field("wavelet_y", &wavelet_y_buffer)?;

        state.end()
    }
}

impl<'de> Deserialize<'de> for TriplesBitmap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct TriplesBitmapData {
            order: Order,
            pub bitmap_y: Bitmap,
            pub adjlist_z: AdjList,
            pub op_index: OpIndex,
            pub wavelet_y: Vec<u8>,
        }

        let data = TriplesBitmapData::deserialize(deserializer)?;

        // Deserialize `sucds` structures
        let mut bitmap_reader = BufReader::new(&data.wavelet_y[..]);
        let wavelet_y =
            WaveletMatrix::<Rank9Sel>::deserialize_from(&mut bitmap_reader).map_err(de::Error::custom)?;

        let bitmap = TriplesBitmap {
            order: data.order,
            bitmap_y: data.bitmap_y,
            adjlist_z: data.adjlist_z,
            op_index: data.op_index,
            wavelet_y,
        };

        Ok(bitmap)
    }
}

impl TriplesBitmap {
    /// read the whole triple section including control information
    pub fn read_sect<R: BufRead>(reader: &mut R) -> Result<Self> {
        let triples_ci = ControlInfo::read(reader)?;

        match &triples_ci.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => TriplesBitmap::read(reader, &triples_ci),
            "<http://purl.org/HDT/hdt#triplesList>" => Err(eyre!("Triples Lists are not supported yet.")),
            _ => Err(eyre!("Unknown triples listing format.")),
        }
    }
    pub fn load_cache<R: BufRead>(reader: &mut R, info: ControlInfo) -> Result<Self> {
        match &info.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => TriplesBitmap::load(reader),
            "<http://purl.org/HDT/hdt#triplesList>" => Err(eyre!("Triples Lists are not supported yet.")),
            _ => Err(eyre!("Unknown triples listing format.")),
        }
    }

    pub fn load<R: BufRead>(reader: &mut R) -> Result<Self> {
        let triples: TriplesBitmap = bincode::deserialize_from(reader).expect("msg");
        Ok(triples)
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.adjlist_z.size_in_bytes() + self.op_index.size_in_bytes() + self.wavelet_y.size_in_bytes()
    }

    /// Position in the wavelet index of the first predicate for the given subject ID.
    pub fn find_y(&self, subject_id: Id) -> usize {
        if subject_id == 0 {
            return 0;
        }
        self.bitmap_y.select1(subject_id - 1).unwrap() as usize + 1
    }

    /// Position in the wavelet index of the last predicate for the given subject ID.
    pub fn last_y(&self, subject_id: usize) -> usize {
        self.find_y(subject_id + 1) - 1
    }

    /// Binary search in the wavelet matrix.
    fn bin_search_y(&self, element: usize, begin: usize, end: usize) -> Option<usize> {
        let mut low = begin;
        let mut high = end;

        while low < high {
            let mid = (low + high) / 2;
            match self.wavelet_y.access(mid).unwrap().cmp(&element) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Some(mid),
            };
        }
        None
    }

    /// Search the wavelet matrix for the position of a given subject, predicate pair.
    pub fn search_y(&self, subject_id: usize, property_id: usize) -> Option<usize> {
        self.bin_search_y(property_id, self.find_y(subject_id), self.last_y(subject_id) + 1)
    }

    fn build_wavelet(mut sequence: Sequence) -> WaveletMatrix<Rank9Sel> {
        debug!("Building wavelet matrix...");
        let mut builder =
            CompactVector::new(sequence.bits_per_entry).expect("Failed to create wavelet matrix builder");
        // possible refactor of Sequence to use sucds CompactVector, then builder can be removed
        for x in &sequence {
            builder.push_int(x).unwrap();
        }
        assert!(sequence.crc_handle.take().unwrap().join().unwrap(), "Wavelet source CRC check failed.");
        drop(sequence);
        let wavelet = WaveletMatrix::new(builder).expect("Error building the wavelet matrix. Aborting.");
        debug!("Built wavelet matrix with length {}", wavelet.len());
        wavelet
    }

    /*
        /// Get the predicate ID for the given z index position.
    fn get_p(bitmap_z:  Bitmap, wavelet_y: WaveletMatrix, pos_z: usize) -> Id {
                let pos_y = bitmap_z.dict.rank(pos_z, true);
                wavelet_y.get(pos_y as usize) as Id
    }
    */

    fn read<R: BufRead>(reader: &mut R, triples_ci: &ControlInfo) -> Result<Self> {
        // read order
        let order: Order;
        if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            order = Order::try_from(n)?;
        } else {
            return Err(eyre!("Unrecognized order"));
        }

        // read bitmaps
        let bitmap_y = Bitmap::read(reader).wrap_err("Failed to read Y level bitmap")?;
        let bitmap_z = Bitmap::read(reader).wrap_err("Failed to read Z level bitmap")?;

        // read sequences
        let sequence_y = Sequence::read(reader)?;
        let wavelet_thread = std::thread::spawn(|| Self::build_wavelet(sequence_y));
        let mut sequence_z = Sequence::read(reader)?;

        // construct adjacency lists
        // construct object-based index to traverse from the leaves and support ??O and ?PO queries
        debug!("Building OPS index...");
        let entries = sequence_z.entries;
        // if it takes too long to calculate, can also pass in as parameter
        let max_object = sequence_z.into_iter().max().unwrap().to_owned();
        // limited to < 2^32 objects
        let mut indicess = vec![Vec::<u32>::with_capacity(4); max_object];

        // Count the indexes of appearance of each object
        // In https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/triples/BitmapTriples.cpp
        // they count the number of appearances in a sequence instead, which saves memory
        // temporarily but they need to loop over it an additional time.
        for pos_z in 0..entries {
            let object = sequence_z.get(pos_z);
            if object == 0 {
                error!("ERROR: There is a zero value in the Z level.");
                continue;
            }
            let pos_y = bitmap_z.rank(pos_z.to_owned());
            indicess[object - 1].push(pos_y as u32); // hdt index counts from 1 but we count from 0 for simplicity
        }
        // reduce memory consumption of index by using adjacency list
        let mut bitmap_index_bitvector = BitVector::new();
        let mut cv = CompactVector::with_capacity(entries, sucds::utils::needed_bits(entries))
            .map_err(|err| eyre!(Box::new(err)))?;
        let wavelet_y = wavelet_thread.join().unwrap();
        /*
        let get_p = |pos_z: u32| {
            let pos_y = bitmap_z.dict.rank(pos_z.to_owned() as u64, true);
            wavelet_y.access(pos_y as usize).unwrap() as Id
        };
        */
        for mut indices in indicess {
            let mut first = true;
            // sort by predicate
            indices.sort_by_cached_key(|pos_y| wavelet_y.access(*pos_y as usize).unwrap());
            for index in indices {
                bitmap_index_bitvector.push_bit(first);
                first = false;
                cv.push_int(index as usize).unwrap();
            }
        }
        let bitmap_index = Bitmap { dict: Rank9Sel::new(bitmap_index_bitvector) };
        let op_index = OpIndex { sequence: cv, bitmap: bitmap_index };
        debug!("built OPS index");
        assert!(sequence_z.crc_handle.take().unwrap().join().unwrap(), "sequence_z CRC check failed.");
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);
        Ok(TriplesBitmap { order, bitmap_y, adjlist_z, op_index, wavelet_y })
    }

    /// Transform the given IDs of the layers in triple section order to a triple ID.
    /// Warning: At the moment only SPO is properly supported anyways, in which case this is equivalent to `TripleId::new(x,y,z)`.
    /// Other orders may lead to undefined behaviour.
    pub fn coord_to_triple(&self, x: Id, y: Id, z: Id) -> Result<TripleId> {
        if x == 0 || y == 0 || z == 0 {
            return Err(eyre!(format!("({x},{y},{z}) none of the components of a triple may be 0."),));
        }
        match self.order {
            Order::SPO => Ok(TripleId::new(x, y, z)),
            Order::SOP => Ok(TripleId::new(x, z, y)),
            Order::PSO => Ok(TripleId::new(y, x, z)),
            Order::POS => Ok(TripleId::new(y, z, x)),
            Order::OSP => Ok(TripleId::new(z, x, y)),
            Order::OPS => Ok(TripleId::new(z, y, x)),
            Order::Unknown => Err(eyre!("unknown triples order")),
        }
    }
}

impl<'a> IntoIterator for &'a TriplesBitmap {
    type Item = TripleId;
    type IntoIter = SubjectIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SubjectIter::new(self)
    }
}

/// Subject, predicate or object ID, starting at 1.
///
/// Subjects and predicate share IDs, starting at 1, for common values.
/// A value of 0 indicates either not found (as a return value) or all of them (in a triple pattern).
/// In the official documentation, u32 is used, however here, usize is used.
/// While u32 caps out at 4 billion, more is not supported by the format anyways so this can probably be changed to u32.
pub type Id = usize;

/// Type for a triple encoded as numeric IDs for subject, predicate and object, respectively.
/// See <https://www.rdfhdt.org/hdt-binary-format/#triples>.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TripleId {
    /// Index starting at 1 in the combined shared and subject section.
    pub subject_id: Id,
    /// Index starting at 1 in the predicate section.
    pub predicate_id: Id,
    /// Index starting at 1 in the combined shared and object section.
    pub object_id: Id,
}

impl TripleId {
    /// Create a new triple ID.
    pub const fn new(subject_id: Id, predicate_id: Id, object_id: Id) -> Self {
        TripleId { subject_id, predicate_id, object_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::tests::init;
    use crate::{FourSectDict, IdKind};
    use pretty_assertions::assert_eq;
    use std::fs::File;
    use std::io::BufReader;

    /// Iterator over all triples with a given ID in the specified position (subject, predicate or object).
    fn triples_with_id<'a>(
        t: &'a TriplesBitmap, id: usize, k: &IdKind,
    ) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        match k {
            IdKind::Subject => Box::new(SubjectIter::with_s(t, id)),
            IdKind::Predicate => Box::new(PredicateIter::new(t, id)),
            IdKind::Object => Box::new(ObjectIter::new(t, id)),
        }
    }

    #[test]
    fn read_triples() {
        init();
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();
        let _dict = FourSectDict::read(&mut reader).unwrap();
        let triples = TriplesBitmap::read_sect(&mut reader).unwrap();
        let v: Vec<TripleId> = triples.into_iter().collect::<Vec<TripleId>>();
        assert_eq!(v.len(), 328);
        assert_eq!(v[0].subject_id, 1);
        assert_eq!(v[2].subject_id, 1);
        assert_eq!(v[3].subject_id, 2);
        let num_subjects = 48;
        let num_predicates = 23;
        let num_objects = 175;
        let mut filtered: Vec<TripleId>;
        let kinds = [IdKind::Subject, IdKind::Predicate, IdKind::Object];
        let lens = [num_subjects, num_predicates, num_objects];
        let funs = [|t: TripleId| t.subject_id, |t: TripleId| t.predicate_id, |t: TripleId| t.object_id];
        for j in 0..kinds.len() {
            for i in 1..=lens[j] {
                filtered = v.iter().filter(|tid| funs[j](**tid) == i).copied().collect();
                filtered.sort_unstable();
                let mut triples_with_id = triples_with_id(&triples, i, &kinds[j]).collect::<Vec<TripleId>>();
                triples_with_id.sort_unstable();
                assert_eq!(filtered, triples_with_id, "triples_with({},{:?})", i, kinds[j]);
            }
        }

        // SubjectIter
        assert_eq!(0, SubjectIter::empty(&triples).count());
        // SPO
        assert_eq!(
            vec![TripleId::new(14, 14, 154)],
            SubjectIter::with_pattern(&triples, &TripleId::new(14, 14, 154)).collect::<Vec<_>>()
        );
        // SP
        assert_eq!(
            vec![TripleId::new(14, 14, 154)],
            SubjectIter::with_pattern(&triples, &TripleId::new(14, 14, 0)).collect::<Vec<_>>()
        );
        // S??
        for i in 1..num_subjects {
            assert_eq!(
                SubjectIter::with_s(&triples, i).collect::<Vec<_>>(),
                SubjectIter::with_pattern(&triples, &TripleId::new(i, 0, 0)).collect::<Vec<_>>()
            );
        }
        // ??? (all triples)
        assert_eq!(v, SubjectIter::with_pattern(&triples, &TripleId::new(0, 0, 0)).collect::<Vec<_>>());
        // SP? where S and P are in the graph, but not together
        assert_eq!(0, SubjectIter::with_pattern(&triples, &TripleId::new(12, 14, 154)).count());
    }
}
