use crate::ControlInfo;
use crate::containers::{AdjList, Bitmap, Sequence, bitmap, control_info, sequence};
use bytesize::ByteSize;
use log::error;
use qwt::QWT512;
use qwt::{AccessUnsigned, BitVector, BitVectorMut, SpaceUsage, bitvector::rs_narrow::RSNarrow};
use std::cmp::Ordering;
use std::fmt;
use std::io::BufRead;

mod subject_iter;
pub use subject_iter::SubjectIter;
mod predicate_iter;
pub use predicate_iter::PredicateIter;
mod predicate_object_iter;
pub use predicate_object_iter::PredicateObjectIter;
mod object_iter;
pub use object_iter::ObjectIter;
#[cfg(feature = "cache")]
use serde::{self, Deserialize, Serialize};

pub type Result<T> = core::result::Result<T, Error>;

/// Order of the triple sections.
/// Only SPO is tested, others probably don't work correctly.
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "cache", derive(serde::Deserialize, serde::Serialize))]
pub enum Order {
    #[default]
    Unknown = 0,
    SPO = 1,
    SOP = 2,
    PSO = 3,
    POS = 4,
    OSP = 5,
    OPS = 6,
}

impl TryFrom<u32> for Order {
    type Error = Error;

    fn try_from(original: u32) -> Result<Self> {
        match original {
            0 => Ok(Order::Unknown),
            1 => Ok(Order::SPO),
            2 => Ok(Order::SOP),
            3 => Ok(Order::PSO),
            4 => Ok(Order::POS),
            5 => Ok(Order::OSP),
            6 => Ok(Order::OPS),
            n => Err(Error::UnrecognizedTriplesOrder(n)),
        }
    }
}

/// Inverse index from object id to positions in the object adjacency list.
/// This object-based index allows to traverse from the leaves and support ??O and ?PO queries.
/// Used for logarithmic (?) time access instead of linear time sequential search.
/// See Martínez-Prieto, M., M. Arias, and J. Fernández (2012). Exchange and Consumption of Huge RDF Data. Pages 8--10.
#[cfg_attr(feature = "cache", derive(Deserialize, Serialize))]
pub struct OpIndex {
    /// Compact integer vector of object positions.
    /// "[...] integer sequence: SoP, which stores, for each object, a sorted list of references to the predicate-subject pairs (sorted by predicate) related to it."
    // we don't use our own Sequence type because CompactVector is easier to construct step by step (should we rename it but if yes to what?)
    // update: as we replaced sucds with QWT, we need to use our own type for now.
    pub sequence: Sequence,
    /// Bitmap with a one bit for every new object to allow finding the starting point for a given object id.
    pub bitmap: Bitmap,
}

impl fmt::Debug for OpIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {} {{", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(
            f,
            "    sequence: {} with {} bits,",
            ByteSize(self.sequence.size_in_bytes() as u64),
            self.sequence.bits_per_entry
        )?;
        write!(f, "    bitmap: {:#?}\n}}", self.bitmap)
    }
}

impl OpIndex {
    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        //self.sequence.len() * self.sequence.width() / 8 + self.bitmap.size_in_bytes()
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }
    /// Find the first position in the OP index of the given object ID.
    pub fn find(&self, o: Id) -> usize {
        self.bitmap.select1(o - 1).unwrap() as usize
    }
    /// Find the last position in the object index of the given object ID.
    pub fn last(&self, o: Id) -> usize {
        self.bitmap.select1(o).map_or_else(|| self.bitmap.len() - 1, |index| index as usize - 1)
    }
}

type WT = QWT512<usize>;

/// `BitmapTriples` variant of the triples section.
//#[derive(Clone)]
#[cfg_attr(feature = "cache", derive(Serialize, Deserialize))]
pub struct TriplesBitmap {
    order: Order,
    /// bitmap to find positions in the wavelet matrix
    pub bitmap_y: Bitmap,
    /// adjacency list storing the object IDs
    pub adjlist_z: AdjList,
    /// Index for object-based access. Points to the predicate layer.
    pub op_index: OpIndex,
    /// wavelet matrix for predicate-based access
    pub wavelet_y: WT,
}

#[derive(Debug)]
pub enum Level {
    Y,
    Z,
}

/// The error type for the triples bitmap read and write function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to read control info")]
    ControlInfo(#[from] control_info::Error),
    #[error("bitmap error in the {0:?} level")]
    Bitmap(Level, #[source] bitmap::Error),
    #[error("sequence read error")]
    Sequence(Level, #[source] sequence::Error),
    #[error("unspecified triples order")]
    UnspecifiedTriplesOrder,
    #[error("unknown triples order")]
    UnknownTriplesOrder,
    #[error("unrecognized triples order {0}")]
    UnrecognizedTriplesOrder(u32),
    #[error("unknown triples format {0}")]
    UnknownTriplesFormat(String),
    #[error("triple lists are not supported yet")]
    TriplesList,
    #[error("({0},{1},{2}) none of the components of a triple may be 0.")]
    TripleComponentZero(usize, usize, usize),
    #[error("unspecified external library error")]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("cache decode error")]
    #[cfg(feature = "cache")]
    Decode(#[from] bincode::error::DecodeError),
}

impl fmt::Debug for TriplesBitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "total size {}", ByteSize(self.size_in_bytes() as u64))?;
        writeln!(f, "adjlist_z {:#?}", self.adjlist_z)?;
        writeln!(f, "op_index {:#?}", self.op_index)?;
        write!(f, "wavelet_y {}", ByteSize(self.wavelet_y.space_usage_byte() as u64))
    }
}

impl TriplesBitmap {
    /// builds the necessary indexes and constructs TriplesBitmap
    pub fn new(order: Order, sequence_y: &Sequence, bitmap_y: Bitmap, adjlist_z: AdjList) -> Self {
        //let wavelet_thread = std::thread::spawn(move || WT::from_iter(&sequence_y));
        let wavelet_y = WT::from_iter(sequence_y);

        let entries = adjlist_z.sequence.entries;
        // if it takes too long to calculate, can also pass in as parameter
        let max_object = adjlist_z.sequence.into_iter().max().unwrap_or(0).to_owned();
        // limited to < 2^32 objects
        let mut indicess = vec![Vec::<u32>::with_capacity(4); max_object];
        // Count the indexes of appearance of each object
        // In https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/triples/BitmapTriples.cpp
        // they count the number of appearances in a sequence instead, which saves memory
        // temporarily but they need to loop over it an additional time.
        for pos_z in 0..entries {
            let object = adjlist_z.sequence.get(pos_z);
            if object == 0 {
                error!("ERROR: There is a zero value in the Z level.");
                continue;
            }
            let pos_y = adjlist_z.bitmap.rank(pos_z.to_owned());
            indicess[object - 1].push(pos_y as u32); // hdt index counts from 1 but we count from 0 for simplicity
        }
        // reduce memory consumption of index by using adjacency list
        let mut bitmap_index_bitvector = BitVectorMut::new();
        //#[allow(clippy::redundant_closure_for_method_calls)] // false positive, anyhow transitive dep
        /*let mut cv = CompactVector::with_capacity(entries, sucds::utils::needed_bits(entries))
        .expect("Failed to create OPS index compact vector.");
        */
        let mut cv = Vec::<_>::new();
        // temporarily disable threading for wasm support
        //let wavelet_y = wavelet_thread.join().unwrap(); // join as late as possible for max parallelization
        for mut indices in indicess {
            let mut first = true;
            // sort by predicate
            indices.sort_by_cached_key(|pos_y| wavelet_y.get(*pos_y as usize).unwrap());
            for index in indices {
                bitmap_index_bitvector.push(first);
                first = false;
                cv.push(index as usize);
            }
        }
        let bv = BitVector::from(bitmap_index_bitvector);
        let bitmap_index = Bitmap { dict: RSNarrow::from(bv) };
        let op_index = OpIndex { sequence: Sequence::new(&cv), bitmap: bitmap_index };
        Self { order, bitmap_y, adjlist_z, op_index, wavelet_y }
    }

    /// Creates a new TriplesBitmap from a list of sorted RDF triples
    pub fn from_triples(triples: &[TripleId]) -> Self {
        let mut y_bitmap = BitVectorMut::new();
        let mut z_bitmap = BitVectorMut::new();
        let mut array_y = Vec::new();
        let mut array_z = Vec::new();

        let mut last_x = 0;
        let mut last_y = 0;
        let mut last_z = 0;
        let mut max_y = 1;
        let mut max_z = 1;

        for (i, triple) in triples.iter().enumerate() {
            let [x, y, z] = *triple;

            assert!(!(x == 0 || y == 0 || z == 0), "triple IDs should never be zero");
            max_y = max_y.max(y);
            max_z = max_z.max(z);

            if i == 0 {
                array_y.push(y);
            } else if x != last_x {
                assert!(x == last_x + 1, "the subjects must be correlative.");
                //x unchanged
                y_bitmap.push(true);
                array_y.push(y);

                z_bitmap.push(true);
            } else if y != last_y {
                assert!(y >= last_y, "the predicates must be in increasing order.");
                // y unchanged
                y_bitmap.push(false);
                array_y.push(y);

                z_bitmap.push(true);
            } else {
                assert!(z >= last_z, "the objects must be in increasing order");
                // z changed
                z_bitmap.push(false);
            }
            array_z.push(z);

            last_x = x;
            last_y = y;
            last_z = z;
        }
        y_bitmap.push(true);
        let n = y_bitmap.len();
        // pad to the next multiple of 64 so our comparisons match
        // TODO: can we just improve the comparisons instead?
        y_bitmap.extend_with_zeros(n.div_ceil(64) * 64 - n);
        z_bitmap.push(true);
        let bitmap_y = Bitmap::from(y_bitmap);
        let bitmap_z = Bitmap::from(z_bitmap);
        // bit_width() only in nightly for now
        /*let sequence_y = Sequence::new(&array_y, (Id::BITS - max_y.leading_zeros()) as usize);
        let sequence_z = Sequence::new(&array_z, (Id::BITS - max_z.leading_zeros()) as usize);*/
        let sequence_y = Sequence::new(&array_y);
        let sequence_z = Sequence::new(&array_z);
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);
        TriplesBitmap::new(Order::SPO, &sequence_y, bitmap_y, adjlist_z)
    }

    /// read the whole triple section including control information
    // TODO: rename to "read" for consistency with the other components and rename existing read function accordingly
    pub fn read_sect<R: BufRead>(reader: &mut R) -> Result<Self> {
        let triples_ci = ControlInfo::read(reader)?;

        match &triples_ci.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => TriplesBitmap::read(reader, &triples_ci),
            "<http://purl.org/HDT/hdt#triplesList>" => Err(Error::TriplesList),
            f => Err(Error::UnknownTriplesFormat(f.to_owned())),
        }
    }

    /// load the cached HDT index file, only supports TriplesBitmap
    #[cfg(feature = "cache")]
    pub fn load_cache<R: BufRead>(reader: &mut R, info: &ControlInfo) -> Result<Self> {
        match &info.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => TriplesBitmap::load(reader),
            "<http://purl.org/HDT/hdt#triplesList>" => Err(Error::TriplesList),
            f => Err(Error::UnknownTriplesFormat(f.to_owned())),
        }
    }

    /// load the entire cached TriplesBitmap object
    #[cfg(feature = "cache")]
    pub fn load<R: BufRead>(reader: &mut R) -> Result<Self> {
        let triples: TriplesBitmap = bincode::serde::decode_from_std_read(reader, bincode::config::standard())?;
        Ok(triples)
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.adjlist_z.size_in_bytes() + self.op_index.size_in_bytes() + self.wavelet_y.space_usage_byte()
    }

    /// Position in the wavelet index of the first predicate for the given subject ID.
    pub fn find_y(&self, subject_id: Id) -> usize {
        if subject_id == 0 {
            return 0;
        }
        self.bitmap_y.select1(subject_id - 1).unwrap_or_else(|| {
            panic!("invalid s_id {subject_id}, there are only {} subjects", self.bitmap_y.num_ones())
        }) as usize
            + 1
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
            let mid = usize::midpoint(low, high);
            match self.wavelet_y.get(mid).unwrap().cmp(&element) {
                Ordering::Less => low = mid + 1,
                Ordering::Greater => high = mid,
                Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Search the wavelet matrix for the position of a given subject, predicate pair.
    pub fn search_y(&self, subject_id: usize, property_id: usize) -> Option<usize> {
        self.bin_search_y(property_id, self.find_y(subject_id), self.last_y(subject_id) + 1)
    }

    /*
        /// Get the predicate ID for the given z index position.
    fn get_p(bitmap_z:  Bitmap, wavelet_y: WaveletMatrix, pos_z: usize) -> Id {
                let pos_y = bitmap_z.dict.rank(pos_z, true);
                wavelet_y.get(pos_y as usize) as Id
    }
    */

    fn read<R: BufRead>(reader: &mut R, triples_ci: &ControlInfo) -> Result<Self> {
        //let order: Order = Order::try_from(triples_ci.get("order").unwrap().parse::<u32>());
        let order: Order;
        if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            order = Order::try_from(n)?;
        } else {
            return Err(Error::UnspecifiedTriplesOrder);
        }

        // read bitmaps
        let bitmap_y = Bitmap::read(reader).map_err(|e| Error::Bitmap(Level::Y, e))?;
        let bitmap_z = Bitmap::read(reader).map_err(|e| Error::Bitmap(Level::Z, e))?;

        // read sequences
        let sequence_y = Sequence::read(reader).map_err(|e| Error::Sequence(Level::Y, e))?;
        let sequence_z = Sequence::read(reader).map_err(|e| Error::Sequence(Level::Z, e))?;
        let adjlist_z = AdjList::new(sequence_z, bitmap_z);

        let triples_bitmap = TriplesBitmap::new(order, &sequence_y, bitmap_y, adjlist_z);
        Ok(triples_bitmap)
    }

    pub fn write(&self, write: &mut impl std::io::Write) -> Result<()> {
        ControlInfo::bitmap_triples(self.order.clone() as u32, self.adjlist_z.len() as u32).write(write)?;
        self.bitmap_y.write(write).map_err(|e| Error::Bitmap(Level::Y, e))?;
        self.adjlist_z.bitmap.write(write).map_err(|e| Error::Bitmap(Level::Z, e))?;
        let y = self.wavelet_y.iter().collect::<Vec<_>>();
        Sequence::new(&y).write(write).map_err(|e| Error::Sequence(Level::Y, e))?;
        //Sequence::new(&y, self.wavelet_y.alph_width()).write(write).map_err(|e| Error::Sequence(Level::Y, e))?;
        self.adjlist_z.sequence.write(write).map_err(|e| Error::Sequence(Level::Z, e))?;
        Ok(())
    }

    /// Transform the given IDs of the layers in triple section order to a triple ID.
    /// Warning: At the moment only SPO is properly supported anyways, in which case this is equivalent to `[x,y,z]`.
    /// Other orders may lead to undefined behaviour.
    pub const fn coord_to_triple(&self, x: Id, y: Id, z: Id) -> Result<TripleId> {
        if x == 0 || y == 0 || z == 0 {
            return Err(Error::TripleComponentZero(x, y, z));
        }
        match self.order {
            Order::SPO => Ok([x, y, z]),
            Order::SOP => Ok([x, z, y]),
            Order::PSO => Ok([y, x, z]),
            Order::POS => Ok([y, z, x]),
            Order::OSP => Ok([z, x, y]),
            Order::OPS => Ok([z, y, x]),
            Order::Unknown => Err(Error::UnknownTriplesOrder),
        }
    }

    /// Number of triples
    pub const fn len(&self) -> usize {
        self.adjlist_z.sequence.entries
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
/// Subject index starting at 1 in the combined shared and subject section.
/// Predicate index starting at 1 in the predicate section.
/// Predicate index starting at 1 in the combined shared and object section.
/// When used as a triple, 0 values are invalid.
/// When used as a pattern, 0 values in a position match all values.
//#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub type TripleId = [Id; 3];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::tests::init;
    use crate::{FourSectDict, IdKind};
    use fs_err::File;
    use pretty_assertions::assert_eq;
    use std::io::BufReader;

    /// Iterator over all triples with a given ID in the specified position (subject, predicate or object).
    fn triples_with_id<'a>(t: &'a TriplesBitmap, id: usize, k: IdKind) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        match k {
            IdKind::Subject => Box::new(SubjectIter::with_s(t, id)),
            IdKind::Predicate => Box::new(PredicateIter::new(t, id)),
            IdKind::Object => Box::new(ObjectIter::new(t, id)),
        }
    }

    #[test]
    fn read_triples() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;
        let _dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        let v: Vec<TripleId> = triples.into_iter().collect::<Vec<TripleId>>();
        assert_eq!(v.len(), 328);
        assert_eq!(v[0][0], 1);
        assert_eq!(v[2][0], 1);
        assert_eq!(v[3][0], 2);
        let lens = [48, 23, 175];
        let [num_subjects, _num_predicates, _num_objects] = lens;
        let mut filtered: Vec<TripleId>;
        let funs = [|t: TripleId| t[0], |t: TripleId| t[1], |t: TripleId| t[2]];
        for j in 0..IdKind::KINDS.len() {
            for i in 1..=lens[j] {
                filtered = v.iter().filter(|tid| funs[j](**tid) == i).copied().collect();
                filtered.sort_unstable();
                let mut triples_with_id =
                    triples_with_id(&triples, i, IdKind::KINDS[j]).collect::<Vec<TripleId>>();
                triples_with_id.sort_unstable();
                assert_eq!(filtered, triples_with_id, "triples_with({},{:?})", i, IdKind::KINDS[j]);
            }
        }

        // SubjectIter
        assert_eq!(0, SubjectIter::empty(&triples).count());
        // SPO
        assert_eq!(vec![[14, 14, 154]], SubjectIter::with_pattern(&triples, [14, 14, 154]).collect::<Vec<_>>());
        // SP
        assert_eq!(vec![[14, 14, 154]], SubjectIter::with_pattern(&triples, [14, 14, 0]).collect::<Vec<_>>());
        // S??
        for i in 1..num_subjects {
            assert_eq!(
                SubjectIter::with_s(&triples, i).collect::<Vec<_>>(),
                SubjectIter::with_pattern(&triples, [i, 0, 0]).collect::<Vec<_>>()
            );
        }
        // ??? (all triples)
        assert_eq!(v, SubjectIter::with_pattern(&triples, [0, 0, 0]).collect::<Vec<_>>());
        // SP? where S and P are in the graph, but not together
        assert_eq!(0, SubjectIter::with_pattern(&triples, [12, 14, 154]).count());
        Ok(())
    }

    /*
      #[test]
        fn from_triples() -> color_eyre::Result<()> {
            //let triples: Vec<TripleId> = vec![[1, 2, 3], TripleId::new(1, 2, 4), TripleId::new(2, 3, 5)]; // TODO: add more or read existing ones from file
            todo!("not yet implemented");
        }
    */
}
