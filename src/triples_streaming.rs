//! Decode-only, one-shot streaming of every triple in an HDT archive in SPO
//! order, **without** building the [`TriplesBitmap`](crate::triples::TriplesBitmap)
//! query indexes.
//!
//! [`Hdt::read`](crate::Hdt::read) calls [`TriplesBitmap::read_sect`], which in
//! turn calls [`TriplesBitmap::new`]. That constructor exists purely to serve
//! triple-pattern / object / predicate *queries*: it builds a wavelet matrix over
//! `sequence_y`, a per-object `Vec<Vec<u32>>`, a `sort_by_cached_key`, and an
//! object-predicate index (a compact vector + a rank/select bitmap). A consumer
//! that only wants to read every triple once — e.g. a bulk import into another
//! store — issues none of those queries, so all of that work is performed and then
//! immediately dropped.
//!
//! [`Hdt::triples_streaming`] reads the same on-disk sections the full path reads
//! (the global control info + header, the four-section dictionary, and the triples
//! section's `bitmap_y` / `bitmap_z` / `sequence_y` / `sequence_z`) and walks the
//! adjacency lists sequentially to yield `[usize; 3]` triple IDs, skipping
//! `TriplesBitmap::new` entirely. The walk is the same one [`SubjectIter`] performs
//! over the built structure, but reads the predicate from `sequence_y` and the
//! end-of-run flags from the two bitmaps directly.
//!
//! [`SubjectIter`]: crate::triples::SubjectIter
//! [`TriplesBitmap::read_sect`]: crate::triples::TriplesBitmap::read_sect
//! [`TriplesBitmap::new`]: crate::triples::TriplesBitmap::new

use crate::Hdt;
use crate::containers::{Bitmap, ControlInfo, ControlType, Sequence};
use crate::four_sect_dict::FourSectDict;
use crate::header::Header;
use std::io::{self, BufRead, Error, ErrorKind};

/// The triples section, read but not indexed: the two adjacency bitmaps and the two
/// ID sequences exactly as they appear on disk. Walking these yields every triple in
/// SPO order without the wavelet matrix / OP-index that `TriplesBitmap::new` builds.
struct StreamingTriples {
    bitmap_y: Bitmap,
    bitmap_z: Bitmap,
    sequence_y: Sequence,
    sequence_z: Sequence,
}

impl StreamingTriples {
    /// Reads the triples section (control info + the two bitmaps + the two
    /// sequences) without building any query index. Mirrors the section order of
    /// [`TriplesBitmap::read`](crate::triples::TriplesBitmap), minus the
    /// `TriplesBitmap::new` call.
    fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        let triples_ci =
            ControlInfo::read(reader).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        if triples_ci.control_type != ControlType::Triples {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("expected triples control info, got {:?}", triples_ci.control_type),
            ));
        }
        match triples_ci.format.as_str() {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => {}
            "<http://purl.org/HDT/hdt#triplesList>" => {
                return Err(Error::new(ErrorKind::InvalidData, "triples list format is not supported"));
            }
            f => return Err(Error::new(ErrorKind::InvalidData, format!("unknown triples format {f}"))),
        }
        // Only SPO (order 1) is supported, matching the full read path.
        match triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            Some(1) => {}
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unsupported triples order {other:?} (only SPO is supported)"),
                ));
            }
        }

        // Same on-disk order as TriplesBitmap::read: bitmap_y, bitmap_z, sequence_y, sequence_z.
        let to_io = |e: crate::containers::bitmap::Error| Error::new(ErrorKind::InvalidData, e.to_string());
        let bitmap_y = Bitmap::read(reader).map_err(to_io)?;
        let bitmap_z = Bitmap::read(reader).map_err(to_io)?;
        let seq_io = |e: crate::containers::sequence::Error| Error::new(ErrorKind::InvalidData, e.to_string());
        let sequence_y = Sequence::read(reader).map_err(seq_io)?;
        let sequence_z = Sequence::read(reader).map_err(seq_io)?;

        Ok(StreamingTriples { bitmap_y, bitmap_z, sequence_y, sequence_z })
    }
}

/// Iterator over `[subject_id, predicate_id, object_id]` triples in SPO order,
/// produced by [`Hdt::triples_streaming`] without any query index. IDs are 1-based,
/// as in the HDT dictionary; resolve them through the dictionary returned alongside
/// the iterator if string terms are needed.
pub struct TripleIdStreamingIter {
    triples: StreamingTriples,
    x: usize,
    pos_y: usize,
    pos_z: usize,
    max_y: usize,
    max_z: usize,
}

impl TripleIdStreamingIter {
    const fn new(triples: StreamingTriples) -> Self {
        let max_y = triples.sequence_y.entries;
        let max_z = triples.sequence_z.entries;
        TripleIdStreamingIter { triples, x: 1, pos_y: 0, pos_z: 0, max_y, max_z }
    }
}

impl Iterator for TripleIdStreamingIter {
    type Item = [usize; 3];

    /// Sequential SPO walk, identical in shape to [`SubjectIter::next`], but reading
    /// the predicate from `sequence_y` and the run-end flags from the raw bitmaps.
    ///
    /// [`SubjectIter::next`]: crate::triples::SubjectIter
    fn next(&mut self) -> Option<[usize; 3]> {
        if self.pos_y >= self.max_y || self.pos_z >= self.max_z {
            return None;
        }
        let s = self.x;
        let p = self.triples.sequence_y.get(self.pos_y);
        let o = self.triples.sequence_z.get(self.pos_z);

        // at_last_sibling reads the bit at the given position without rank/select.
        if self.triples.bitmap_z.at_last_sibling(self.pos_z) {
            if self.triples.bitmap_y.at_last_sibling(self.pos_y) {
                self.x += 1;
            }
            self.pos_y += 1;
        }
        self.pos_z += 1;
        Some([s, p, o])
    }
}

impl Hdt {
    /// Decode-only: read the dictionary, then stream every triple as `[s, p, o]`
    /// dictionary IDs in SPO order **without** building the wavelet matrix / OP-index
    /// that [`Hdt::read`] constructs for pattern queries.
    ///
    /// This is intended for one-shot bulk reads (e.g. importing every triple into
    /// another store) where none of the query indexes are ever used. It returns the
    /// validated [`FourSectDict`] dictionary (so IDs can be resolved to terms) and an
    /// iterator over the triple IDs. The IDs follow the HDT convention: subjects and
    /// objects share the leading "shared" section, predicates are a separate space;
    /// use [`FourSectDict::id_to_string`] with the appropriate
    /// [`IdKind`](crate::IdKind) to resolve them.
    ///
    /// # Example
    /// ```
    /// let file = std::fs::File::open("tests/resources/snikmeta.hdt").expect("open");
    /// let (dict, triples) = hdt::Hdt::triples_streaming(std::io::BufReader::new(file)).unwrap();
    /// let count = triples.count();
    /// assert!(count > 0);
    /// let _ = dict;
    /// ```
    pub fn triples_streaming<R: BufRead>(mut reader: R) -> io::Result<(FourSectDict, TripleIdStreamingIter)> {
        // Global control info + header, then the four-section dictionary.
        ControlInfo::read(&mut reader).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        Header::read(&mut reader).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let dict = FourSectDict::read(&mut reader)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
            .validate()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let triples = StreamingTriples::read(&mut reader)?;
        Ok((dict, TripleIdStreamingIter::new(triples)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Hdt, IdKind};
    use std::collections::BTreeSet;
    use std::fs::File;
    use std::io::BufReader;

    const SNIKMETA: &str = "tests/resources/snikmeta.hdt";

    /// The decode-only stream must yield exactly the same triples (as resolved term
    /// strings) as the full `Hdt::read` path, which builds the query indexes.
    #[test]
    fn streaming_matches_full_read() {
        // Full path: build everything, collect resolved triples.
        let full = Hdt::read(BufReader::new(File::open(SNIKMETA).unwrap())).unwrap();
        let expected: BTreeSet<[String; 3]> =
            full.triples_all().map(|[s, p, o]| [s.to_string(), p.to_string(), o.to_string()]).collect();

        // Decode-only path: stream IDs, resolve through the dictionary, no indexes built.
        let (dict, stream) = Hdt::triples_streaming(BufReader::new(File::open(SNIKMETA).unwrap())).unwrap();
        let got: BTreeSet<[String; 3]> = stream
            .map(|[s, p, o]| {
                [
                    dict.id_to_string(s, IdKind::Subject).unwrap(),
                    dict.id_to_string(p, IdKind::Predicate).unwrap(),
                    dict.id_to_string(o, IdKind::Object).unwrap(),
                ]
            })
            .collect();

        assert!(!expected.is_empty(), "fixture must contain triples");
        assert_eq!(expected, got, "decode-only stream must equal the full read path");
    }
}
