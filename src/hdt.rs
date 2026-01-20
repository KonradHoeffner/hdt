use crate::containers::{ControlInfo, control_info};
use crate::four_sect_dict::{self, IdKind};
use crate::header::Header;
use crate::triples::{Id, ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId, TriplesBitmap};
use crate::{FourSectDict, header};
use bytesize::ByteSize;
use log::{debug, error};
#[cfg(feature = "cache")]
use std::fs::File;
#[cfg(feature = "cache")]
use std::io::{Seek, SeekFrom, Write};
use std::iter;
use std::sync::Arc;

pub type Result<T> = core::result::Result<T, Error>;

#[cfg(feature = "cache")]
const CACHE_EXT: &str = "index.v1-rust-cache";
#[cfg(feature = "nt")]
#[path = "nt.rs"]
/// Converting N-Triples to HDT, available only if HDT is built with the experimental `"nt"` feature.
mod nt;

/// In-memory representation of an RDF graph loaded from an HDT file.
/// Allows queries by triple patterns.
#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    // header is not necessary for querying but shouldn't waste too much space and we need it for writing in the future, may also make it optional
    header: Header,
    /// in-memory representation of dictionary
    pub dict: FourSectDict,
    /// in-memory representation of triples
    pub triples: TriplesBitmap,
}

type StringTriple = [Arc<str>; 3];

/// The error type for the `translate_id` method.
#[derive(thiserror::Error, Debug)]
#[error("cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateError {
    #[source]
    e: four_sect_dict::ExtractError,
    t: TripleId,
}

/// The error type for the `new` method.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to read HDT control info")]
    ControlInfo(#[from] control_info::Error),
    #[error("failed to read HDT header")]
    Header(#[from] header::Error),
    #[error("failed to read HDT four section dictionary")]
    FourSectDict(#[from] four_sect_dict::Error),
    #[error("failed to read HDT triples section")]
    Triples(#[from] crate::triples::Error),
    #[error("IO Error")]
    Io(#[from] std::io::Error),
}

impl Hdt {
    #[deprecated(since = "0.4.0", note = "please use `read` instead")]
    pub fn new<R: std::io::BufRead>(reader: R) -> Result<Self> {
        Self::read(reader)
    }

    /// Creates an immutable HDT instance containing the dictionary and triples from the given reader.
    /// The reader must point to the beginning of the data of an HDT file.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    /// # Example
    /// ```
    /// let file = std::fs::File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    /// let hdt = hdt::Hdt::read(std::io::BufReader::new(file)).unwrap();
    /// ```
    pub fn read<R: std::io::BufRead>(mut reader: R) -> Result<Self> {
        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { header, dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// Write as N-Triples
    #[cfg(feature = "sophia")]
    pub fn write_nt(&self, write: &mut impl std::io::Write) -> std::io::Result<()> {
        use sophia::api::prelude::TripleSerializer;
        use sophia::turtle::serializer::nt::NtSerializer;
        NtSerializer::new(write).serialize_graph(self).map_err(|e| std::io::Error::other(format!("{e}")))?;
        Ok(())
    }

    /// Creates an immutable HDT instance containing the dictionary and triples from the Path.
    /// Will utilize a custom cached TriplesBitmap file if exists or create one if it does not exist.
    /// The file path must point to the beginning of the data of an HDT file.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    /// # Example
    /// ```
    /// let hdt = hdt::Hdt::read_from_path(std::path::Path::new("tests/resources/snikmeta.hdt")).unwrap();
    /// ```
    #[cfg(feature = "cache")]
    pub fn read_from_path(f: &std::path::Path) -> Result<Self> {
        use log::warn;

        let source = File::open(f)?;
        let mut reader = std::io::BufReader::new(source);
        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let mut abs_path = std::fs::canonicalize(f)?;
        let _ = abs_path.pop();
        let index_file_name = format!("{}.{CACHE_EXT}", f.file_name().unwrap().to_str().unwrap());
        let index_file_path = abs_path.join(index_file_name);
        let triples = if index_file_path.exists() {
            let pos = reader.stream_position()?;
            match Self::load_with_cache(&mut reader, &index_file_path, header.length) {
                Ok(triples) => triples,
                Err(e) => {
                    warn!("error loading cache, overwriting: {e}");
                    reader.seek(SeekFrom::Start(pos))?;
                    Self::load_without_cache(&mut reader, &index_file_path, header.length)?
                }
            }
        } else {
            Self::load_without_cache(&mut reader, &index_file_path, header.length)?
        };

        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { header, dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    #[cfg(feature = "cache")]
    fn load_without_cache<R: std::io::BufRead>(
        mut reader: R, index_file_path: &std::path::PathBuf, header_length: usize,
    ) -> Result<TriplesBitmap> {
        use log::warn;

        debug!("no cache detected, generating index");
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        debug!("index generated, saving cache to {}", index_file_path.display());
        if let Err(e) = Self::write_cache(index_file_path, &triples, header_length) {
            warn!("error trying to save cache to file: {e}");
        }
        Ok(triples)
    }

    #[cfg(feature = "cache")]
    fn load_with_cache<R: std::io::BufRead>(
        mut reader: R, index_file_path: &std::path::PathBuf, header_length: usize,
    ) -> core::result::Result<TriplesBitmap, Box<dyn std::error::Error>> {
        use std::io::Read;
        // load cached index
        debug!("hdt file cache detected, loading from {}", index_file_path.display());
        let index_source = File::open(index_file_path)?;
        let mut index_reader = std::io::BufReader::new(index_source);
        let triples_ci = ControlInfo::read(&mut reader)?;
        // we cannot rely on the numTriples property being present, see https://github.com/rdfhdt/hdt-cpp/issues/289
        // let num_triples = triples_ci.get("numTriples").expect("numTriples key missing in triples CI");
        // thus we use the number of bytes of the header data
        let mut buf = [0u8; size_of::<usize>()];
        index_reader.read_exact(&mut buf)?;
        if header_length != usize::from_le_bytes(buf) {
            return Err("failed index validation".into());
        }
        let triples = TriplesBitmap::load_cache(&mut index_reader, &triples_ci)?;
        Ok(triples)
    }

    #[cfg(feature = "cache")]
    /// Writes a custom cache file to improve load times. This cache file is usuable only by
    /// this library and is not intended to be used with hdt-cpp or hdt-java versions of the HDT tooling
    pub fn write_cache(
        index_file_path: &std::path::PathBuf, triples: &TriplesBitmap, header_length: usize,
    ) -> core::result::Result<(), Box<dyn std::error::Error>> {
        let new_index_file = File::create(index_file_path)?;
        let mut writer = std::io::BufWriter::new(new_index_file);
        writer.write_all(&header_length.to_le_bytes())?;
        bincode::serde::encode_into_std_write(triples, &mut writer, bincode::config::standard())?;
        writer.flush()?;
        Ok(())
    }

    pub fn write(&self, write: &mut impl std::io::Write) -> Result<()> {
        ControlInfo::global().write(write)?;
        self.header.write(write)?;
        self.dict.write(write)?;
        self.triples.write(write)?;
        write.flush()?;
        Ok(())
    }

    /// Recursive size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triples.size_in_bytes()
    }

    /// An iterator visiting *all* triples as strings in order.
    /// Using this method with a filter can be inefficient for large graphs,
    /// because the strings are stored in compressed form and must be decompressed and allocated.
    /// Whenever possible, use [`Hdt::triples_with_pattern`] instead.
    /// # Example
    /// ```
    /// fn print_first_triple(hdt: hdt::Hdt) {
    ///     println!("{:?}", hdt.triples_all().next().expect("no triple in the graph"));
    /// }
    /// ```
    pub fn triples_all(&self) -> impl Iterator<Item = StringTriple> + '_ {
        let mut triple_cache = TripleCache::new(self);
        self.triples.into_iter().map(move |ids| triple_cache.translate(ids).unwrap())
    }

    /// Get all subjects with the given property and object (?PO pattern).
    /// Use this over `triples_with_pattern(None,Some(p),Some(o))` if you don't need whole triples.
    /// # Example
    /// Who was born in Leipzig?
    /// ```
    /// fn query(dbpedia: hdt::Hdt) {
    ///     for person in dbpedia.subjects_with_po(
    ///       "http://dbpedia.org/ontology/birthPlace", "http://dbpedia.org/resource/Leipzig") {
    ///       println!("{person:?}");
    ///     }
    /// }
    /// ```
    pub fn subjects_with_po(&self, p: &str, o: &str) -> Box<dyn Iterator<Item = String> + '_> {
        let pid = self.dict.string_to_id(p, IdKind::Predicate);
        let oid = self.dict.string_to_id(o, IdKind::Object);
        // predicate or object not in dictionary, iterator would interpret 0 as variable
        if pid == 0 || oid == 0 {
            return Box::new(iter::empty());
        }
        // needed for extending the lifetime of the parameters into the iterator for error messages
        let p_owned = p.to_owned();
        let o_owned = o.to_owned();
        Box::new(
            PredicateObjectIter::new(&self.triples, pid, oid)
                .map(move |sid| self.dict.id_to_string(sid, IdKind::Subject))
                .filter_map(move |r| {
                    r.map_err(|e| error!("Error on triple with property {p_owned} and object {o_owned}: {e}")).ok()
                }),
        )
    }

    /// Get all triples that fit the given triple patterns, where `None` stands for a variable.
    /// For example, `triples_with_pattern(Some(s), Some(p), None)` answers an SP? pattern.
    /// # Example
    /// What is the capital of the United States of America?
    /// ```
    /// fn query(dbpedia: hdt::Hdt) {
    ///   println!("{:?}", dbpedia.triples_with_pattern(
    ///     Some("http://dbpedia.org/resource/United_States"), Some("http://dbpedia.org/ontology/capital"), None)
    ///     .next().expect("no capital found")[2]);
    /// }
    /// ```
    pub fn triples_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = StringTriple> + 'a> {
        let pattern: [Option<(Arc<str>, usize)>; 3] = [(0, sp), (1, pp), (2, op)]
            .map(|(i, x)| x.map(|x| (Arc::from(x), self.dict.string_to_id(x, IdKind::KINDS[i]))));
        // at least one term does not exist in the graph
        if pattern.iter().flatten().any(|x| x.1 == 0) {
            return Box::new(iter::empty());
        }
        // TODO: improve error handling
        let mut cache = TripleCache::new(self);
        match pattern {
            [Some(s), Some(p), Some(o)] => {
                if SubjectIter::with_pattern(&self.triples, [s.1, p.1, o.1]).next().is_some() {
                    Box::new(iter::once([s.0, p.0, o.0]))
                } else {
                    Box::new(iter::empty())
                }
            }
            [Some(s), Some(p), None] => {
                Box::new(SubjectIter::with_pattern(&self.triples, [s.1, p.1, 0]).map(move |t| {
                    [s.0.clone(), p.0.clone(), Arc::from(self.dict.id_to_string(t[2], IdKind::Object).unwrap())]
                }))
            }
            [Some(s), None, Some(o)] => {
                Box::new(SubjectIter::with_pattern(&self.triples, [s.1, 0, o.1]).map(move |t| {
                    [s.0.clone(), Arc::from(self.dict.id_to_string(t[1], IdKind::Predicate).unwrap()), o.0.clone()]
                }))
            }
            [Some(s), None, None] => Box::new(
                SubjectIter::with_pattern(&self.triples, [s.1, 0, 0])
                    .map(move |t| [s.0.clone(), cache.get(1, t[1]).unwrap(), cache.get(2, t[2]).unwrap()]),
            ),
            [None, Some(p), Some(o)] => {
                Box::new(PredicateObjectIter::new(&self.triples, p.1, o.1).map(move |sid| {
                    [Arc::from(self.dict.id_to_string(sid, IdKind::Subject).unwrap()), p.0.clone(), o.0.clone()]
                }))
            }
            [None, Some(p), None] => Box::new(
                PredicateIter::new(&self.triples, p.1)
                    .map(move |t| [cache.get(0, t[0]).unwrap(), p.0.clone(), cache.get(2, t[2]).unwrap()]),
            ),
            [None, None, Some(o)] => Box::new(
                ObjectIter::new(&self.triples, o.1)
                    .map(move |t| [cache.get(0, t[0]).unwrap(), cache.get(1, t[1]).unwrap(), o.0.clone()]),
            ),
            [None, None, None] => Box::new(self.triples_all()),
        }
    }

    /// Get all internal triple IDs that fit the given triple patterns, where `None` stands for a variable.
    /// Used for specific optimizations, call triples_with_pattern instead to get actual string results.
    pub fn triple_ids_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        let pattern: [Option<usize>; 3] =
            [(0, sp), (1, pp), (2, op)].map(|(i, x)| x.map(|x| self.dict.string_to_id(x, IdKind::KINDS[i])));
        // fail fast if any constant is 0, meaning missing from the dict
        if pattern.contains(&Some(0)) {
            return Box::new(iter::empty());
        }
        // we did not fail, so all constants are >0 so we transform None values into 0 to signify variables
        let pattern: TripleId = pattern.map(|x| x.unwrap_or(0));
        self.triple_ids_with_id_pattern(pattern)
    }

    /// Get all internal triple IDs that fit the given triple patterns, where 0 stands for a variable.
    /// Used for specific optimizations, call triples_with_pattern instead to get actual string results.
    pub fn triple_ids_with_id_pattern<'a>(&'a self, pattern: TripleId) -> Box<dyn Iterator<Item = TripleId> + 'a> {
        let ts = &self.triples;
        let [s, p, o] = pattern;
        // can't use slice: half_open_range_patterns_in_slices is still unstable, see https://github.com/rust-lang/rust/issues/67264
        match (s, p, o) {
            (1.., _, _) => Box::new(SubjectIter::with_pattern(ts, [s, p, o]).map(move |t| [s, t[1], t[2]])),
            (0, 1.., 1..) => Box::new(PredicateObjectIter::new(ts, p, o).map(move |sid| [sid, p, o])),
            (0, 1.., 0) => Box::new(PredicateIter::new(ts, p).map(move |t| [t[0], p, t[2]])),
            (0, 0, 1..) => Box::new(ObjectIter::new(ts, o).map(move |t| [t[0], t[1], o])),
            (0, 0, 0) => Box::new(self.triples.into_iter()),
        }
    }
}

/// A TripleCache stores the `Arc<str>` of the last returned triple
#[derive(Clone, Debug)]
struct TripleCache<'a> {
    hdt: &'a Hdt,
    tid: TripleId,
    arc: [Option<Arc<str>>; 3],
}

impl<'a> TripleCache<'a> {
    /// Build a new [`TripleCache`] for the given [`Hdt`]
    const fn new(hdt: &'a super::Hdt) -> Self {
        TripleCache { hdt, tid: [0; 3], arc: [None, None, None] }
    }

    /// Translate a triple of indexes into a triple of strings.
    fn translate(&mut self, t: TripleId) -> core::result::Result<StringTriple, TranslateError> {
        // refactor when try_map for arrays becomes stable
        Ok([
            self.get(0, t[0]).map_err(|e| TranslateError { e, t })?,
            self.get(1, t[1]).map_err(|e| TranslateError { e, t })?,
            self.get(2, t[2]).map_err(|e| TranslateError { e, t })?,
        ])
    }

    fn get(&mut self, pos: usize, id: Id) -> core::result::Result<Arc<str>, four_sect_dict::ExtractError> {
        debug_assert!(id != 0);
        debug_assert!(pos < 3);
        if self.tid[pos] == id {
            Ok(self.arc[pos].as_ref().unwrap().clone())
        } else {
            let ret: Arc<str> = self.hdt.dict.id_to_string(id, IdKind::KINDS[pos])?.into();
            self.arc[pos] = Some(ret.clone());
            self.tid[pos] = id;
            Ok(ret)
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::tests::init;
    use color_eyre::Result;
    use fs_err::File;
    use pretty_assertions::{assert_eq, assert_ne};

    /// reusable test HDT read from SNIK Meta test HDT file
    pub fn snikmeta() -> Result<Hdt> {
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename)?;
        Ok(Hdt::read(std::io::BufReader::new(file))?)
    }

    #[test]
    fn write() -> Result<()> {
        init();
        let hdt = snikmeta()?;
        snikmeta_check(&hdt)?;
        let mut buf = Vec::<u8>::new();
        hdt.write(&mut buf)?;
        let hdt2 = Hdt::read(std::io::Cursor::new(buf))?;
        snikmeta_check(&hdt2)?;
        Ok(())
    }

    // make sure loading with cache works under different circumstances
    // e.g. clear cache, prexisting cache, stale cache
    #[cfg(feature = "cache")]
    #[test]
    fn cache() -> Result<()> {
        use fs_err::remove_file;
        use std::path::Path;
        init();
        // start with an empty cache
        let filename = "tests/resources/snikmeta.hdt";
        let cachename = format!("{filename}.{CACHE_EXT}");
        let path = Path::new(filename);
        let path_cache = Path::new(&cachename);
        // force fresh cache
        let _ = remove_file(path_cache);
        let hdt1 = Hdt::read_from_path(path)?;
        snikmeta_check(&hdt1)?;
        // now it should be cached
        let hdt2 = Hdt::read_from_path(path)?;
        snikmeta_check(&hdt2)?;
        #[cfg(feature = "nt")]
        {
            // create a cache for an empty HDT
            let path_empty_nt = Path::new("tests/resources/empty.nt");
            // it's empty so we could just pass an empty buffer
            let hdt_empty = Hdt::read_nt(path_empty_nt)?;
            let filename_empty_hdt = "tests/resources/empty.hdt";
            let path_empty_hdt = Path::new(filename_empty_hdt);
            if !path_empty_hdt.exists() {
                let file_empty_hdt = File::create(filename_empty_hdt)?;
                let mut writer = std::io::BufWriter::new(file_empty_hdt);
                hdt_empty.write(&mut writer)?;
            }
            // we don't care about the empty HDT, we just need it to create the cache
            let filename_empty_cache = format!("{filename_empty_hdt}.{CACHE_EXT}");
            let path_empty_cache = Path::new(&filename_empty_cache);
            let _ = remove_file(path_empty_cache);
            Hdt::read_from_path(path_empty_hdt)?;
            // purposefully create a mismatch between cache and HDT file for the same name
            fs_err::rename(path_empty_cache, path_cache)?;
            // mismatch should be detected and handled
            let hdt3 = Hdt::read_from_path(path)?;
            snikmeta_check(&hdt3)?;
        }
        Ok(())
    }

    pub fn snikmeta_check(hdt: &Hdt) -> Result<()> {
        let triples = &hdt.triples;
        assert_eq!(triples.bitmap_y.num_ones(), 49, "{:?}", triples.bitmap_y); // one for each subjecct
        //assert_eq!();
        let v: Vec<StringTriple> = hdt.triples_all().collect();
        assert_eq!(v.len(), 328);
        assert_eq!(hdt.dict.shared.num_strings, 43);
        assert_eq!(hdt.dict.subjects.num_strings, 6);
        assert_eq!(hdt.dict.predicates.num_strings, 23);
        assert_eq!(hdt.dict.objects.num_strings, 133);
        assert_eq!(v, hdt.triples_with_pattern(None, None, None).collect::<Vec<_>>(), "all triples not equal ???");
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple[0].as_ref() == uri).collect();
            let with_s: Vec<_> = hdt.triples_with_pattern(Some(uri), None, None).collect();
            assert_eq!(filtered, with_s, "results differ between triples_all() and S?? query for {}", uri);
        }
        let s = "http://www.snik.eu/ontology/meta/Top";
        let p = "http://www.w3.org/2000/01/rdf-schema#label";
        let o = "\"top class\"@en";
        let triple_vec = vec![[Arc::from(s), Arc::from(p), Arc::from(o)]];
        // triple patterns with 2-3 terms
        assert_eq!(triple_vec, hdt.triples_with_pattern(Some(s), Some(p), Some(o)).collect::<Vec<_>>(), "SPO");
        assert_eq!(triple_vec, hdt.triples_with_pattern(Some(s), Some(p), None).collect::<Vec<_>>(), "SP?");
        assert_eq!(triple_vec, hdt.triples_with_pattern(Some(s), None, Some(o)).collect::<Vec<_>>(), "S?O");
        assert_eq!(triple_vec, hdt.triples_with_pattern(None, Some(p), Some(o)).collect::<Vec<_>>(), "?PO");
        let et = "http://www.snik.eu/ontology/meta/EntityType";
        let meta = "http://www.snik.eu/ontology/meta";
        let subjects = ["ApplicationComponent", "Method", "RepresentationType", "SoftwareProduct"]
            .map(|s| meta.to_owned() + "/" + s)
            .to_vec();
        assert_eq!(
            subjects,
            hdt.subjects_with_po("http://www.w3.org/2000/01/rdf-schema#subClassOf", et).collect::<Vec<_>>()
        );
        assert_eq!(
            12,
            hdt.triples_with_pattern(None, Some("http://www.w3.org/2000/01/rdf-schema#subClassOf"), None).count()
        );
        assert_eq!(20, hdt.triples_with_pattern(None, None, Some(et)).count());
        let snikeu = "http://www.snik.eu";
        let triple_vec = [
            "http://purl.org/dc/terms/publisher", "http://purl.org/dc/terms/source",
            "http://xmlns.com/foaf/0.1/homepage",
        ]
        .into_iter()
        .map(|p| [Arc::from(meta), Arc::from(p), Arc::from(snikeu)])
        .collect::<Vec<_>>();
        assert_eq!(
            triple_vec,
            hdt.triples_with_pattern(Some(meta), None, Some(snikeu)).collect::<Vec<_>>(),
            "S?O multiple"
        );
        let s = "http://www.snik.eu/ontology/meta/хобби-N-0";
        assert_eq!(hdt.dict.string_to_id(s, IdKind::Subject), 49);
        assert_eq!(hdt.dict.id_to_string(49, IdKind::Subject)?, s);
        let o = "\"ХОББИ\"@ru";
        let triple_vec = vec![[Arc::from(s), Arc::from(p), Arc::from(o)]];
        assert_eq!(hdt.triples_with_pattern(Some(s), Some(p), None).collect::<Vec<_>>(), triple_vec);
        Ok(())
    }
}
