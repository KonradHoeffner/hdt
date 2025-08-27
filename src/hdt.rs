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
    /// The reader must point to the beginning of the data of an HDT file as produced by hdt-cpp.
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

    /// Converts RDF N-Triples to HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order.
    /// *This function is available only if HDT is built with the `"sophia"` feature, included by default.*
    /// # Example
    /// ```no_run
    /// let path = std::path::Path::new("example.nt");
    /// let hdt = hdt::Hdt::read_nt(path).unwrap();
    /// ```
    ///// let hdt = hdt::Hdt::read_nt(std::io::BufReader::new(file)).unwrap();
    // TODO: I (KH) prefer to use a BufRead here, is the file IRI important? I don't mind leaving it out of the header.
    #[cfg(feature = "sophia")]
    //pub fn read_nt<R: std::io::BufRead>(mut reader: R) -> Result<Self> {
    pub fn read_nt(f: &std::path::Path) -> Result<Self> {
        use std::collections::BTreeSet;
        use std::io::Write;

        const BLOCK_SIZE: usize = 16;

        let source = std::fs::File::open(f)?;
        let mut reader = std::io::BufReader::new(source);
        let (dict, mut encoded_triples) = FourSectDict::read_nt(&mut reader, BLOCK_SIZE)?;
        let num_triples = encoded_triples.len();
        if num_triples == 0 {
            use crate::triples;

            return Err(Error::Triples(triples::Error::Empty));
        }
        encoded_triples.sort_unstable();
        let triples = TriplesBitmap::from_triples(&encoded_triples);

        let header = Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() };

        let mut hdt = Hdt { header, dict, triples };
        hdt.build_header(f, BLOCK_SIZE, num_triples);
        let mut buf = Vec::<u8>::new();
        for triple in &hdt.header.body {
            writeln!(buf, "{triple}")?;
        }
        hdt.header.length = buf.len();
        //println!("header length {}", hdt.header.length);
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// populated HDT header fields
    // TODO are all of these headers required for HDT spec? Populating same triples as those in C++ version for now
    #[cfg(feature = "sophia")]
    fn build_header(&mut self, path: &std::path::Path, block_size: usize, num_triples: usize) {
        use crate::containers::rdf::Term::Literal as Lit;
        use crate::containers::rdf::{Id, Literal, Term, Triple};
        use crate::vocab::*;
        use std::collections::BTreeSet;

        const ORDER: &str = "SPO";
        let mut headers = BTreeSet::<Triple>::new();

        macro_rules! literal {
            ($s:expr, $p:expr, $o:expr) => {
                headers.insert(Triple::new($s.clone(), $p.to_owned(), Lit(Literal::new($o.to_string()))));
            };
        }
        macro_rules! insert_id {
            ($s:expr, $p:expr, $o:expr) => {
                headers.insert(Triple::new($s.clone(), $p.to_owned(), Term::Id($o.clone())));
            };
        }

        let file_iri = format!("file://{}", path.canonicalize().unwrap().display());
        let base = Id::Named(file_iri);

        literal!(base, RDF_TYPE, HDT_CONTAINER);
        literal!(base, RDF_TYPE, VOID_DATASET);
        literal!(base, VOID_TRIPLES, num_triples);
        literal!(base, VOID_PROPERTIES, self.dict.predicates.num_strings);
        let [d_s, d_o] =
            [&self.dict.subjects, &self.dict.objects].map(|s| s.num_strings + self.dict.shared.num_strings);
        literal!(base, VOID_DISTINCT_SUBJECTS, d_s);
        literal!(base, VOID_DISTINCT_OBJECTS, d_o);
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = Id::Blank("statistics".to_owned());
        let pub_id = Id::Blank("publicationInformation".to_owned());
        let format_id = Id::Blank("format".to_owned());
        let dict_id = Id::Blank("dictionary".to_owned());
        let triples_id = Id::Blank("triples".to_owned());
        insert_id!(base, HDT_STATISTICAL_INFORMATION, stats_id);
        insert_id!(base, HDT_STATISTICAL_INFORMATION, pub_id);
        insert_id!(base, HDT_FORMAT_INFORMATION, format_id);
        insert_id!(format_id, HDT_DICTIONARY, dict_id);
        insert_id!(format_id, HDT_TRIPLES, triples_id);
        // DICTIONARY
        literal!(dict_id, HDT_DICT_SHARED_SO, self.dict.shared.num_strings);
        literal!(dict_id, HDT_DICT_MAPPING, "1");
        literal!(dict_id, HDT_DICT_SIZE_STRINGS, ByteSize(self.dict.size_in_bytes() as u64));
        literal!(dict_id, HDT_DICT_BLOCK_SIZE, block_size);
        // TRIPLES
        literal!(triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP);
        literal!(triples_id, HDT_NUM_TRIPLES, num_triples);
        literal!(triples_id, HDT_TRIPLES_ORDER, ORDER);
        // // Sizes
        let meta = std::fs::File::open(path).unwrap().metadata().unwrap();
        literal!(stats_id, HDT_ORIGINAL_SIZE, meta.len());
        literal!(stats_id, HDT_SIZE, ByteSize(self.size_in_bytes() as u64));
        // exclude for now to skip dependency on chrono
        //let datetime_str = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
        //literal!(pub_id,DC_TERMS_ISSUED,datetime_str);
        self.header.body = headers;
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
    /// The file path must point to the beginning of the data of an HDT file as produced by hdt-cpp.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    /// # Example
    /// ```
    /// let hdt = hdt::Hdt::new_from_path(std::path::Path::new("tests/resources/snikmeta.hdt")).unwrap();
    /// ```
    #[cfg(feature = "cache")]
    pub fn new_from_path(f: &std::path::Path) -> Result<Self> {
        use log::warn;

        let source = File::open(f)?;
        let mut reader = std::io::BufReader::new(source);
        ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let mut abs_path = std::fs::canonicalize(f)?;
        let _ = abs_path.pop();
        let index_file_name = format!("{}.index.v1-rust-cache", f.file_name().unwrap().to_str().unwrap());
        let index_file_path = abs_path.join(index_file_name);
        let triples = if index_file_path.exists() {
            let pos = reader.stream_position()?;
            match Self::load_with_cache(&mut reader, &index_file_path) {
                Ok(triples) => triples,
                Err(e) => {
                    warn!("error loading cache, overwriting: {e}");
                    reader.seek(SeekFrom::Start(pos))?;
                    Self::load_without_cache(&mut reader, &index_file_path)?
                }
            }
        } else {
            Self::load_without_cache(&mut reader, &index_file_path)?
        };

        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { header, dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    #[cfg(feature = "cache")]
    fn load_without_cache<R: std::io::BufRead>(
        mut reader: R, index_file_path: &std::path::PathBuf,
    ) -> Result<TriplesBitmap> {
        use log::warn;

        debug!("no cache detected, generating index");
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        debug!("index generated, saving cache to {}", index_file_path.display());
        if let Err(e) = Self::write_cache(index_file_path, &triples) {
            warn!("error trying to save cache to file: {e}");
        }
        Ok(triples)
    }

    #[cfg(feature = "cache")]
    fn load_with_cache<R: std::io::BufRead>(
        mut reader: R, index_file_path: &std::path::PathBuf,
    ) -> core::result::Result<TriplesBitmap, Box<dyn std::error::Error>> {
        // load cached index
        debug!("hdt file cache detected, loading from {}", index_file_path.display());
        let index_source = File::open(index_file_path)?;
        let mut index_reader = std::io::BufReader::new(index_source);
        let triples_ci = ControlInfo::read(&mut reader)?;
        Ok(TriplesBitmap::load_cache(&mut index_reader, &triples_ci)?)
    }

    #[cfg(feature = "cache")]
    fn write_cache(
        index_file_path: &std::path::PathBuf, triples: &TriplesBitmap,
    ) -> core::result::Result<(), Box<dyn std::error::Error>> {
        let new_index_file = File::create(index_file_path)?;
        let mut writer = std::io::BufWriter::new(new_index_file);
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
    use std::path::Path;

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

    #[test]
    #[cfg(feature = "sophia")]
    fn read_nt() -> Result<()> {
        init();
        let path = std::path::Path::new("tests/resources/snikmeta.nt");
        if !path.exists() {
            log::info!("Creating test resource snikmeta.nt.");
            let mut writer = std::io::BufWriter::new(File::create(path)?);
            snikmeta()?.write_nt(&mut writer)?;
        }
        let snikmeta_nt = Hdt::read_nt(path)?;

        let snikmeta = snikmeta()?;
        let hdt_triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let nt_triples: Vec<StringTriple> = snikmeta_nt.triples_all().collect();

        assert_eq!(nt_triples, hdt_triples);
        assert_eq!(snikmeta.triples.bitmap_y.dict, snikmeta_nt.triples.bitmap_y.dict);
        snikmeta_check(&snikmeta_nt)?;
        let path = std::path::Path::new("tests/resources/empty.nt");
        let hdt_empty = Hdt::read_nt(path)?;
        let mut buf = Vec::<u8>::new();
        hdt_empty.write(&mut buf)?;
        Hdt::read(std::io::Cursor::new(buf))?;
        Ok(())
    }

    #[test]
    #[cfg(feature = "sophia")]
    fn w3c_tests() -> Result<()> {
        use std::fs;
        use std::path::Path;

        for sparql_test_version in ["sparql10", "sparql11", "sparql12"] {
            let input_files = find_ttl_files(format!("tests/resources/rdf-tests/sparql/{}", sparql_test_version));
            for f in &input_files {
                if f.ends_with("manifest.ttl")
                    || Path::new(f).parent().unwrap().file_name().unwrap() == sparql_test_version
                {
                    continue;
                }
                let parent_folder_name = Path::new(f).parent().unwrap().file_name().unwrap();

                let nt_file_name = format!(
                    "tests/resources/generated/nt/{sparql_test_version}/{:#?}/{}",
                    parent_folder_name,
                    Path::new(f).file_name().unwrap().to_str().unwrap().replace(".ttl", ".nt")
                );
                let nt_file_path = Path::new(&nt_file_name);
                std::fs::create_dir_all(format!(
                    "tests/resources/generated/nt/{sparql_test_version}/{:#?}",
                    parent_folder_name
                ))?;
                ttl_to_nt(f, &nt_file_path)?;
                match Hdt::read_nt(&nt_file_path) {
                    Ok(h) => {
                        #[cfg(feature = "sophia")]
                        {
                            use std::{
                                fs::OpenOptions,
                                io::{BufWriter, Write},
                            };

                            let hdt_file_path = format!(
                                "tests/resources/generated/hdt/{sparql_test_version}/{:#?}/{}",
                                parent_folder_name,
                                Path::new(f).file_name().unwrap().to_str().unwrap().replace(".ttl", ".hdt")
                            );
                            std::fs::create_dir_all(Path::new(&hdt_file_path).parent().unwrap())?;
                            let out_file =
                                OpenOptions::new().create(true).write(true).truncate(true).open(&hdt_file_path)?;
                            let mut writer = BufWriter::new(out_file);
                            h.write(&mut writer)?;
                            writer.flush()?;
                            assert!(Path::new(&hdt_file_path).exists());
                        }
                    }
                    Err(e) => {
                        use crate::triples;

                        matches!(e, Error::Triples(triples::Error::Empty));
                    }
                }
            }
        }
        fs::remove_dir_all("tests/resources/generated")?;
        Ok(())
    }

    fn find_ttl_files<P: AsRef<std::path::Path>>(dir: P) -> Vec<String> {
        walkdir::WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "ttl"))
            .map(|e| e.path().display().to_string())
            .collect()
    }

    fn ttl_to_nt(source_ttl: &str, dest_nt: &Path) -> Result<()> {
        use sophia::api::parser::TripleParser;
        use sophia::api::prelude::TripleSerializer;
        use sophia::api::prelude::TripleSource;
        use sophia::turtle::serializer::nt::NtSerializer;
        use std::io::Write;

        let ttl_file = std::fs::File::open(source_ttl)?;
        let reader = std::io::BufReader::new(ttl_file);

        let nt_file = std::fs::File::options().read(true).write(true).create(true).truncate(true).open(dest_nt)?;

        let mut writer = std::io::BufWriter::new(nt_file);

        let mut sophia_serializer = NtSerializer::new(writer.by_ref());

        let mut graph = sophia::inmem::graph::LightGraph::default();
        let ttl_parser = sophia::turtle::parser::turtle::TurtleParser {
            base: Some(sophia::iri::Iri::new(format!(
                "file://{}",
                std::path::Path::new(source_ttl).file_name().unwrap().to_str().unwrap()
            ))?),
        };

        ttl_parser.parse(reader).add_to_graph(&mut graph)?;

        sophia_serializer.serialize_graph(&graph)?;
        writer.flush()?;
        Ok(())
    }

    fn snikmeta_check(hdt: &Hdt) -> Result<()> {
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
