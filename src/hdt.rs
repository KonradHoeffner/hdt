use crate::containers::rdf::Triple;
use crate::containers::{ControlInfo, control_info};
use crate::four_sect_dict::{DictError, DictReadError, IdKind};
use crate::triples::{ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId, TriplesBitmap};
use crate::vocab::*;
use crate::{FourSectDict, containers};
use crate::{header, header::Header};
use bytesize::ByteSize;
use log::{debug, error};
use std::cmp::Ordering;
use std::collections::BTreeSet;
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

type StringTriple = (Arc<str>, Arc<str>, Arc<str>);

/// The error type for the `translate_id` method.
#[derive(thiserror::Error, Debug)]
#[error("cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateError {
    #[source]
    e: DictError,
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
    FourSectDict(#[from] DictReadError),
    #[error("failed to read HDT triples section")]
    Triples(#[from] crate::triples::Error),
    #[error("failed to validate HDT dictionary")]
    DictionaryValidationErrorTodo(#[from] std::io::Error),
}

#[derive(Clone, Debug)]
pub struct Options {
    pub block_size: usize,
    pub order: String,
}
impl Default for Options {
    fn default() -> Self {
        Options { block_size: 16, order: "SPO".to_string() }
    }
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
    /// # Example
    /// ```
    /// let path = std::path::Path::new("example.nt");
    /// let hdt = hdt::Hdt::read_nt(path).unwrap();
    ///// let hdt = hdt::Hdt::read_nt(std::io::BufReader::new(file)).unwrap();
    /// ```
    pub fn read_nt(f: &std::path::Path) -> Result<Self> {
        //pub fn read_nt<R: std::io::BufRead>(mut reader: R) -> Result<Self> {
        let source = std::fs::File::open(f)?;
        let mut reader = std::io::BufReader::new(source);
        let opts = Options::default();
        let (dictionary, mut encoded_triples) = FourSectDict::read_nt(&mut reader, opts.clone())?;
        let num_triples = encoded_triples.len();

        sort_triples_spo(&mut encoded_triples);

        let mut converted_hdt = Hdt {
            header: Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() },
            dict: dictionary,
            triples: TriplesBitmap::from_triples(encoded_triples),
        };

        converted_hdt.build_header(f, opts, num_triples);
        debug!("HDT size in memory {}, details:", ByteSize(converted_hdt.size_in_bytes() as u64));
        debug!("{converted_hdt:#?}");
        Ok(converted_hdt)
    }

    /// populated HDT header fields
    /// TODO are all of these headers required for HDT spec? Populating same triples as those in C++ version for now
    fn build_header(&mut self, f: &std::path::Path, opts: Options, num_triples: usize) {
        let mut headers = BTreeSet::new();
        // libhdt/src/hdt/BasicHDT.cpp::fillHeader()

        // uint64_t origSize = header->getPropertyLong(statisticsNode.c_str(), HDTVocabulary::ORIGINAL_SIZE.c_str());

        // header->clear();
        let file_iri = format!("file://{}", f.canonicalize().unwrap().display());
        let base_iri = containers::rdf::Id::Named(file_iri);
        // // BASE
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::HDT_DATASET);
        headers.insert(Triple::new(
            base_iri.clone(),
            RDF_TYPE.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(HDT_CONTAINER.to_owned())),
        ));

        // // VOID
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::VOID_DATASET);
        headers.insert(Triple::new(
            base_iri.clone(),
            RDF_TYPE.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(VOID_DATASET.to_owned())),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_TRIPLES, triples->getNumberOfElements());
        headers.insert(Triple::new(
            base_iri.clone(),
            VOID_TRIPLES.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(num_triples.to_string())),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_PROPERTIES, dictionary->getNpredicates());
        headers.insert(Triple::new(
            base_iri.clone(),
            VOID_PROPERTIES.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                self.dict.predicates.num_strings.to_string(),
            )),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_SUBJECTS, dictionary->getNsubjects());
        headers.insert(Triple::new(
            base_iri.clone(),
            VOID_DISTINCT_SUBJECTS.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                (self.dict.subjects.num_strings + self.dict.shared.num_strings).to_string(),
            )),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_OBJECTS, dictionary->getNobjects());
        headers.insert(Triple::new(
            base_iri.clone(),
            VOID_DISTINCT_OBJECTS.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                (self.dict.objects.num_strings + self.dict.shared.num_strings).to_string(),
            )),
        ));
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = containers::rdf::Id::Blank("statistics".to_owned());
        let pub_id = containers::rdf::Id::Blank("publicationInformation".to_owned());
        let format_id = containers::rdf::Id::Blank("format".to_owned());
        let dict_id = containers::rdf::Id::Blank("dictionary".to_owned());
        let triples_id = containers::rdf::Id::Blank("triples".to_owned());
        // header->insert(baseUri, HDTVocabulary::HDT_STATISTICAL_INFORMATION,	statisticsNode);
        headers.insert(Triple::new(
            base_iri.clone(),
            HDT_STATISTICAL_INFORMATION.to_owned(),
            containers::rdf::Term::Id(stats_id.clone()),
        ));
        // header->insert(baseUri, HDTVocabulary::HDT_PUBLICATION_INFORMATION,	publicationInfoNode);
        headers.insert(Triple::new(
            base_iri.clone(),
            HDT_STATISTICAL_INFORMATION.to_owned(),
            containers::rdf::Term::Id(pub_id.clone()),
        ));
        // header->insert(baseUri, HDTVocabulary::HDT_FORMAT_INFORMATION, formatNode);
        headers.insert(Triple::new(
            base_iri.clone(),
            HDT_FORMAT_INFORMATION.to_owned(),
            containers::rdf::Term::Id(format_id.clone()),
        ));
        // header->insert(formatNode, HDTVocabulary::HDT_DICTIONARY, dictNode);
        headers.insert(Triple::new(
            format_id.clone(),
            HDT_DICTIONARY.to_owned(),
            containers::rdf::Term::Id(dict_id.clone()),
        ));
        // header->insert(formatNode, HDTVocabulary::HDT_TRIPLES, triplesNode);
        headers.insert(Triple::new(
            format_id,
            HDT_TRIPLES.to_owned(),
            containers::rdf::Term::Id(triples_id.clone()),
        ));

        // DICTIONARY
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_NUMSHARED, getNshared());
        headers.insert(Triple::new(
            dict_id.clone(),
            HDT_DICT_SHARED_SO.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                self.dict.shared.num_strings.to_string(),
            )),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_MAPPING, this->mapping);
        headers.insert(Triple::new(
            dict_id.clone(),
            HDT_DICT_MAPPING.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new("1".to_owned())),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_SIZE_STRINGS, size());
        headers.insert(Triple::new(
            dict_id.clone(),
            HDT_DICT_SIZE_STRINGS.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                ByteSize(self.dict.size_in_bytes() as u64).to_string(),
            )),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_BLOCK_SIZE, this->blocksize);
        headers.insert(Triple::new(
            dict_id,
            HDT_DICT_BLOCK_SIZE.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(opts.block_size.to_string())),
        ));

        // TRIPLES
        // header.insert(rootNode, HDTVocabulary::TRIPLES_TYPE, getType());
        headers.insert(Triple::new(
            triples_id.clone(),
            DC_TERMS_FORMAT.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(HDT_TYPE_BITMAP.to_owned())),
        ));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_NUM_TRIPLES, getNumberOfElements() );
        headers.insert(Triple::new(
            triples_id.clone(),
            HDT_NUM_TRIPLES.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(num_triples.to_string())),
        ));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_ORDER, getOrderStr(order) );
        headers.insert(Triple::new(
            triples_id,
            HDT_TRIPLES_ORDER.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(opts.order)),
        ));

        // // Sizes
        let meta = std::fs::File::open(f).unwrap().metadata().unwrap();
        // header->insert(statisticsNode, HDTVocabulary::ORIGINAL_SIZE, origSize);
        headers.insert(Triple::new(
            stats_id.clone(),
            HDT_ORIGINAL_SIZE.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(meta.len().to_string())),
        ));
        // header->insert(statisticsNode, HDTVocabulary::HDT_SIZE, getDictionary()->size() + getTriples()->size());
        headers.insert(Triple::new(
            stats_id,
            HDT_SIZE.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(
                ByteSize(self.size_in_bytes() as u64).to_string(),
            )),
        ));

        // // Current time
        // struct tm* today = localtime(&now);
        // strftime(date, 40, "%Y-%m-%dT%H:%M:%S%z", today);
        // header->insert(publicationInfoNode, HDTVocabulary::DUBLIN_CORE_ISSUED, date);
        let now = chrono::Utc::now(); // Get current local datetime
        let datetime_str = now.format("%Y-%m-%dT%H:%M:%S%z").to_string(); // Format as string
        headers.insert(Triple::new(
            pub_id,
            DC_TERMS_ISSUED.to_owned(),
            containers::rdf::Term::Literal(containers::rdf::Literal::new(datetime_str)),
        ));

        // TODO fix header length

        self.header.body = headers;
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
    ///     println!("{:?}", hdt.triples().next().expect("no triple in the graph"));
    /// }
    /// ```
    pub fn triples(&self) -> impl Iterator<Item = StringTriple> + '_ {
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
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        let oid = self.dict.string_to_id(o, &IdKind::Object);
        // predicate or object not in dictionary, iterator would interpret 0 as variable
        if pid == 0 || oid == 0 {
            return Box::new(iter::empty());
        }
        // needed for extending the lifetime of the parameters into the iterator for error messages
        let p_owned = p.to_owned();
        let o_owned = o.to_owned();
        Box::new(
            PredicateObjectIter::new(&self.triples, pid, oid)
                .map(move |sid| self.dict.id_to_string(sid, &IdKind::Subject))
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
    ///     .next().expect("no capital found").2);
    /// }
    /// ```
    pub fn triples_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = StringTriple> + 'a> {
        let xso: Option<(Arc<str>, usize)> =
            sp.map(|s| (Arc::from(s), self.dict.string_to_id(s, &IdKind::Subject)));
        let xpo: Option<(Arc<str>, usize)> =
            pp.map(|p| (Arc::from(p), self.dict.string_to_id(p, &IdKind::Predicate)));
        let xoo: Option<(Arc<str>, usize)> =
            op.map(|o| (Arc::from(o), self.dict.string_to_id(o, &IdKind::Object)));
        if [&xso, &xpo, &xoo].into_iter().flatten().any(|x| x.1 == 0) {
            // at least one term does not exist in the graph
            return Box::new(iter::empty());
        }
        // TODO: improve error handling
        let mut cache = TripleCache::new(self);
        match (xso, xpo, xoo) {
            (Some(s), Some(p), Some(o)) => {
                if SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, p.1, o.1)).next().is_some() {
                    Box::new(iter::once((s.0, p.0, o.0)))
                } else {
                    Box::new(iter::empty())
                }
            }
            (Some(s), Some(p), None) => {
                Box::new(SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, p.1, 0)).map(move |t| {
                    (
                        s.0.clone(),
                        p.0.clone(),
                        Arc::from(self.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                    )
                }))
            }
            (Some(s), None, Some(o)) => {
                Box::new(SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, 0, o.1)).map(move |t| {
                    (
                        s.0.clone(),
                        Arc::from(self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                        o.0.clone(),
                    )
                }))
            }
            (Some(s), None, None) => {
                Box::new(SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, 0, 0)).map(move |t| {
                    (
                        s.0.clone(),
                        cache.get_p_string(t.predicate_id).unwrap(),
                        cache.get_o_string(t.object_id).unwrap(),
                    )
                }))
            }
            (None, Some(p), Some(o)) => {
                Box::new(PredicateObjectIter::new(&self.triples, p.1, o.1).map(move |sid| {
                    (Arc::from(self.dict.id_to_string(sid, &IdKind::Subject).unwrap()), p.0.clone(), o.0.clone())
                }))
            }
            (None, Some(p), None) => Box::new(PredicateIter::new(&self.triples, p.1).map(move |t| {
                (cache.get_s_string(t.subject_id).unwrap(), p.0.clone(), cache.get_o_string(t.object_id).unwrap())
            })),
            (None, None, Some(o)) => Box::new(ObjectIter::new(&self.triples, o.1).map(move |t| {
                (
                    cache.get_s_string(t.subject_id).unwrap(),
                    cache.get_p_string(t.predicate_id).unwrap(),
                    o.0.clone(),
                )
            })),
            (None, None, None) => Box::new(self.triples()),
        }
    }
}

/// Function to sort a vector of Triples in SPO order
fn sort_triples_spo(triples: &mut [TripleId]) {
    triples.sort_by(spo_comparator);
}

fn spo_comparator(a: &TripleId, b: &TripleId) -> Ordering {
    let subject_order = a.subject_id.cmp(&b.subject_id);
    if subject_order != Ordering::Equal {
        return subject_order;
    }

    let predicate_order = a.predicate_id.cmp(&b.predicate_id);
    if predicate_order != Ordering::Equal {
        return predicate_order;
    }

    a.object_id.cmp(&b.object_id)
}

/// A TripleCache stores the `Arc<str>` of the last returned triple
#[derive(Clone, Debug)]
pub struct TripleCache<'a> {
    hdt: &'a super::Hdt,
    idx: [usize; 3],
    arc: [Option<Arc<str>>; 3],
}

impl<'a> TripleCache<'a> {
    /// Build a new [`TripleCache`] for the given [`Hdt`]
    pub const fn new(hdt: &'a super::Hdt) -> Self {
        TripleCache { hdt, idx: [0; 3], arc: [None, None, None] }
    }

    /// Get the string representation of the subject `sid`.
    pub fn get_s_string(&mut self, sid: usize) -> core::result::Result<Arc<str>, DictError> {
        self.get_x_string(sid, 0, &IdKind::Subject)
    }

    /// Get the string representation of the predicate `pid`.
    pub fn get_p_string(&mut self, pid: usize) -> core::result::Result<Arc<str>, DictError> {
        self.get_x_string(pid, 1, &IdKind::Predicate)
    }

    /// Get the string representation of the object `oid`.
    pub fn get_o_string(&mut self, oid: usize) -> core::result::Result<Arc<str>, DictError> {
        self.get_x_string(oid, 2, &IdKind::Object)
    }

    /// Translate a triple of indexes into a triple of strings.
    pub fn translate(&mut self, t: TripleId) -> core::result::Result<StringTriple, TranslateError> {
        Ok((
            self.get_s_string(t.subject_id).map_err(|e| TranslateError { e, t })?,
            self.get_p_string(t.predicate_id).map_err(|e| TranslateError { e, t })?,
            self.get_o_string(t.object_id).map_err(|e| TranslateError { e, t })?,
        ))
    }

    fn get_x_string(
        &mut self, i: usize, pos: usize, kind: &'static IdKind,
    ) -> core::result::Result<Arc<str>, DictError> {
        debug_assert!(i != 0);
        if self.idx[pos] == i {
            Ok(self.arc[pos].as_ref().unwrap().clone())
        } else {
            let ret: Arc<str> = self.hdt.dict.id_to_string(i, kind)?.into();
            self.arc[pos] = Some(ret.clone());
            self.idx[pos] = i;
            Ok(ret)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use color_eyre::Result;
    use fs_err::File;
    use pretty_assertions::{assert_eq, assert_ne};

    #[test]
    fn write() -> Result<()> {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename)?;
        let hdt = Hdt::read(std::io::BufReader::new(file))?;
        triples(&hdt)?;
        let mut buf = Vec::<u8>::new();
        hdt.write(&mut buf)?;
        let hdt2 = Hdt::read(std::io::Cursor::new(buf))?;
        triples(&hdt2)?;
        Ok(())
    }

    #[test]
    fn nt_to_hdt() -> Result<()> {
        init();
        //let filename = "tests/resources/snikmeta.nt";
        let filename = "tests/resources/apple.nt";
        let hdt = Hdt::read_nt(std::path::Path::new(filename))?;
        hdt.write(&mut std::io::BufWriter::new(File::create("/tmp/fromnt.hdt")?))?;
        assert_eq!(hdt.dict.shared.num_strings, 1);
        assert_eq!(hdt.dict.predicates.num_strings, 7);
        //triples(&hdt)?;
        Ok(())
    }

    fn triples(hdt: &Hdt) -> Result<()> {
        let triples = hdt.triples();
        let v: Vec<StringTriple> = triples.collect();
        assert_eq!(v.len(), 328);
        assert_eq!(v, hdt.triples_with_pattern(None, None, None).collect::<Vec<_>>(), "all triples not equal ???");
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", &IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0.as_ref() == uri).collect();
            let with_s: Vec<_> = hdt.triples_with_pattern(Some(uri), None, None).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
        let s = "http://www.snik.eu/ontology/meta/Top";
        let p = "http://www.w3.org/2000/01/rdf-schema#label";
        let o = "\"top class\"@en";
        let triple_vec = vec![(Arc::from(s), Arc::from(p), Arc::from(o))];
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
        .map(|p| (Arc::from(meta), Arc::from(p), Arc::from(snikeu)))
        .collect::<Vec<_>>();
        assert_eq!(
            triple_vec,
            hdt.triples_with_pattern(Some(meta), None, Some(snikeu)).collect::<Vec<_>>(),
            "S?O multiple"
        );
        let s = "http://www.snik.eu/ontology/meta/хобби-N-0";
        let o = "\"ХОББИ\"@ru";
        let triple_vec = vec![(Arc::from(s), Arc::from(p), Arc::from(o))];
        assert_eq!(triple_vec, hdt.triples_with_pattern(Some(s), Some(p), None).collect::<Vec<_>>(),);
        Ok(())
    }
}
