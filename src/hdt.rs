use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;
use crate::triples::{ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId, TriplesBitmap};
use crate::FourSectDict;
use bytesize::ByteSize;
use eyre::WrapErr;
use log::{debug, error};
use std::error::Error;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::{fs, iter};
use thiserror::Error;

/// In-memory representation of an RDF graph loaded from an HDT file.
/// Allows queries by triple patterns.
#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    //header: Header,
    /// in-memory representation of dictionary
    pub dict: FourSectDict,
    /// in-memory representation of triples
    pub triples: TriplesBitmap,
}

type StringTriple = (Arc<str>, Arc<str>, Arc<str>);

/// The error type for the `translate_id` method.
#[derive(Error, Debug)]
#[error("Cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateErr {
    #[source]
    e: DictErr,
    t: TripleId,
}

impl Hdt {
    /// Creates an immutable HDT instance containing the dictionary and triples from the given reader.
    /// The reader must point to the beginning of the data of an HDT file as produced by hdt-cpp.
    /// FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order is the only supported implementation.
    /// The format is specified at <https://www.rdfhdt.org/hdt-binary-format/>, however there are some deviations.
    /// The initial HDT specification at <http://www.w3.org/Submission/2011/03/> is outdated and not supported.
    /// # Example
    /// ```
    /// let file = std::fs::File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    /// let hdt = hdt::Hdt::new(std::io::BufReader::new(file)).unwrap();
    /// ```
    pub fn new<R: std::io::BufRead>(mut reader: R) -> Result<Self, Box<dyn Error>> {
        ControlInfo::read(&mut reader).wrap_err("Failed to read HDT control info")?;
        Header::read(&mut reader).wrap_err("Failed to read HDT header")?;
        let unvalidated_dict = FourSectDict::read(&mut reader).wrap_err("Failed to read HDT dictionary")?;
        let triples = TriplesBitmap::read_sect(&mut reader).wrap_err("Failed to read HDT triples section")?;
        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    pub fn new_from_file(f: &Path) -> Result<Self, Box<dyn Error>> {
        let source = std::fs::File::open(f)?;
        let mut reader = std::io::BufReader::new(source);
        ControlInfo::read(&mut reader).wrap_err("Failed to read HDT control info")?;
        Header::read(&mut reader).wrap_err("Failed to read HDT header")?;
        let unvalidated_dict = FourSectDict::read(&mut reader).wrap_err("Failed to read HDT dictionary")?;
        let abs_path = fs::canonicalize(f)?;
        let index_file = format!(
            "{}/{}.index.v1-rust-cache",
            abs_path.parent().unwrap().display(),
            f.file_name().unwrap().to_str().unwrap().to_string()
        );
        let triples = if Path::new(&index_file).exists() {
            // load cached index
            debug!("hdt file cache detected, loading from {index_file}");
            let index_source = std::fs::File::open(index_file)?;
            let mut index_reader = std::io::BufReader::new(index_source);
            let triples_ci = ControlInfo::read(&mut reader)?;
            TriplesBitmap::load_cache(&mut index_reader, triples_ci)?
        } else {
            debug!("no cache detected, generating index");
            let triples = TriplesBitmap::read_sect(&mut reader).wrap_err("Failed to read HDT triples section")?;
            debug!("index generated, saving cache to {index_file}");
            let new_index_file = std::fs::File::create(index_file)?;
            let mut writer = BufWriter::new(new_index_file);
            bincode::serialize_into(&mut writer, &triples).expect("Serialization failed");
            writer.flush()?;
            triples
        };

        let dict = unvalidated_dict.validate()?;
        let hdt = Hdt { dict, triples };
        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
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
    pub fn get_s_string(&mut self, sid: usize) -> Result<Arc<str>, DictErr> {
        self.get_x_string(sid, 0, &IdKind::Subject)
    }

    /// Get the string representation of the predicate `pid`.
    pub fn get_p_string(&mut self, pid: usize) -> Result<Arc<str>, DictErr> {
        self.get_x_string(pid, 1, &IdKind::Predicate)
    }

    /// Get the string representation of the object `oid`.
    pub fn get_o_string(&mut self, oid: usize) -> Result<Arc<str>, DictErr> {
        self.get_x_string(oid, 2, &IdKind::Object)
    }

    /// Translate a triple of indexes into a triple of strings.
    pub fn translate(&mut self, t: TripleId) -> Result<StringTriple, TranslateErr> {
        Ok((
            self.get_s_string(t.subject_id).map_err(|e| TranslateErr { e, t })?,
            self.get_p_string(t.predicate_id).map_err(|e| TranslateErr { e, t })?,
            self.get_o_string(t.object_id).map_err(|e| TranslateErr { e, t })?,
        ))
    }

    fn get_x_string(&mut self, i: usize, pos: usize, kind: &'static IdKind) -> Result<Arc<str>, DictErr> {
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
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::File;

    #[test]
    fn triples() {
        init();
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename).expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
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
    }
}
