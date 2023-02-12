use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;
use crate::triples::{ObjectIter, PredicateIter, PredicateObjectIter, SubjectIter, TripleId, TriplesBitmap};
use crate::FourSectDict;
use bytesize::ByteSize;
use log::{debug, error};
use mownstr::MownStr;
use std::io;
use std::iter;
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

type StringTriple<'a> = (MownStr<'a>, MownStr<'a>, MownStr<'a>);

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
    pub fn new<R: std::io::BufRead>(mut reader: R) -> io::Result<Self> {
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;
        let unvalidated_dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
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

    /// Don't use this for many triples with shared values as you won't benefit from deduplication.
    fn translate_id(&self, t: TripleId) -> Result<StringTriple, TranslateErr> {
        let s = self.dict.id_to_string(t.subject_id, &IdKind::Subject).map_err(|e| TranslateErr { e, t })?;
        let p = self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).map_err(|e| TranslateErr { e, t })?;
        let o = self.dict.id_to_string(t.object_id, &IdKind::Object).map_err(|e| TranslateErr { e, t })?;
        Ok((s.into(), p.into(), o.into()))
    }

    /// An iterator visiting *all* triples as strings in order.
    /// Using this method with a filter can be inefficient for large graphs,
    /// because the strings are stored in compressed form and must be decompressed and allocated.
    /// Whenever possible, use [`Hdt::triples_with_pattern`] instead.
    pub fn triples(&self) -> impl Iterator<Item = StringTriple> + '_ {
        // TODO deduplicate
        self.triples.into_iter().map(|id| self.translate_id(id).unwrap())
    }

    /// Get all objects with the given subject and property.
    pub fn objects_with_sp(&self, s: &str, p: &str) -> Box<dyn Iterator<Item = String> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        if sid == 0 || pid == 0 {
            return Box::new(iter::empty());
        }
        let s_owned = s.to_owned();
        let p_owned = p.to_owned();
        Box::new(
            SubjectIter::with_pattern(&self.triples, &TripleId::new(sid, pid, 0))
                .map(move |tid| self.dict.id_to_string(tid.object_id, &IdKind::Object))
                .filter_map(move |r| {
                    r.map_err(|e| error!("Error on triple with subject {s_owned} and property {p_owned}: {e}"))
                        .ok()
                }),
        )
    }

    /// Get all subjects with the given property and object (?PO pattern).
    /// Use this over `triples_with_pattern(None,Some(p),Some(o))` if you don't need whole triples.
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
    /// For example, `triples_with_pattern(None, Some(p), Some(o)` answers an ?PO pattern.
    pub fn triples_with_pattern<'a>(
        &'a self, sp: Option<&'a str>, pp: Option<&'a str>, op: Option<&'a str>,
    ) -> Box<dyn Iterator<Item = StringTriple<'a>> + '_> {
        let xso = sp.map(|s| (MownStr::from_str(s), self.dict.string_to_id(s, &IdKind::Subject)));
        let xpo = pp.map(|p| (MownStr::from_str(p), self.dict.string_to_id(p, &IdKind::Predicate)));
        let xoo = op.map(|o| (MownStr::from_str(o), self.dict.string_to_id(o, &IdKind::Object)));
        if [&xso, &xpo, &xoo].into_iter().flatten().any(|x| x.1 == 0) {
            // at least one term does not exist in the graph
            return Box::new(iter::empty());
        }
        // TODO: improve error handling
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
                        MownStr::from(self.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                    )
                }))
            }
            (Some(s), None, Some(o)) => {
                Box::new(SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, 0, o.1)).map(move |t| {
                    (
                        s.0.clone(),
                        MownStr::from(self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                        o.0.clone(),
                    )
                }))
            }
            (Some(s), None, None) => {
                Box::new(SubjectIter::with_pattern(&self.triples, &TripleId::new(s.1, 0, 0)).map(move |t| {
                    (
                        s.0.clone(),
                        MownStr::from(self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                        MownStr::from(self.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                    )
                }))
            }
            (None, Some(p), Some(o)) => {
                Box::new(PredicateObjectIter::new(&self.triples, p.1, o.1).map(move |sid| {
                    (
                        MownStr::from(self.dict.id_to_string(sid, &IdKind::Subject).unwrap()),
                        p.0.clone(),
                        o.0.clone(),
                    )
                }))
            }
            (None, Some(p), None) => Box::new(PredicateIter::new(&self.triples, p.1).map(move |t| {
                (
                    MownStr::from(self.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap()),
                    p.0.clone(),
                    MownStr::from(self.dict.id_to_string(t.object_id, &IdKind::Object).unwrap()),
                )
            })),
            (None, None, Some(o)) => Box::new(ObjectIter::new(&self.triples, o.1).map(move |t| {
                (
                    MownStr::from(self.dict.id_to_string(t.subject_id, &IdKind::Subject).unwrap()),
                    MownStr::from(self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).unwrap()),
                    o.0.clone(),
                )
            })),
            (None, None, None) => Box::new(self.triples()),
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
        assert_eq!(v.len(), 327);
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
        let triple_vec = vec![(MownStr::from(s), MownStr::from(p), MownStr::from(o))];
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
        .map(|p| (MownStr::from(meta), MownStr::from(p), MownStr::from(snikeu)))
        .collect::<Vec<_>>();
        assert_eq!(
            triple_vec,
            hdt.triples_with_pattern(Some(meta), None, Some(snikeu)).collect::<Vec<_>>(),
            "S?O multiple"
        );
    }
}
