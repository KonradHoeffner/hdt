use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;
use crate::triples::{BitmapIter, TripleId, TriplesBitmap};
use crate::FourSectDict;
use bytesize::ByteSize;
use std::io;
use thiserror::Error;
use rayon::prelude::*;

/// In-memory representation of an RDF graph loaded from an HDT file.
/// Allows queries by triple patterns.
#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    //header: Header,
    dict: FourSectDict,
    triples: TriplesBitmap,
}

type StringTriple = (String, String, String);

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
        let dict = FourSectDict::read(&mut reader)?;
        let triples = TriplesBitmap::read_sect(&mut reader)?;
        let hdt = Hdt { dict, triples };
        println!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        println!("{hdt:#?}");
        Ok(hdt)
    }

    /// Recursive size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triples.size_in_bytes()
    }

    fn translate_id(&self, t: TripleId) -> Result<StringTriple, TranslateErr> {
        let s = self.dict.id_to_string(t.subject_id, &IdKind::Subject).map_err(|e| TranslateErr { e, t })?;
        let p = self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).map_err(|e| TranslateErr { e, t })?;
        let o = self.dict.id_to_string(t.object_id, &IdKind::Object).map_err(|e| TranslateErr { e, t })?;
        Ok((s, p, o))
    }

    /// An iterator visiting *all* triples as strings in order.
    /// Using this method with a filter can be inefficient for large graphs,
    /// because the strings are stored in compressed form and must be decompressed and allocated.
    /// Whenever possible, use [`Hdt::triples_with`] instead.
    pub fn triples(&self) -> impl Iterator<Item = StringTriple> + '_ {
        self.triples.into_iter().map(|id| self.translate_id(id).unwrap())
    }

    /// An iterator visiting all triples for a given triple pattern with exactly two variables, i.e. either given subject, property or object.
    /// Returns translated triples as strings.
    /// If the subject is given, you can also use [`BitmapIter::with_pattern`] with a TripleId where property and object are 0.
    /// Much more effient than filtering the result of [`Hdt::triples`].
    /// If you want to query triple patterns with only one variable, use `triples_with_sp` etc. instead.
    pub fn triples_with(&self, s: &str, kind: &'static IdKind) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        debug_assert_ne!("", s);
        let id = self.dict.string_to_id(s, kind);
        if id == 0 {
            return Box::new(std::iter::empty());
        }
        let owned = s.to_owned();
        Box::new(
            self.triples
                .triples_with_id(id, kind)
                .par_bridge()
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| r.map_err(|e| eprintln!("Error on triple with {kind:?} {owned}: {e}")).ok()),
        )
    }

    // TODO extract common code out of triples_with_...

    /// Get all triples with the given subject and property.
    pub fn triples_with_sp(&self, s: &str, p: &str) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        if sid == 0 || pid == 0 {
            return Box::new(std::iter::empty());
        }
        let s_owned = s.to_owned();
        let p_owned = p.to_owned();
        Box::new(
            BitmapIter::with_pattern(&self.triples, &TripleId::new(sid, pid, 0))
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| {
                    r.map_err(|e| eprintln!("Error on triple with subject {s_owned} and property {p_owned}: {e}"))
                        .ok()
                }),
        )
    }

    /// Get all triples with the given subject and object.
    /// The current implementation queries all triple IDs for the given subject and filters them for the given object.
    /// This method is faster then filtering on translated strings but can be further optimized by creating a special SO iterator.
    pub fn triples_with_so(&self, s: &str, o: &str) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let oid = self.dict.string_to_id(o, &IdKind::Object);
        if sid == 0 || oid == 0 {
            return Box::new(std::iter::empty());
        }
        let s_owned = s.to_owned();
        let o_owned = o.to_owned();
        Box::new(
            self.triples
                .triples_with_id(sid, &IdKind::Subject)
                .filter(move |tid| tid.object_id == oid)
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| {
                    r.map_err(|e| eprintln!("Error on triple with subject {s_owned} and object {o_owned}: {e}"))
                        .ok()
                }),
        )
    }

    /// Get all triples with the given property and object.
    /// The current implementation queries all triple IDs for the given property and filters them for the given object.
    /// This method is faster then filtering on translated strings but can be further optimized by creating a special PO iterator.
    pub fn triples_with_po(&self, p: &str, o: &str) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        let oid = self.dict.string_to_id(o, &IdKind::Object);
        if pid == 0 || oid == 0 {
            return Box::new(std::iter::empty());
        }
        let p_owned = p.to_owned();
        let o_owned = o.to_owned();
        Box::new(
            self.triples
                .triples_with_id(pid, &IdKind::Predicate)
                .filter(move |tid| tid.object_id == oid)
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| {
                    r.map_err(|e| eprintln!("Error on triple with property {p_owned} and object {o_owned}: {e}"))
                        .ok()
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::File;

    #[test]
    fn triples() {
        let filename = "tests/resources/snikmeta.hdt";
        let file = File::open(filename).expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let triples = hdt.triples();
        let v: Vec<StringTriple> = triples.collect();
        assert_eq!(v.len(), 327);
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", &IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0 == uri).collect();
            let with_s: Vec<_> = hdt.triples_with(uri, &IdKind::Subject).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
        let s = "http://www.snik.eu/ontology/meta/Top";
        let p = "http://www.w3.org/2000/01/rdf-schema#label";
        let o = "\"top class\"@en";
        let triple_vec = vec![(s.to_owned(), p.to_owned(), o.to_owned())];
        assert_eq!(triple_vec, hdt.triples_with_sp(s, p).collect::<Vec<_>>());
        assert_eq!(triple_vec, hdt.triples_with_so(s, o).collect::<Vec<_>>());
        assert_eq!(triple_vec, hdt.triples_with_po(p, o).collect::<Vec<_>>());
    }
}
