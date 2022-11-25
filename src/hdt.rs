use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;
use crate::triples::TripleId;
use crate::triples::TripleSect;
use crate::FourSectDict;
use bytesize::ByteSize;
use std::io;
use thiserror::Error;

#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    //header: Header,
    dict: FourSectDict,
    triple_sect: TripleSect,
}

type StringTriple = (String, String, String);

#[derive(Error, Debug)]
#[error("Cannot translate triple ID {t:?} to string triple: {e}")]
pub struct TranslateErr {
    #[source]
    e: DictErr,
    t: TripleId,
}

impl Hdt {
    pub fn new<R: std::io::BufRead>(mut reader: R) -> io::Result<Self> {
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;
        let dict = FourSectDict::read(&mut reader)?;
        let triple_sect = TripleSect::read(&mut reader)?;
        let hdt = Hdt { dict, triple_sect };
        println!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        println!("{hdt:#?}");
        Ok(hdt)
    }

    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triple_sect.size_in_bytes()
    }

    fn translate_id(&self, t: TripleId) -> Result<StringTriple, TranslateErr> {
        let s = self.dict.id_to_string(t.subject_id, &IdKind::Subject).map_err(|e| TranslateErr { e, t })?;
        let p = self.dict.id_to_string(t.predicate_id, &IdKind::Predicate).map_err(|e| TranslateErr { e, t })?;
        let o = self.dict.id_to_string(t.object_id, &IdKind::Object).map_err(|e| TranslateErr { e, t })?;
        Ok((s, p, o))
    }
    /*
    fn translate_ids<'a>(&'a self, it: impl Iterator<Item = TripleId> + 'a) -> impl Iterator<Item = StringTriple> + '_
    {
        it.map(|tid| self.translate_id(tid))
        .filter_map(move |r| r.map_err(|e| eprintln!("Error translating triple {tid:?}: {e}")).ok())
    }
    */
    pub fn triples(&self) -> impl Iterator<Item = StringTriple> + '_ {
        self.triple_sect.read_all_ids().map(|id| self.translate_id(id).unwrap())
    }

    pub fn triples_with(&self, s: &str, kind: &'static IdKind) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        debug_assert_ne!("", s);
        let id = self.dict.string_to_id(s, kind);
        if id == 0 {
            return Box::new(std::iter::empty());
        }
        let owned = s.to_owned();
        Box::new(
            self.triple_sect
                .triples_with_id(id, kind)
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| r.map_err(|e| eprintln!("Error on triple with {kind:?} {owned}: {e}")).ok()),
        )
    }

    // TODO extract common code out of triples_with_...

    /// Get all triples with the given subject and property.
    /// The current implementation queries all triple IDs for the given subject and filters them for the given property.
    /// This method is faster then filtering on translated strings but can be further optimized by creating a special SP iterator.
    pub fn triples_with_sp(&self, s: &str, p: &str) -> Box<dyn Iterator<Item = StringTriple> + '_> {
        let sid = self.dict.string_to_id(s, &IdKind::Subject);
        let pid = self.dict.string_to_id(p, &IdKind::Predicate);
        if sid == 0 || pid == 0 {
            if sid == 0 {
                println!("WARNING SID");
            }
            if pid == 0 {
                println!("WARNING SID");
            }
            return Box::new(std::iter::empty());
        }
        let s_owned = s.to_owned();
        let p_owned = p.to_owned();
        Box::new(
            self.triple_sect
                .triples_with_id(sid, &IdKind::Subject)
                .filter(move |tid| tid.predicate_id == pid)
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
            self.triple_sect
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
            self.triple_sect
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
