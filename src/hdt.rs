use crate::containers::ControlInfo;
use crate::four_sect_dict::{DictErr, IdKind};
use crate::header::Header;

use crate::triples::TripleId;
use crate::triples::TripleSect;
use crate::FourSectDict;
use bytesize::ByteSize;

use std::io;

#[derive(Debug)]
pub struct Hdt {
    //global_ci: ControlInfo,
    //header: Header,
    dict: FourSectDict,
    triple_sect: TripleSect,
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

    fn translate_id(&self, t: TripleId) -> Result<(String, String, String), DictErr> {
        let subject = self.dict.id_to_string(t.subject_id, IdKind::Subject)?;
        let predicate = self.dict.id_to_string(t.predicate_id, IdKind::Predicate)?;
        let object = self.dict.id_to_string(t.object_id, IdKind::Object)?;
        Ok((subject, predicate, object))
    }

    pub fn triples(&self) -> impl Iterator<Item = (String, String, String)> + '_ {
        self.triple_sect.read_all_ids().map(|id| self.translate_id(id).unwrap())
    }

    pub fn triples_with(&self, s: &str, kind: IdKind) -> Box<dyn Iterator<Item = (String, String, String)> + '_> {
        debug_assert_ne!("", s);
        let id = self.dict.string_to_id(s, kind.clone());
        if id == 0 {
            return Box::new(std::iter::empty());
        }
        let owned = s.to_owned();
        Box::new(
            self.triple_sect
                .triples_with_id(id, kind.clone())
                .map(move |tid| self.translate_id(tid))
                .filter_map(move |r| r.map_err(|e| eprintln!("Error on triple with {kind:?} {owned}: {e}")).ok()),
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
        let v: Vec<(String, String, String)> = triples.collect();
        assert_eq!(v.len(), 327);
        assert_ne!(0, hdt.dict.string_to_id("http://www.snik.eu/ontology/meta", IdKind::Subject));
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta", "doesnotexist"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0 == uri).collect();
            let with_s: Vec<_> = hdt.triples_with(uri, IdKind::Subject).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
    }
}
