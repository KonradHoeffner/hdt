use crate::containers::ControlInfo;
use crate::four_sect_dict::IdKind;
use crate::header::Header;
use crate::triples::BitmapIter;
use crate::triples::TripleId;
use crate::triples::TripleSect;
use crate::FourSectDict;
use bytesize::ByteSize;
use std::fs::File;
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
        println!("{:#?}", hdt);
        Ok(hdt)
    }

    pub fn size_in_bytes(&self) -> usize {
        self.dict.size_in_bytes() + self.triple_sect.size_in_bytes()
    }

    fn translate_ids<'a>(
        &'a self, ids: impl Iterator<Item = TripleId> + 'a,
    ) -> impl Iterator<Item = (String, String, String)> + 'a {
        ids.map(move |id: TripleId| {
            let subject = self.dict.id_to_string(id.subject_id, IdKind::Subject);
            let predicate = self.dict.id_to_string(id.predicate_id, IdKind::Predicate);
            let object = self.dict.id_to_string(id.object_id, IdKind::Object);
            (subject, predicate, object)
        })
    }

    pub fn triples(&self) -> impl Iterator<Item = (String, String, String)> + '_ {
        let mut ids = self.triple_sect.read_all_ids();
        self.translate_ids(ids)
    }

    pub fn triples_with(&self, kind: IdKind, s: &str) -> Box<dyn Iterator<Item = (String, String, String)> + '_> {
        let id = self.dict.string_to_id(s, IdKind::Subject);
        if (id == 0) {
            return Box::new(std::iter::empty());
        }
        match kind {
            IdKind::Subject => Box::new(self.translate_ids(self.triple_sect.triples_with_s(id))),
            IdKind::Predicate => Box::new(self.translate_ids(self.triple_sect.triples_with_p(id))),
            IdKind::Object => Box::new(self.translate_ids(self.triple_sect.triples_with_o(id))),
        }
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
        let mut triples = hdt.triples();
        let v: Vec<(String, String, String)> = triples.collect();
        assert_eq!(v.len(), 327);
        for uri in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta"] {
            let filtered: Vec<_> = v.clone().into_iter().filter(|triple| triple.0 == uri).collect();
            let with_s: Vec<_> = hdt.triples_with(IdKind::Subject, uri).collect();
            assert_eq!(filtered, with_s, "different results between triples() and triples_with_s() for {}", uri);
        }
    }
}
