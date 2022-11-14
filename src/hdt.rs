use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::dict::IdKind;
use crate::header::Header;
use crate::triples::BitmapIter;
use crate::triples::TripleId;
use crate::triples::TripleSect;
use std::fs::File;
use std::io;

pub struct Hdt {
    global_ci: ControlInfo,
    header: Header,
    dict: Dict,
    triple_sect: TripleSect,
}

impl Hdt {
    pub fn new<R: std::io::BufRead>(mut reader: R) -> io::Result<Self> {
        let global_ci = ControlInfo::read(&mut reader)?;
        let header = Header::read(&mut reader)?;
        let dict = Dict::read(&mut reader)?;
        let triple_sect = TripleSect::read(&mut reader)?;
        Ok(Hdt { global_ci, header, dict, triple_sect })
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
    use std::fs::File;

    #[test]
    fn triples() {
        let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
        //let file = File::open("tests/resources/snik.hdt").expect("error opening file");
        // let file = File::open("data/wordnet.hdt").expect("error opening file");
        //let file = File::open("tests/resources/qbench2.hdt").expect("error opening file");
        //let file = File::open("tests/resources/lscomplete20143.hdt").expect("error opening file");
        let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
        let mut triples = hdt.triples();
        let v: Vec<(String, String, String)> = triples.collect();
        assert_eq!(v.len(), 242256);
        //assert_eq!(v.len(), 42742);
        //println!("{:?}",triples.iter().filter(|(s,p,o)| s == "<http://ymatsuo.com/>"));
        //<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> .
        let sample = &v[0..8];
        println!("triples {:#?}", sample);
        let tws = hdt.triples_with(IdKind::Subject,"http://ymatsuo.com/");
        let twsv: Vec<(String, String, String)> = tws.collect();
        println!("{:?}", twsv);
    }
}
