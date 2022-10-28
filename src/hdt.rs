use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::dict::IdKind;
use crate::hdt_reader::HdtReader;
use crate::header::Header;
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
    pub fn new(file: File) -> io::Result<Self> {
        let mut hdtr = HdtReader::new(file);
        hdtr.read_meta()?;
        let triple_sect = TripleSect::read(&mut hdtr.reader)?;
        Ok(Hdt {
            global_ci: hdtr.global_ci.unwrap(),
            header: hdtr.header.unwrap(),
            dict: hdtr.dict.unwrap(),
            triple_sect,
        })
    }

    // TODO: refactor out common code of triples methods
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
        // todo: implement and use into_iter with references for bitmap
        // current implementation is inefficient due to cloning
        self.translate_ids(self.triple_sect.clone().read_all_ids().into_iter())
    }

    pub fn triples_with_s(&self, s: &str) -> impl Iterator<Item = (String, String, String)> + '_ {
        // TODO: optimize, for example with binary search
        let subject_id = self.dict.string_to_id(s, IdKind::Subject);
        println!(
            "string_to_id({},IdKind::Subject) == {}, reverse test {}",
            s,
            subject_id,
            self.dict.id_to_string(subject_id, IdKind::Subject)
        );
        self.translate_ids(
            self.triple_sect
                .clone()
                .read_all_ids()
                .into_iter()
                .filter(move |id: &TripleId| id.subject_id == subject_id),
        )
    }
    pub fn triples_with_p(&self, p: &str) -> impl Iterator<Item = (String, String, String)> + '_ {
        // TODO: optimize
        let predicate_id = self.dict.string_to_id(p, IdKind::Predicate);
        let test = self.dict.id_to_string(predicate_id, IdKind::Predicate);
        if test != p {
            eprintln!("string_to_id({},IdKind::Predicate) == {}, reverse test {}", p, predicate_id, test);
        }
        self.translate_ids(
            self.triple_sect
                .clone()
                .read_all_ids()
                .into_iter()
                .filter(move |id: &TripleId| id.predicate_id == predicate_id),
        )
    }

    pub fn triples_with_o(&self, o: &str) -> impl Iterator<Item = (String, String, String)> + '_ {
        // TODO: optimize
        let object_id = self.dict.string_to_id(o, IdKind::Object);
        println!(
            "string_to_id({},IdKind::Subject) == {}, reverse test {}",
            o,
            object_id,
            self.dict.id_to_string(object_id, IdKind::Object)
        );
        self.translate_ids(
            self.triple_sect
                .clone()
                .read_all_ids()
                .into_iter()
                .filter(move |id: &TripleId| id.object_id == object_id),
        )
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
        let hdt = Hdt::new(file).unwrap();
        let mut triples = hdt.triples();
        let v: Vec<(String, String, String)> = triples.collect();
        assert_eq!(v.len(), 242256);
        //assert_eq!(v.len(), 42742);
        //println!("{:?}",triples.iter().filter(|(s,p,o)| s == "<http://ymatsuo.com/>"));
        //<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> .
        let sample = &v[0..8];
        println!("triples {:#?}", sample);
        let tws = hdt.triples_with_s("http://ymatsuo.com/");
        let twsv: Vec<(String, String, String)> = tws.collect();
        println!("{:?}", twsv);
    }
}
