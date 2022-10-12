//use crate::containers::rdf::Triple;
use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::header::Header;
use crate::hdt_reader::HDTReader;
use std::io;
use std::io::BufReader;
use std::fs::File;
use crate::triples::TripleSect;
use crate::triples::TripleId;
use crate::dict::IdKind;
use crate::triples::BitmapIter;
/*
use io::Error;
use std::collections::BTreeSet;
        use io::ErrorKind::Other;
*/

pub struct Hdt {
    global_ci: ControlInfo,
    header: Header,
    dict: Dict,
    triple_sect : TripleSect,
}

impl Hdt {
    fn load(filename: &str) -> io::Result<Self> {
        let file = File::open(filename).expect("error opening file");
        let mut br = BufReader::new(file);
        let mut hdtr = HDTReader::new(&mut br);
        hdtr.read_meta()?;
        let triple_sect = TripleSect::read(&mut hdtr.reader)?;
        Ok (Hdt {global_ci: hdtr.global_ci.unwrap(), header: hdtr.header.unwrap(), dict: hdtr.dict.unwrap(), triple_sect})
    }


    pub fn triples(&mut self) -> impl Iterator<Item = (String, String, String)> {
        // todo: implement and use into_iter with references for bitmap
        self.triple_sect.clone().read_all_ids().into_iter()
        .map(|_| ("".to_owned(),"".to_owned(),"".to_owned()))
/*            it.map(|id: &TripleId| {
                let subject = self.dict.id_to_string(id.subject_id, IdKind::Subject);
                let predicate = self.dict.id_to_string(id.predicate_id, IdKind::Predicate);
                let object = self.dict.id_to_string(id.object_id, IdKind::Object);
                (subject, predicate, object)
        })*/
    }
}

#[cfg(test)]
mod tests {
}
