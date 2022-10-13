use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::hdt_reader::HdtReader;
use crate::header::Header;
use std::io;

use crate::dict::IdKind;
use crate::triples::TripleId;
use crate::triples::TripleSect;
use std::fs::File;

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

    pub fn triples(&self) -> impl Iterator<Item = (String, String, String)> + '_ {
        // todo: implement and use into_iter with references for bitmap
        // current implementation is inefficient due to cloning
        self.triple_sect
            .clone()
            .read_all_ids()
            .into_iter()
            .map(move |id: TripleId| {
                let subject = self.dict.id_to_string(id.subject_id, IdKind::Subject);
                let predicate = self.dict.id_to_string(id.predicate_id, IdKind::Predicate);
                let object = self.dict.id_to_string(id.object_id, IdKind::Object);
                (subject, predicate, object)
            })
    }
}

#[cfg(test)]
mod tests {}
