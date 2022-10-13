use crate::containers::rdf::Triple;
use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::header::Header;
use crate::triples::TripleSect;
use std::collections::BTreeSet;
use std::io;
use std::io::BufReader;
use std::fs::File;

pub struct HdtReader {
    pub reader: BufReader<File>,
    pub global_ci: Option<ControlInfo>,
    pub header: Option<Header>,
    pub dict: Option<Dict>,
}

impl HdtReader {
    pub fn new(file: File) -> Self {
        HdtReader {
            reader: BufReader::new(file),
            global_ci: None,
            header: None,
            dict: None,
        }
    }

    fn has_read_meta(&self) -> bool {
        self.global_ci.is_some() && self.header.is_some() && self.dict.is_some()
    }

    pub fn read_meta(&mut self) -> io::Result<()> {
        if !self.has_read_meta() {
            self.global_ci = Some(ControlInfo::read(&mut self.reader)?);
            self.header = Some(Header::read(&mut self.reader)?);
            self.dict = Some(Dict::read(&mut self.reader)?);
        }

        Ok(())
    }

    /// Blocking operation that reads the entire file.
    pub fn read_all_triples(&mut self) -> io::Result<Vec<(String, String, String)>> {
        use io::Error;
        use io::ErrorKind::Other;

        //println!("read meta");
        self.read_meta()?;
        //println!("read triples");
        let mut triple_sect = TripleSect::read(&mut self.reader)?;
        //println!("read ids");
        let triple_ids = triple_sect.read_all_ids().into_iter().collect();

        if let Some(dict) = &mut self.dict {
            Ok(dict.translate_all_ids(triple_ids))
        } else {
            Err(Error::new(
                Other,
                "Something unexpected went wrong when reading the dictionary.",
            ))
        }
    }

    // TODO: (this is going to be an iterator variant that reads on-demand)
    pub fn triples(&mut self) -> impl Iterator<Item = (String, String, String)> {
        self.read_all_triples().unwrap().into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_full_triples() {
        let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
        // let file = File::open("data/wordnet.hdt").expect("error opening file");
        //let file = File::open("tests/resources/qbench2.hdt").expect("error opening file");
        //let file = File::open("tests/resources/lscomplete20143.hdt").expect("error opening file");
        let mut hdt_reader = HdtReader::new(file);
        let triples = hdt_reader.read_all_triples().unwrap();
        assert_eq!(triples.len(), 242256);
        //println!("{:?}",triples.iter().filter(|(s,p,o)| s == "<http://ymatsuo.com/>"));
        //<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> .
        let end: Vec<(String, String, String)> = triples.into_iter().rev().take(10).collect();
        //let end: Vec<(String, String, String)> = triples.into_iter().collect();
        println!("triples {:#?}", end);
    }
}
