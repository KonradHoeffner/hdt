use crate::containers::rdf::Triple;
use crate::containers::ControlInfo;
use crate::dict::Dict;
use crate::header::Header;
use crate::triples::TripleSect;
use std::collections::BTreeSet;
use std::io;
use std::io::BufRead;

pub struct HDTReader<'a, R: BufRead> {
    reader: &'a mut R,
    global_ci: Option<ControlInfo>,
    header: Option<Header>,
    dict: Option<Dict>,
}

impl<'a, R: BufRead> HDTReader<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        HDTReader {
            reader,
            global_ci: None,
            header: None,
            dict: None,
        }
    }

    fn has_read_meta(&self) -> bool {
        self.global_ci.is_some() && self.header.is_some() && self.dict.is_some()
    }

    fn read_meta(&mut self) -> io::Result<()> {
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
        let triple_ids = triple_sect.read_all_ids();

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
    // pub fn triples() -> impl Iterator<Item = Triple> {
    //     let v: Vec<Triple> = Vec::new();
    //     v.into_iter()
    // }
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
        let mut reader = BufReader::new(file);
        let mut hdt_reader = HDTReader::new(&mut reader);
        let triples = hdt_reader.read_all_triples().unwrap();
        assert_eq!(triples.len(), 242256);

        let ten: Vec<(String, String, String)> = triples.into_iter().take(50).collect();
        //panic!("{:#?}", ten);
    }
}
