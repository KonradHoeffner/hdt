#![allow(unused)]
// types for storing and reading data
pub mod containers;
// types for representing dictionaries
pub mod dict;
// types for representing the header
pub mod header;
// types for representing triple sections
pub mod triples;

use containers::rdf::Triple;
use containers::ControlInfo;
use dict::Dict;
use header::Header;
use triples::TripleSect;

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
    pub fn read_all_triples(&mut self) -> io::Result<BTreeSet<Triple>> {
        use io::Error;
        use io::ErrorKind::Other;

        self.read_meta()?;

        let mut triple_sect = TripleSect::read(&mut self.reader)?;
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

    // (this is going to be an iterator variant that reads on-demand)
    // pub fn triples() -> impl Iterator<Item = Triple> {
    //     let v: Vec<Triple> = Vec::new();
    //     v.into_iter()
    // }
}
