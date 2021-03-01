#![allow(unused)]
mod control_info;
mod dict;
mod header;
pub mod rdf;

use control_info::{ControlInfo, ControlType};
use dict::Dict;
pub use header::Header;
use rdf::Triple;

use std::io;
use std::io::BufRead;

pub struct HDTReader<'a, R: BufRead> {
    reader: &'a mut R,
    base_iri: String,
    global: Option<ControlInfo>,
    header: Option<Header>,
    dictionary: Option<Dict>,
}

impl<'a, R: BufRead> HDTReader<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        HDTReader {
            reader,
            base_iri: String::new(),
            global: None,
            header: None,
            dictionary: None,
        }
    }

    pub fn read_global(&mut self) -> io::Result<ControlInfo> {
        if let Some(global) = &self.global {
            Ok(global.clone())
        } else {
            let info = ControlInfo::read(&mut self.reader)?;
            self.global = Some(info.clone());
            Ok(info)
        }
    }

    pub fn read_header(&mut self) -> io::Result<Header> {
        // Ensure the global control information was read.
        if let None = self.global {
            self.read_global();
        }

        if let Some(header) = &self.header {
            Ok(header.clone())
        } else {
            let header = Header::read(&mut self.reader)?;
            self.header = Some(header.clone());
            Ok(header)
        }
    }

    pub fn read_dictionary(&mut self) -> io::Result<Dict> {
        // Ensure the global control information was read.
        if let None = self.global {
            self.read_global();
        }

        // Ensure the dictionary control information was read.
        if let None = self.header {
            self.read_header();
        }

        if let Some(dictionary) = &self.dictionary {
            Ok(dictionary.clone())
        } else {
            let dictionary = Dict::read(&mut self.reader)?;
            self.dictionary = Some(dictionary.clone());
            Ok(dictionary)
        }
    }

    // TODO
    pub fn triples() -> impl Iterator<Item = Triple> {
        let v: Vec<Triple> = Vec::new();
        v.into_iter()
    }

    // TODO
    pub fn read_triple() -> io::Result<Triple> {
        unimplemented!();
    }
}
