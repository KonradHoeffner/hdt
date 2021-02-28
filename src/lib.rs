#![allow(unused)]
mod control_info;
mod dictionary;
mod header;

use control_info::{ControlInfo, ControlType};
use dictionary::Dictionary;
pub use header::Header;

use std::io;
use std::io::BufRead;

pub struct HDTReader<'a, R: BufRead> {
    reader: &'a mut R,
    global: Option<ControlInfo>,
    header: Option<Header>,
    dictionary: Option<Dictionary>,
}

impl<'a, R: BufRead> HDTReader<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        HDTReader {
            reader,
            global: None,
            header: None,
            dictionary: None,
        }
    }

    // TODO: Make generic
    fn read_global(&mut self) -> io::Result<ControlInfo> {
        if let Some(global) = &self.global {
            Ok(global.clone())
        } else {
            let info = ControlInfo::read(&mut self.reader)?;
            self.global = Some(info.clone());
            Ok(info)
        }
    }

    // TODO: Make generic
    pub fn read_header(&mut self) -> io::Result<Header> {
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

    fn read_dictionary(&mut self) -> io::Result<Dictionary> {
        if let None = self.global {
            self.read_global();
        }

        if let None = self.header {
            self.read_header();
        }

        if let Some(dictionary) = &self.dictionary {
            Ok(dictionary.clone())
        } else {
            let dictionary = Dictionary::read(&mut self.reader)?;
            self.dictionary = Some(dictionary.clone());
            Ok(dictionary)
        }
    }

    // TODO: once we've got a type for Triple data
    // pub fn triples() -> Iterator<???> {
    // }

    // TODO: once we've got a type for Triple data
    // pub fn read_triple() -> ??? {
    // }
}
