#![allow(unused)]
// types for storing and reading data
pub mod containers;
// types for representing dictionaries
pub mod dict;
// types for representing the header
pub mod hdt_reader;
pub mod header;
// types for representing triple sections
pub mod triples;
use containers::ControlInfo;
use header::Header;
use dict::Dict;
