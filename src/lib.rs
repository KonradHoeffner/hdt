//! [![github]](https://github.com/konradhoeffner/hdt)&ensp;[![crates-io]](https://crates.io/crates/hdt)&ensp;[![docs-rs]](crate)
//!
//! [github]: https://img.shields.io/badge/github-8da0cb?style=for-the-badge&labelColor=555555&logo=github
//! [crates-io]: https://img.shields.io/badge/crates.io-fc8d62?style=for-the-badge&labelColor=555555&logo=rust
//! [docs-rs]: https://img.shields.io/badge/docs.rs-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs
//!
//! <br>
//!
//! HDT is a loading and triple pattern querying library for the [Header Dictionary Triples](https://www.rdfhdt.org/) compressed binary RDF format.
//!
//! Currently this library only supports loading and querying existing HDT files as created by [hdt-cpp](https://github.com/rdfhdt/hdt-cpp).
//! For reference implementations of HDT in C++ and Java, which support conversion and serialization from and into HDT with different format options,
//! and acknowledgement of all the original authors, please look at the <https://github.com/rdfhdt> organisation.
//!
//! # Example of loading and querying an HDT file
//!
//! ```no_run
//! use hdt::Hdt;
//! // Load an hdt file
//! let file = std::fs::File::open("example.hdt").expect("error opening file");
//! let hdt = Hdt::<std::rc::Rc<str>>::new(std::io::BufReader::new(file)).expect("error loading HDT");
//! // query
//! let majors = hdt.triples_with_sp("http://dbpedia.org/resource/Leipzig", "http://dbpedia.org/ontology/major");
//! println!("{:?}", majors.collect::<Vec<_>>());
//! ```
//!
//! Using the Sophia adapter:
//!
//! ```no_run
//! use hdt::{Hdt,HdtGraph};
//! use std::rc::Rc;
//! use sophia::term::BoxTerm;
//! use sophia::graph::Graph;
//! let file = std::fs::File::open("dbpedia.hdt").expect("error opening file");
//! let hdt = Hdt::<Rc<str>>::new(std::io::BufReader::new(file)).expect("error loading HDT");
//! let graph = HdtGraph::new(hdt);
//! let s = BoxTerm::new_iri_unchecked("http://dbpedia.org/resource/Leipzig");
//! let p = BoxTerm::new_iri_unchecked("http://dbpedia.org/ontology/major");
//! let majors = graph.triples_with_sp(&s,&p);
//! ```
//!
//! # Optional features
//!
//! The following features are available.
//!
//! - **`sophia`** *(enabled by default)* ??? Implements the Graph trait from the [Sophia](https://crates.io/crates/sophia) RDF toolkit.
//! This allows you to drastically reduce the RAM usage of an existing application based on Sophia that loads a large knowledge base but requires an input file in the HDT format.
#![feature(round_char_boundary)]
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::similar_names)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::if_not_else)]
#![warn(clippy::missing_const_for_fn)]
/// Types for storing and reading data.
pub mod containers;
// Types for representing dictionaries.
mod dict_sect_pfc;
mod four_sect_dict;
/// Types for representing triple sections.
pub mod hdt;
#[cfg(feature = "sophia")]
/// Adapter for the Sophia library.
pub mod hdt_graph;
/// Types for representing the header.
pub mod header;
/// Iterator over all triples with a given object.
pub mod object_iter;
/// Iterator over all triples with a given predicate.
pub mod predicate_iter;
/// Iterator over all triples with a given predicate and object.
pub mod predicate_object_iter;
/// Types for representing triples.
pub mod triples;

pub use crate::hdt::Hdt;
use containers::ControlInfo;
use dict_sect_pfc::DictSectPFC;
use four_sect_dict::FourSectDict;
pub use four_sect_dict::IdKind;
#[cfg(feature = "sophia")]
#[cfg_attr(doc_cfg, doc(cfg(feature = "parsing")))]
pub use hdt_graph::HdtGraph;

#[cfg(test)]
mod tests {
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn init() {
        INIT.call_once(|| {
            env_logger::init();
        });
    }
}
