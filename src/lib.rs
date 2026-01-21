#![cfg_attr(all(doc, feature = "cache"), doc = include_str!("../README.md"))]
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
//! Currently this library only supports loading and querying existing HDT files as created by this library or [hdt-cpp](https://github.com/rdfhdt/hdt-cpp).
//! For reference implementations of HDT in C++ and Java, which support conversion and serialization from and into HDT with different format options,
//! and acknowledgement of all the original authors, please look at the <https://github.com/rdfhdt> organisation.
//!
//! # Example of loading and querying an HDT file
//!
//! ```no_run
//! use hdt::Hdt;
//! // Load an hdt file
//! let file = std::fs::File::open("example.hdt").expect("error opening file");
//! let hdt = Hdt::read(std::io::BufReader::new(file)).expect("error loading HDT");
//! // query
//! let majors = hdt.triples_with_pattern(Some("http://dbpedia.org/resource/Leipzig"), Some("http://dbpedia.org/ontology/major"),None);
//! println!("{:?}", majors.collect::<Vec<_>>());
//! ```
//!
#![cfg_attr(
    feature = "cache",
    doc = r#"
# Experimental Features
The **cache** feature is experimental and may change or be removed in future releases.
 
Creating and/or loading a HDT file leveraging a custom cache:

```no_run
let hdt = hdt::Hdt::read_from_path(std::path::Path::new("tests/resources/snikmeta.hdt")).unwrap();
``` 
"#
)]
#![cfg_attr(
    feature = "sophia",
    doc = r#"
# Additional Optional Features

Using the **sophia** Graph trait implementation for Hdt: 

```
use hdt::Hdt;
use hdt::sophia::api::graph::Graph;
use hdt::sophia::api::term::{IriRef, SimpleTerm, matcher::Any};

fn query(hdt: Hdt)
{
  let s = SimpleTerm::Iri(IriRef::new_unchecked("http://dbpedia.org/resource/Leipzig".into()));
  let p = SimpleTerm::Iri(IriRef::new_unchecked("http://dbpedia.org/ontology/major".into()));
  let majors = hdt.triples_matching(Some(s),Some(p),Any);
}
```
"#
)]
// # Optional features
//
// The following features are available.
//
// - **`sophia`** *(enabled by default)* — Implements the Graph trait from the [Sophia](https://crates.io/crates/sophia) RDF toolkit.
// This allows you to drastically reduce the RAM usage of an existing application based on Sophia that loads a large knowledge base but requires an input file in the HDT format.
//#![warn(missing_docs)] //TODO: comment again after refactoring
#![warn(clippy::pedantic)]
#![warn(clippy::cargo)]
#![warn(clippy::str_to_string)]
#![warn(clippy::print_stdout)]
#![warn(clippy::print_stderr)]
#![warn(clippy::missing_const_for_fn)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::enum_glob_use)]
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
#![allow(clippy::into_iter_without_iter)]
#![allow(clippy::len_without_is_empty)]
// multiple versions of syn crate in transitive dependencies
#![allow(clippy::multiple_crate_versions)]
/// Types for storing and reading data.
pub mod containers;
/// Types for representing dictionaries.
pub mod dict_sect_pfc;
/// Types for representing a four section dictionary
pub mod four_sect_dict;
/// Types for representing triple sections.
pub mod hdt;
#[cfg(feature = "sophia")]
pub use sophia;
#[cfg(feature = "sophia")]
/// Adapter for the Sophia library.
pub mod hdt_graph;
/// Types for representing the header.
pub mod header;
#[cfg(feature = "sparql")]
/// SPARQL queries.
pub mod sparql;
/// Types for representing and querying triples.
pub mod triples;
/// Constants for triple terms
pub mod vocab;
#[cfg(target_arch = "wasm32")]
pub mod wasm;

pub use crate::hdt::Hdt;
use containers::ControlInfo;
use dict_sect_pfc::DictSectPFC;
use four_sect_dict::FourSectDict;
pub use four_sect_dict::IdKind;

#[cfg(test)]
mod tests {
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn init() {
        INIT.call_once(|| {
            color_eyre::install().unwrap();
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
                .is_test(true)
                .init();
        });
    }
}
