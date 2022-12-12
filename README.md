# HDT

[![Latest Version](https://img.shields.io/crates/v/hdt.svg)](https://crates.io/crates/hdt)
[![Documentation](https://docs.rs/hdt/badge.svg)](https://docs.rs/hdt/)

A Rust library for the [Header Dictionary Triples](https://www.rdfhdt.org/) compressed RDF format, including:

* loading the HDT default format as created by [hdt-cpp](https://github.com/rdfhdt/hdt-cpp)
* efficient querying by triple patterns
* serializing into other formats like RDF Turtle and N-Triples using the [Sophia](https://crates.io/crates/sophia) adapter

However it cannot:

* load other RDF formats
* load other HDT variants 

For this functionality and acknowledgement of all the original authors, please look at the reference implementations in C++ and Java by the [https://github.com/rdfhdt](https://github.com/rdfhdt) organisation.

It also cannot:

* swap data to disk
* modify the RDF graph in memory
* run SPARQL queries

If you need any of the those features, consider using a SPARQL endpoint instead.

## Examples

```toml
[dependencies]
hdt = "0.0.6"
```

```rust
use hdt::Hdt;

pub fn main() {
  // Load an hdt file
  let file = File::open("dbpedia.hdt").expect("error opening file");
  let hdt = Hdt::new(std::io::BufReader::new(file)).expect("error loading HDT");
  // query
  let majors = hdt.triples_with_sp("http://dbpedia.org/resource/Leipzig", "http://dbpedia.org/ontology/major");
  println!("{:?}", majors.collect::<Vec<(String, String, String)>(majors));
}
```

You can also use the Sophia adapter to load HDT files and reduce memory consumption of an existing application based on Sophia:

```rust
use hdt::{Hdt,HdtGraph};

pub fn main() {
  let file = File::open("dbpedia.hdt").expect("error opening file");
  let hdt = Hdt::new(std::io::BufReader::new(file)).expect("error loading HDT");
  let graph = HdtGraph::new(hdt);
  let s = BoxTerm::new_iri_unchecked("http://dbpedia.org/resource/Leipzig");
  let p = BoxTerm::new_iri_unchecked("http://dbpedia.org/ontology/major");
  let majors = graph.triples_with_sp(s,p);
}
```
