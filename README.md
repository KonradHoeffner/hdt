# HDT

[![Latest Version](https://img.shields.io/crates/v/hdt.svg)](https://crates.io/crates/hdt)
[![Documentation](https://docs.rs/hdt/badge.svg)](https://docs.rs/hdt/)
[![Benchmarks](https://img.shields.io/badge/Benchmarks--x.svg?style=social)](https://github.com/KonradHoeffner/hdt_benchmark/blob/master/benchmark_results.ipynb)

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

## Setup
Add the following to Cargo.toml:

```toml
[dependencies]
hdt = "0.0.12"
```

Since version 0.0.7, nightly is required:

    rustup component add rustfmt --toolchain nightly

## Examples

```rust
use hdt::Hdt;

let file = std::fs::File::open("example.hdt").expect("error opening file");
let hdt = Hdt::Rc<str>::new(std::io::BufReader::new(file)).expect("error loading HDT");
// query
let majors = hdt.triples_with_sp("http://dbpedia.org/resource/Leipzig", "http://dbpedia.org/ontology/major");
println!("{:?}", majors.collect::<Vec<_>>());
```

You can also use the Sophia adapter to load HDT files and reduce memory consumption of an existing application based on Sophia:

```rust
use hdt::{Hdt,HdtGraph};
use sophia::term::BoxTerm;
use sophia::graph::Graph;

let file = std::fs::File::open("dbpedia.hdt").expect("error opening file");
let hdt = Hdt::<std::rc::Rc<str>>::new(std::io::BufReader::new(file)).expect("error loading HDT");
let graph = HdtGraph::new(hdt);
let s = BoxTerm::new_iri_unchecked("http://dbpedia.org/resource/Leipzig");
let p = BoxTerm::new_iri_unchecked("http://dbpedia.org/ontology/major");
let majors = graph.triples_with_sp(&s,&p);
```

If you don't want to pull in the Sophia dependency, you can exclude the adapter:

```toml
[dependencies]
hdt = { version = "0.0.12", default-features = false }
```

Version 0.0.13 with a slightly different syntax is currently in development on the main branch but not yet released on [crates.io](https://crates.io/crates/hdt).

## Performance and Benchmarks
[The benchmarks](https://github.com/KonradHoeffner/hdt_benchmark/blob/master/benchmark_results.ipynb) show the performance of this and some other RDF libraries.
The performance of a query depends on the size of the graph, the type of triple pattern and the size of the result set.
When using large HDT files, make sure to enable the release profile, such as through `cargo build --release`, as this can be much faster than using the dev profile.

### Profiling
If you want to optimize the code, you can use a profiler.
The provided test data is very small in order to keep the size of the crate down; locally modifying the tests to use a large HDT file returns more meaningful results.

#### Example with perf and Firefox Profiler

    $ cargo test --release
    [...]
    Running unittests src/lib.rs (target/release/deps/hdt-2b2f139dafe69681)
    [...]
    $ perf record --call-graph=dwarf target/release/deps/hdt-2b2f139dafe69681 hdt::tests::triples
    $ perf script > /tmp/test.perf

Then go to <https://profiler.firefox.com/> and open `/tmp/test.perf`.

## Community Guidelines

### Issues and Support
If you have a problem with the software, want to report a bug or have a feature request, please use the [issue tracker](https://github.com/KonradHoeffner/hdt/issues).
If have a different type of request, feel free to send an email to [Konrad](mailto:konrad.hoeffner@uni-leipzig.de).

### Contribute
We are happy to receive pull requests.
Please use `cargo fmt` before committing, make sure that `cargo test` succeeds and that the code compiles on the nightly toolchain both with and without the "sophia" feature active.
`cargo clippy` should not report any warnings.
