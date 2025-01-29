# HDT

[![Latest Version](https://img.shields.io/crates/v/hdt.svg)](https://crates.io/crates/hdt)
[![Lint and Test](https://github.com/konradhoeffner/hdt/actions/workflows/lint_and_test.yml/badge.svg)](https://github.com/konradhoeffner/hdt/actions/workflows/lint_and_test.yml)
[![Documentation](https://docs.rs/hdt/badge.svg)](https://docs.rs/hdt/)
[![Benchmarks](https://img.shields.io/badge/Benchmarks--x.svg?style=social)](https://github.com/KonradHoeffner/hdt_benchmark/blob/master/benchmark_results.ipynb)
[![HDT Rust @ LD Party Video](https://img.shields.io/badge/video-8A2BE2)](https://www.youtube.com/watch?v=R-S0o_UwPMk)
[![DOI](https://joss.theoj.org/papers/10.21105/joss.05114/status.svg)](https://doi.org/10.21105/joss.05114)

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

```rust
use hdt::Hdt;

let file = std::fs::File::open("example.hdt").expect("error opening file");
let hdt = Hdt::new(std::io::BufReader::new(file)).expect("error loading HDT");
// query
let majors = hdt.triples_with_pattern(Some("http://dbpedia.org/resource/Leipzig"), Some("http://dbpedia.org/ontology/major"),None);
println!("{:?}", majors.collect::<Vec<_>>());
```

You can also use the Sophia adapter to load HDT files and reduce memory consumption of an existing application based on Sophia, which is re-exported as `hdt::sophia`:

```rust
use hdt::{Hdt,HdtGraph};
use hdt::sophia::api::graph::Graph;
use hdt::sophia::api::term::{IriRef, SimpleTerm, matcher::Any};

let file = std::fs::File::open("dbpedia.hdt").expect("error opening file");
let hdt = Hdt::new(std::io::BufReader::new(file)).expect("error loading HDT");
let graph = HdtGraph::new(hdt);
let s = SimpleTerm::Iri(IriRef::new_unchecked("http://dbpedia.org/resource/Leipzig".into()));
let p = SimpleTerm::Iri(IriRef::new_unchecked("http://dbpedia.org/ontology/major".into()));
let majors = graph.triples_matching(Some(s),Some(p),Any);
```

If you don't want to pull in the Sophia dependency, you can exclude the adapter:

```toml
[dependencies]
hdt = { version = "...", default-features = false }
```

There is also a runnable example [in the examples folder](https://github.com/KonradHoeffner/hdt/tree/main/examples), which you can run with `cargo run --example query`.

Users can also choose to use the experimental `cache` feature. If enabled, the library will utilize a custom cached TriplesBitmap file if it exists or create one if it does not exist.

```rust
let hdt = hdt::Hdt::new_from_path(std::path::Path::new("tests/resources/snikmeta.hdt")).unwrap();
```

The `cache` feature is experimental and may change or be removed in future releases.

## API Documentation

See [docs.rs/latest/hdt](https://docs.rs/hdt) or generate for yourself with `cargo doc --no-deps` without disabling default features.

## Performance
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

## Criterion benchmark

    cargo bench --bench criterion

* requires [persondata\_en.hdt](https://github.com/KonradHoeffner/hdt/releases/download/benchmarkdata/persondata_en.hdt.bz2) placed in `tests/resources`

## iai benchmark

    cargo bench --bench iai

* requires [persondata\_en\_10k.hdt](https://github.com/KonradHoeffner/hdt/releases/download/benchmarkdata/persondata_en_10k.hdt.bz2) placed in `tests/resources`
* requires [Valgrind](https://valgrind.org/) to be installed

## Comparative benchmark suite

[The separate benchmark suite](https://github.com/KonradHoeffner/hdt_benchmark/blob/master/benchmark_results.ipynb) compares the performance of this and some other RDF libraries.

## Community Guidelines

### Issues and Support
If you have a problem with the software, want to report a bug or have a feature request, please use the [issue tracker](https://github.com/KonradHoeffner/hdt/issues).
If have a different type of request, feel free to send an email to [Konrad](mailto:konrad.hoeffner@uni-leipzig.de).

### Citation

[![DOI](https://joss.theoj.org/papers/10.21105/joss.05114/status.svg)](https://doi.org/10.21105/joss.05114)

If you use this library in your research, please cite our paper in the Journal of Open Source Software.
We also provide a [CITATION.cff](./CITATION.cff) file.

#### BibTeX entry

```bibtex
@article{hdtrs,
  doi = {10.21105/joss.05114},
  year = {2023},
  publisher = {The Open Journal},
  volume = {8},
  number = {84},
  pages = {5114},
  author = {Konrad Höffner and Tim Baccaert},
  title = {hdt-rs: {A} {R}ust library for the {H}eader {D}ictionary {T}riples binary {RDF} compression format},
  journal = {Journal of Open Source Software}
}
```

#### Citation string

Höffner et al., (2023). hdt-rs: A Rust library for the Header Dictionary Triples binary RDF compression format. Journal of Open Source Software, 8(84), 5114, https://doi.org/10.21105/joss.05114

### Contribute
We are happy to receive pull requests.
Please use `cargo fmt` before committing, make sure that `cargo test` succeeds and that the code compiles on the stable and nightly toolchain both with and without the "sophia" feature active.
`cargo clippy` should not report any warnings.
