---
title: 'HDT: A Rust library for the Header Data Triples binary RDF compression format'
tags:
  - Rust
  - HDT
  - RDF
  - linked data
  - semantic web
authors:
  - name: Konrad Höffner
    orcid: 0000-0001-7358-3217
    equal-contrib: true
    corresponding: true
    affiliation: 1
  - name: Tim Baccaert
    equal-contrib: true
    affiliation: 2
affiliations:
 - name: Institute for Medical Informatics, Statistics and Epidemiology, Medical Faculty, Leipzig University
   index: 1
 - name: Independent Researcher, Belgium
   index: 2
date: 1 January 2022
bibliography: paper.bib
---

# Summary

We present the Rust library hdt-rs (named "hdt" in the context of Rust libraries, such as [on crates.io](https://crates.io/crates/hdt)) for the Header Data Triples (HDT) binary RDF compression format described by @hdt2012 and @hdt2013.
This allows writing high-performance Rust applications that load and query HDT files using triple patterns.
Existing Rust applications using the Sophia [@sophia] library can easily and drastically reduce their RAM usage by using the provided Sophia HDT adapter.

# Statement of need

Semantic Web technologies have seen adoption by major tech companies in recent years
but widespread use is still inhibited by a lack of freely available performant, accessible, robust and adaptable tooling [@semanticwebreview].
SPARQL endpoints provide a standard publication channel and API to any RDF graph but they are not suitable for all use cases.
On small knowledge bases, there is a large relative overhead in both memory and CPU resources.
On large knowledge bases on the other hand, query complexity and shared access may cause an overload of the server, causing delayed or missed responses.
Longterm availability of SPARQL endpoints is often compromised [@readyforaction], which impacts all applications depending on them.

To insulate against such problems, Semantic Web applications may integrate and query an RDF graph using libraries such as Apache Jena [@jena] for Java,
RDFlib [@rdflib] for Python, librdf [@librdf] for C or Sophia [@sophia] for Rust.
However those libraries do not scale to large RDF graphs due to their excessive memory usage, see \autoref{fig:benchmark},
which can be drastically lowered by using the Header Data Triples (HDT) binary RDF format, which can be loaded into memory in compressed form while still allowing efficient queries.
Implementations existed for C++ [@hdtcpp] and Java [@hdtjava] but not for Rust, a popular modern, statically typed high-level programming language that allows writing performant software while still ensuring memory safety,
which aligns with the challenges to the adoption of the Semantic Web.
The Rust HDT library is used through the included Sophia adapter by the RickView [@rickview] RDF browser to publish large knowledge bases, for example LinkedSpending [@linkedspending] at <https://linkedspending.aksw.org>,
which previously suffered from frequent downtimes when based on a SPARQL endpoint.

![Dataset loading time, memory usage (resident set size) and ?PO triple pattern query time of different RDF libraries on an Intel i9-12900k CPU based on the benchmark suite of @sophia.
librdf was not benchmarked on $10^6$ triples and beyond due to graph loading exceeding several hours.
hdt-java produces `DelayedString` instances, which are converted to strings to account for the time that would otherwise be spent later.
The index files that hdt-java and hdt-cpp produce are deleted before each run.
Versions: Apache Jena 4.6.1, n3.js 1.6.3, librdf 1.0.17, RDFlib 6.2.0, sophia 0.8.0-alpha, hdt-rs 0.0.13-alpha, hdt-java 3.0.9, hdt-cpp master fbcb31a, OpenJDK 19, Node.js 16.18.0, clang 14.0.6, Python 3.10.8, rustc 1.69.0-nightly (target-cpu=native), GCC 12.2.1.
\label{fig:benchmark}](img/benchmark.png){ width=100% }

# Limitations

HDT is a read-only file format.
For querying *and* modification of large RDF graphs, SPARQL queries on a separate endpoint are better suited.
*hdt-rs* does not supply additional command line tools, for example for converting different RDF serializations to and from HDT.
For this purpose, the command line tools of hdt-cpp [@hdtcpp] and hdt-java [@hdtjava] can be used.
Extensions such as HDT++ [@serializingrdf] or iHDT++ [@ihdt] are currently not supported.

# Acknowledgements

We thank Pierre-Antoine Champin for explaining the details of Sophia and for creating [the benchmark suite](https://github.com/pchampin/sophia_benchmark) 
that the [HDT benchmarks](https://github.com/KonradHoeffner/hdt_benchmark) are based on and for the thorough code review.

# Preliminaries

## RDF

The *Resource Description Framework* (RDF) is a data model that represents information using *triples*, each consisting of a *subject*, *predicate* and *object*.
A set of triples is called an *RDF graph*, where the subjects and objects can be visualized as nodes and the predicates as labeled, directed edges.
Predicates are always IRIs, however subjects and objects can also be *blank nodes* and objects can also be *literals*.
There are multiple text-based RDF serialization formats with different compromises between verbosity, ease of automatic processing and human readability.
For example, the N-Triples serialization of the fact "the mayor of Leipzig is Burkhard Jung" from DBpedia is:

```ntriples
<http://dbpedia.org/resource/Leipzig> <http://dbpedia.org/ontology/mayor>
    (linebreak) <http://dbpedia.org/resource/Burkhard_Jung> .
```

## Querying RDF
The default query language 

## Triple Patterns

In a *triple pattern*, each part of the triple is either a constant or a variable, resulting in eight different types of triple patterns. 
We denote the triple pattern with all constants as SPO (matching one or zero triples) and the pattern with all constants with ??? (matching all triples in the graph).
The other triple patterns are denoted analogously.

## Header Data Triples

![The Bitmap Triples structure represents the adjacency matrix of the RDF graph as a tree.
Image source and further information in @hdt2012.
\label{fig:bt}](img/bt.png){ width=100% }

## Load an HDT file

```rust
use hdt::Hdt;
use std::fs::File;
use std::io::BufReader;

let f = File::open("example.hdt").expect("error opening file");
let hdt = Hdt::new(std::io::BufReader::new(f)).expect("error loading HDT");
```
## Query SP? pattern

For example, we can select majors of Leipzig from DBpedia using an SP? triple pattern where the subject (Leipzig) and predicate (major) are constant and the object is a variable.
Continuing the previous example of loading a file, we can get an iterator over all triples that match the pattern:

```rust
let majors = hdt.triples_with_pattern(
    Some("http://dbpedia.org/resource/Leipzig"),
    Some("http://dbpedia.org/ontology/major"),
    None);
println!("{:?}", majors.collect::<Vec<_>>());
```

All patterns with constant subject (SPO, SP?, SO? and S??) are answered using the Bitmap Triples structure.

## Query PO? pattern

![The HDT *Focused on Querying* (HDT-FoQ) extension allows efficient queries with ?PO, ?P? and ??O patterns.
Image source and further information in @hdt2012.
\label{fig:foq}](img/hdt-foq.png){ width=50% }

# References
