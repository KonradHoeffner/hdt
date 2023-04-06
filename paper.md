---
title: 'hdt-rs: A Rust library for the Header Dictionary Triples binary RDF compression format'
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

We present the Rust library hdt-rs (named "hdt" in the context of Rust libraries, such as [on crates.io](https://crates.io/crates/hdt)) for the Header Dictionary Triples (HDT) binary RDF compression format.
This allows writing high-performance Rust applications that load and query HDT datasets using triple patterns.
Existing Rust applications using the Sophia library [@sophia] can easily and drastically reduce their RAM usage by using the provided Sophia HDT adapter.

# Preliminaries

## RDF

The *Resource Description Framework* (RDF) is a data model that represents information using *triples*, each consisting of a *subject*, *predicate* and *object*.
A set of triples is called an *RDF graph*, where the subjects and objects can be visualized as nodes and the predicates as labeled, directed edges.
A predicate is always an *IRI* (Internationalized Resource Identifier), which is a generalization of an URI that permits additional characters.
Subjects and objects can also be *blank nodes* and objects can also be *literals*.
There are multiple text-based RDF serialization formats with different compromises between verbosity, ease of automatic processing and human readability.
For example, the N-Triples representation of the fact "the mayor of Leipzig is Burkhard Jung" from DBpedia [@dbpedia] is:

```ntriples
<http://dbpedia.org/resource/Leipzig> <http://dbpedia.org/ontology/mayor>
    (no linebreak) <http://dbpedia.org/resource/Burkhard_Jung> .
```

## Triple Patterns

*Triple patterns* allow matching a subset of a graph.
Each part of the pattern is either a constant or a variable, resulting in eight different types. 
We denote the pattern type with all constants as SPO (matching one or zero triples) and the type with all constants with ??? (matching all triples in the graph).
The other triple patterns are denoted analogously.

## Header Dictionary Triples
While text-based RDF serialization formats can be read by humans, they are too verbose to be practical on large graphs.
The serialized size of a graph can be drastically lowered by using the Header Dictionary Triples binary RDF format, which can be loaded into memory in compressed form while still allowing efficient queries.
The *header* contains metadata as uncompressed RDF that describes the dataset.
The *dictionary* stores all the *RDF terms* (IRIs, literals and blank nodes) in the dataset in compressed form using front-coding [@frontcoding],
and assigns a unique numerical identifier (ID) to each of them.
This allows the *triples* component to store the adjacency matrix of the graph using those IDs in compressed form.

![The Bitmap Triples structure represents the adjacency matrix of the RDF graph as trees.
Image source and further information in @hdt2012.
\label{fig:bt}](img/bt.png){ width=100% }

All patterns with constant subject (SPO, SP?, SO? and S??) as well as ??? are answered using the Bitmap Triples structure, see \autoref{fig:bt}, while the other
patterns are answered using HDT-FoQ, see \autoref{fig:foq}.
As HDT is a very complex format, we recommend referring to @hdt2012 and @hdt2013 for a comprehensive documentation.

![The HDT *Focused on Querying* (HDT-FoQ) extension allows efficient queries with ?PO, ?P? and ??O patterns.
Image source and further information in @hdt2012.
\label{fig:foq}](img/hdt-foq.png){ width=50% }

# Statement of need

Semantic Web technologies have seen adoption by major tech companies in recent years
but widespread use is still inhibited by a lack of freely available performant, accessible, robust and adaptable tooling [@semanticwebreview].
SPARQL endpoints provide a standard publication channel and API to any RDF graph but they are not suitable for all use cases.
On small graphs, there is a large relative overhead in both memory and CPU resources.
On large graphs on the other hand, query complexity and shared access may cause an overload of the server, causing delayed or missed responses.
Longterm availability of SPARQL endpoints is often compromised [@readyforaction], which impacts all applications depending on them.

To insulate against such problems, Semantic Web applications may integrate and query an RDF graph using libraries such as Apache Jena [@jena] for Java,
RDFlib [@rdflib] for Python, librdf [@librdf] for C or Sophia [@sophia] for Rust.
However those libraries do not scale to large RDF graphs due to their excessive memory usage, see \autoref{fig:benchmark}.
To complement hdt-cpp [@hdtcpp] and hdt-java [@hdtjava], we implement HDT in Rust, which is a popular modern, statically typed high-level programming language that allows writing performant software while still ensuring memory safety,
which aligns with the challenges to the adoption of the Semantic Web.
hdt-rs is used through the included Sophia adapter by the RickView [@rickview] RDF browser to publish large graphs, for example LinkedSpending [@linkedspending] at <https://linkedspending.aksw.org>,
which previously suffered from frequent downtimes when based on a SPARQL endpoint.

# Benchmark

![Dataset loading time, memory usage (resident set size) and ?PO triple pattern query time of different RDF libraries on an Intel i9-12900k CPU based on the benchmark suite of @sophia.
librdf was not benchmarked on $10^6$ triples and beyond due to graph loading exceeding several hours.
hdt-java produces `DelayedString` instances, which are converted to strings to account for the time that would otherwise be spent later.
The index files that hdt-java and hdt-cpp produce are deleted before each run.
Versions: Apache Jena 4.6.1, n3.js 1.6.3, librdf 1.0.17, RDFlib 6.2.0, sophia 0.8.0-alpha, hdt-rs 0.0.13-alpha, hdt-java 3.0.9, hdt-cpp master fbcb31a, OpenJDK 19, Node.js 16.18.0, clang 14.0.6, Python 3.10.8, rustc 1.69.0-nightly (target-cpu=native), GCC 12.2.1.
\label{fig:benchmark}](img/benchmark.png){ width=100% }

| Library         | Memory in MB | Load Time in ms | Query Time in ms |
|:----------------|-------------:|----------------:|-----------------:|
| hdt_cpp         |      **112** |            1985 |              362 |
| sophia_hdt      |          263 |             930 |              355 |
| hdt_rs          |          264 |         **912** |              315 |
| hdt_java (DelayedString) | 738 |            3170 |          **214** |
| hdt_java (String)        | 785 |            3476 |          **321** |
| sophia_lg       |      **834** |       **11656** |               85 |
| sophia          |         1371 |           15990 |           **20** |
| jena (java)     |         5352 |           40400 |              159 |
| n3js (js)       |        12404 |          100820 |              654 |
| rdflib (python) |        14481 |          182002 |              940 |
| librdf (c)      |           -- |              -- |               -- |

: Rounded averages over four runs on the complete persondata dataset containing 10310105 triples (rightmost points in \autoref{fig:benchmark}) serialized as a 90 MB HDT and 1.2 GB RDF Turtle file.
Sorted by memory usage for of the graph. For better comparision, results for hdt_java are given both with and without calling `DelayedString::toString` on the results.
Measured values are subject to considerable fluctuations, see vertical bars in \autoref{fig:benchmark}.\label{tab:benchmark}

\autoref{tab:benchmark} demonstrates the advantage of HDT libraries in memory usage with hdt_cpp only using 112 MB compared to 834 MB for the most memory-efficient tested non-HDT RDF library of sophia_lg (LightGraph).
When comparing only Rust libraries, sophia_lg still needs more than three times the amount of memory that hdt_rs does.
Memory consumption is calculated by comparing resident set size before and after graph loading and index generation, in between which memory usage may be higher.
Converting other formats to HDT in the first place is also a time and memory intensive process.
The uncompressed and fully indexed Sophia FastGraph (sophia) strongly outperforms the HDT libraries in ?PO query time, with 20ms compared to 214ms respectively 321ms for hdt_java.
While being the fastest querying HDT library in this test, hdt_java has a large memory usage for an HDT library placing it near the much faster sophia_lg.
The large overhead on small graph sizes for hdt_java in \autoref{fig:benchmark} suggests that these considerations might turn out differently with larger graph sizes.
In fact, HDT allows loading much larger datasets, however at that point several of the tested libraries could not have been included, such as rdflib, which already uses more than 14 GB of memory to load the ~10 million triples.
hdt_rs achieves the lowest graph loading time with 912ms compared to more than 11s for the fastest loading non-HDT library sophia_lg.
hdt_cpp and hdt_java can speed up loading by reusing previously saved indexes but these were deleted between runs to achieve consistent measurements.


# Examples

## Add the dependency to a Rust application

```bash
$ cargo add hdt
```

## Load an HDT file

```rust
use hdt::Hdt;
use std::{fs::File,io::BufReader};
let f = File::open("example.hdt").expect("error opening file");
let hdt = Hdt::new(BufReader::new(f)).expect("error loading HDT");
```

## Query SP? pattern

Find the mayor of Leipzig from DBpedia using an SP? triple pattern:

```rust
hdt.triples_with_pattern(
    Some("http://dbpedia.org/resource/Leipzig"),
    Some("http://dbpedia.org/ontology/mayor"),
    None).next();
```

## Query ?PO pattern

Which city has Burkhard Jung as the mayor?

```rust
hdt.triples_with_pattern(
    None,
    Some("http://dbpedia.org/ontology/mayor"),
    Some("http://dbpedia.org/resource/Burkhard_Jung")).next();
```

## Use HDT with the Sophia library

```rust
use hdt::{Hdt,HdtGraph};
use hdt::sophia::api::graph::Graph;
use hdt::sophia::api::term::{IriRef, SimpleTerm, matcher::Any};
use std::{fs::File,io::BufReader};

let file = File::open("dbpedia.hdt").expect("error opening file");
let hdt = Hdt::new(BufReader::new(file)).expect("error loading HDT");
let graph = HdtGraph::new(hdt);
// now Sophia can be used as usual
let s = SimpleTerm::Iri(
    IriRef::new_unchecked("http://dbpedia.org/resource/Leipzig".into()));
let p = SimpleTerm::Iri(
    IriRef::new_unchecked("http://dbpedia.org/ontology/mayor".into()));
let mayors = graph.triples_matching(Some(s),Some(p),Any);
```

# Limitations

HDT is read-only.
For querying and modification of large graphs, a separate SPARQL endpoint is better suited.
We do not supply command line tools for converting other formats to and from HDT.
Instead, the tools of hdt-cpp and hdt-java can be used.
Extensions such as HDT++ [@serializingrdf] or iHDT++ [@ihdt] are unsupported.

# Acknowledgements

We thank Pierre-Antoine Champin for explaining the details of Sophia and for creating [the benchmark suite](https://github.com/pchampin/sophia_benchmark) 
that the [HDT benchmarks](https://github.com/KonradHoeffner/hdt_benchmark) are based on and for the thorough code review.
We thank Edgard Marx for proofreading the paper.

# References
