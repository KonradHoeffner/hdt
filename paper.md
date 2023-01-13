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

We present a Rust library for the Header Data Triples (HDT) binary RDF compression format.
This allows writing high-performance Rust applications that load and query HDT files.

# Statement of need

Semantic Web technologies have seen adoption by major tech companies in recent years
but widespread use is still inhibited by a lack of freely available performant, accessible, robust and adaptable tooling [@semanticwebreview].
SPARQL endpoints provide a standard publication channel and API to any RDF graph but they are not suitable for all use cases.
On small knowledge bases, there is a large relative overhead in both memory and CPU resources.
On large knowledge bases on the other hand, query complexity and shared access may cause an overload of the server, causing delayed or missed responses.
Finally, downtime of a SPARQL endpoint causes all applications depending on it to stop working.

To insulate against such problems, Semantic Web applications may integrate and query an RDF graph using libraries such as Apache Jena [@jena] for Java,
RDFlib for Python or Sophia [@sophia] for Rust.
However those libraries do not scale to large RDF graphs due to their excessive memory usage, which can be drastically lowered by using the Header Data Triples (HDT) binary RDF compression format, which still allows efficient queries [@hdt2012, @hdt2013].
Implementations exist for C++ [@hdtcpp] and Java [@hdtjava] but not for Rust, a popular modern, statically typed high-level programming language that allows writing performant software while still ensuring memory safety, which aligns with the challenges to the adoption of the Semantic Web.

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Related Work

@hdt2012
@hdt2013

* loading the HDT default format as created by [hdt-cpp](https://github.com/rdfhdt/hdt-cpp)
* serializing into other formats like RDF Turtle and N-Triples using the [Sophia](https://crates.io/crates/sophia) adapter

Non-goals:

* load other RDF formats
* load other HDT variants 

It also cannot:

* swap data to disk
* modify the RDF graph in memory
* run SPARQL queries


# Acknowledgements

We thank Pierre-Antoine Champin for explaining the details of Sophia and for supplying the benchmarking code.
For this functionality and acknowledgement of all the original authors, please look at the reference implementations in C++ and Java by the [https://github.com/rdfhdt](https://github.com/rdfhdt) organisation.

# References
