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

We present a Rust library for the Header Data Triples binary RDF compression format with the following design goals:

* loading the HDT default format as created by [hdt-cpp](https://github.com/rdfhdt/hdt-cpp)
* efficient querying by triple patterns
* serializing into other formats like RDF Turtle and N-Triples using the [Sophia](https://crates.io/crates/sophia) adapter

Non-goals:

* load other RDF formats
* load other HDT variants 

It also cannot:

* swap data to disk
* modify the RDF graph in memory
* run SPARQL queries

# Statement of need

Semantic Web technologies have seen adoption by major tech companies in recent years in the form of *knowledge graphs*,
but widespread industry use is still inhibited by a lack of freely available performant, accessible, robust and adaptable tooling [@semanticwebreview].

While SPARQL endpoints provide a standard publication channel and API to any knowledge base, they are not suitable for all use cases.
On small knowledge bases, the overhead in both memory and CPU resources is too large.
On large knowledge bases on the other hand, query complexity may cause an overload of the server, causing delayed or missed responses.

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Acknowledgements

For this functionality and acknowledgement of all the original authors, please look at the reference implementations in C++ and Java by the [https://github.com/rdfhdt](https://github.com/rdfhdt) organisation.

# References
