# HDT Rust
This is a fork of the archived <https://github.com/timplication/hdt-rs> that the author kindly licensed as MIT on request.
Current efforts focus on an in-memory representation and iterators for visiting all triples with a given subject, predicate or object without materializing all triples in memory. 
Detailed profiling, performance optimization and cleanup will come later.
Pull requests are welcome.
See the original README below.

# Original README

Rust implementation for the HDT library, [https://www.rdfhdt.org/](https://www.rdfhdt.org/).
The library can read out triples, but it's super slow and there's a bug here and there. I do not
really recommend using it right now. Implementing this correctly is not exactly easy because the
specification and other implementatios are sometimes conflicting.

## Authors

- Tim Baccaert [tbaccaer@vub.be](mailto:tbaccaer@vub.be)

## Acknowledgement

This file-format is not my design, I merely tried created a Rust implementation of it. For a
reference implementation and acknowledgement of all the original authors, please look at the
[https://github.com/rdfhdt](https://github.com/rdfhdt) organisation.
