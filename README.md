# Rust implementation of the HDT compression format
Header Dictionary Triples (HDT) is a compression format for RDF data that can also be queried for Triple Patterns.

## History 
This is a fork of the archived <https://github.com/timplication/hdt-rs> by Tim Baccaert [tbaccaer@vub.be](mailto:tbaccaer@vub.be), who kindly licensed it as MIT on request.
Current efforts focus on an in-memory representation and iterators for visiting all triples with a given subject, predicate or object without materializing all triples in memory. 
Detailed profiling, performance optimization and cleanup will come later.
Pull requests are welcome.

For reference implementations in C++ and Java and acknowledgement of all the original authors, please look at the [https://github.com/rdfhdt](https://github.com/rdfhdt) organisation.
