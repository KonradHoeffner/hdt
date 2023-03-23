use hdt::Hdt;
use hdt::HdtGraph;
use sophia::api::graph::Graph;
use sophia::api::term::matcher::Any;
use sophia::api::term::IriRef;
use sophia::api::term::SimpleTerm;
use std::fs::File;

const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const PERSON: &str = "http://dbpedia.org/ontology/Person";

fn load() -> HdtGraph {
    let file = File::open("tests/resources/persondata_en_10k.hdt").expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
    HdtGraph::new(hdt)
}

// iai currently does not allow excluding loading time so that has to be subtracted

fn query_all() {
    let hdt = load().hdt;
    hdt.triples_with_pattern(None, None, None).count();
}

fn query_all_sophia() {
    let graph = load();
    graph.triples_matching(Any, Any, Any).count();
}

fn query_po() {
    let hdt = load().hdt;
    hdt.triples_with_pattern(None, Some(TYPE), Some(PERSON)).count();
}

fn query_po_sophia() {
    let graph = load();
    let type_term = SimpleTerm::Iri(IriRef::new_unchecked(TYPE.into()));
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    graph.triples_matching(Any, Some(&type_term), Some(&person_term)).count();
}

fn query_o() {
    let hdt = load().hdt;
    hdt.triples_with_pattern(None, None, Some(PERSON)).count();
}

fn query_o_sophia() {
    let graph = load();
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    graph.triples_matching(Any, Any, Some(&person_term)).count();
}

iai::main!(load, query_all, query_all_sophia, query_po, query_po_sophia, query_o, query_o_sophia);
