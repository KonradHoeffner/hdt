use color_eyre::Result;
use color_eyre::eyre::WrapErr;
use fs_err::File;
use hdt::Hdt;
use sophia::api::graph::Graph;
use sophia::api::term::matcher::Any;
use sophia::api::term::{IriRef, SimpleTerm};

const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const PERSON: &str = "http://dbpedia.org/ontology/Person";

fn load() -> Result<Hdt> {
    color_eyre::install().unwrap();
    let n = "tests/resources/persondata_en_10k.hdt";
    let f = File::open(n).wrap_err(format!("Error opening {n}, did you download it? See README.md."))?;
    Ok(Hdt::read(std::io::BufReader::new(f))?)
}

// iai currently does not allow excluding loading time so that has to be subtracted

fn query_all() {
    let hdt = load().unwrap();
    hdt.triples_with_pattern(None, None, None).count();
}

fn query_all_sophia() {
    let graph = load().unwrap();
    graph.triples_matching(Any, Any, Any).count();
}

fn query_po() {
    let hdt = load().unwrap();
    hdt.triples_with_pattern(None, Some(TYPE), Some(PERSON)).count();
}

fn query_po_sophia() {
    let graph = load().unwrap();
    let type_term = SimpleTerm::Iri(IriRef::new_unchecked(TYPE.into()));
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    graph.triples_matching(Any, Some(&type_term), Some(&person_term)).count();
}

fn query_o() {
    let hdt = load().unwrap();
    hdt.triples_with_pattern(None, None, Some(PERSON)).count();
}

fn query_o_sophia() {
    let graph = load().unwrap();
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    graph.triples_matching(Any, Any, Some(&person_term)).count();
}

iai::main!(load, query_all, query_all_sophia, query_po, query_po_sophia, query_o, query_o_sophia);
