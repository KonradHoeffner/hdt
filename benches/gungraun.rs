use color_eyre::eyre::WrapErr;
use fs_err::File;
use gungraun::{library_benchmark, library_benchmark_group, main};
use hdt::Hdt;
use sophia::api::graph::Graph;
use sophia::api::term::matcher::Any;
use sophia::api::term::{IriRef, SimpleTerm};

const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const PERSON: &str = "http://dbpedia.org/ontology/Person";

fn load() -> Hdt {
    color_eyre::install().unwrap();
    let n = "tests/resources/persondata_en_10k.hdt";
    let f = File::open(n).wrap_err(format!("Error opening {n}, did you download it? See README.md.")).unwrap();
    Hdt::read(std::io::BufReader::new(f)).unwrap()
}

#[library_benchmark]
fn bench_load() -> Hdt {
    load()
}

#[library_benchmark]
#[bench::default(load())]
fn query_all(hdt: Hdt) {
    hdt.triples_with_pattern(None, None, None).count();
}

#[library_benchmark]
#[bench::default(load())]
fn query_all_sophia(hdt: Hdt) {
    hdt.triples_matching(Any, Any, Any).count();
}

#[library_benchmark]
#[bench::default(load())]
fn query_po(hdt: Hdt) {
    hdt.triples_with_pattern(None, Some(TYPE), Some(PERSON)).count();
}

#[library_benchmark]
#[bench::default(load())]
fn query_po_sophia(hdt: Hdt) {
    let type_term = SimpleTerm::Iri(IriRef::new_unchecked(TYPE.into()));
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    hdt.triples_matching(Any, Some(&type_term), Some(&person_term)).count();
}

#[library_benchmark]
#[bench::default(load())]
fn query_o(hdt: Hdt) {
    hdt.triples_with_pattern(None, None, Some(PERSON)).count();
}

#[library_benchmark]
#[bench::default(load())]
fn query_o_sophia(hdt: Hdt) {
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));
    hdt.triples_matching(Any, Any, Some(&person_term)).count();
}

library_benchmark_group!(
    name = queries;
    benchmarks = bench_load, query_all, query_all_sophia, query_po, query_po_sophia, query_o, query_o_sophia
);
main!(library_benchmark_groups = queries);
