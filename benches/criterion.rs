use criterion::{Criterion, criterion_group, criterion_main};
use hdt::Hdt;
use hdt::HdtGraph;
use hdt::IdKind;
use hdt::triples::*;
use sophia::api::graph::Graph;
use sophia::api::term::IriRef;
use sophia::api::term::SimpleTerm;
use sophia::api::term::matcher::Any;
use std::fs::File;

const VINCENT: &str = "http://dbpedia.org/resource/Vincent_Descombes_Sevoie";
const TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const PERSON: &str = "http://dbpedia.org/ontology/Person";

fn load() -> HdtGraph {
    let filename = "tests/resources/persondata_en.hdt";
    let file = File::open(filename)
        .expect(&format!("Error opening file {filename}, did you forget to download it? See README.md."));
    //let file = File::open("tests/resources/lscomplete2015.hdt").expect("error opening file");
    //let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
    HdtGraph::new(hdt)
}

fn query(c: &mut Criterion) {
    let graph = load();
    let triples = &graph.hdt.triples;
    let twp = |s, p, o| graph.hdt.triples_with_pattern(s, p, o);

    let vincent_id = graph.hdt.dict.string_to_id(VINCENT, &IdKind::Subject);
    let type_id = graph.hdt.dict.string_to_id(TYPE, &IdKind::Predicate);
    let person_id = graph.hdt.dict.string_to_id(PERSON, &IdKind::Object);
    let vincent_term = SimpleTerm::Iri(IriRef::new_unchecked(VINCENT.into()));
    let type_term = SimpleTerm::Iri(IriRef::new_unchecked(TYPE.into()));
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(PERSON.into()));

    // count to prevent optimizing away function call
    let mut group = c.benchmark_group("??? (all)");
    group.sample_size(10);
    group.bench_function("0.1 all triple IDs", |b| b.iter(|| graph.hdt.triples.into_iter().count()));
    group.bench_function("0.2 all str triples", |b| b.iter(|| graph.hdt.triples().count()));
    group.bench_function("0.3 all Sophia triples", |b| b.iter(|| graph.triples().count()));
    group.finish();
    let mut group = c.benchmark_group("S??");
    //let mut group = c.benchmark_group("query");
    group.bench_function("1.1 (vincent, ?, ?) triple IDs", |b| {
        b.iter(|| SubjectIter::with_pattern(triples, &TripleId::new(vincent_id, 0, 0)).count())
    });
    group.bench_function("1.2 (vincent, ?, ?) str triples", |b| b.iter(|| twp(Some(VINCENT), None, None).count()));
    group.bench_function("1.3 (vincent, ?, ?) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Some(&vincent_term), Any, Any).count())
    });
    group.finish();

    let mut group = c.benchmark_group(format!("?P? {} triples", PredicateIter::new(triples, type_id).count()));
    group.sample_size(10);
    group.bench_function("2.1 (?, type, ?) triple IDs", |b| {
        b.iter(|| PredicateIter::new(triples, type_id).count())
    });
    group.bench_function("2.2 (?, type, ?) str triples", |b| b.iter(|| twp(None, Some(TYPE), None).count()));
    group.bench_function("2.3 (?, type, ?) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Any, Some(&type_term), Any).count())
    });
    group.finish();
    let mut group = c.benchmark_group(format!("??O {} triples", ObjectIter::new(triples, person_id).count()));
    group.bench_function("3.1 (?, ?, person) triple IDs", |b| {
        b.iter(|| ObjectIter::new(triples, person_id).count())
    });
    group.bench_function("3.2 (?, ?, person) str triples", |b| b.iter(|| twp(None, None, Some(PERSON)).count()));
    group.bench_function("3.3 (?, ?, person) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Any, Any, Some(&person_term)).count())
    });
    group.finish();
    let mut group = c
        .benchmark_group(format!("?PO {} triples", PredicateObjectIter::new(triples, type_id, person_id).count()));
    group.sample_size(10);
    group.bench_function("4.1 (?, type, person) triple IDs", |b| {
        b.iter(|| PredicateObjectIter::new(triples, type_id, person_id).count())
    });
    group.bench_function("4.2 (?, type, person) str subjects", |b| {
        b.iter(|| graph.hdt.subjects_with_po(TYPE, PERSON).count())
    });
    group.bench_function("4.3 (?, type, person) str triples", |b| {
        b.iter(|| twp(None, Some(TYPE), Some(PERSON)).count())
    });
    group.bench_function("4.4 (?, type, person) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Any, Some(&type_term), Some(&person_term)).count())
    });
    group.finish();
}

criterion_group!(criterion, query);
criterion_main!(criterion);
