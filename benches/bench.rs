use criterion::{criterion_group, criterion_main, Criterion};
use hdt::triples::*;
use hdt::Hdt;
use hdt::HdtGraph;
use hdt::IdKind;
use sophia::api::graph::Graph;
use sophia::api::term::matcher::Any;
use sophia::api::term::IriRef;
use sophia::api::term::SimpleTerm;
use std::fs::File;

fn bench_query(c: &mut Criterion) {
    let vincent = "http://dbpedia.org/resource/Vincent_Descombes_Sevoie";
    let type_ = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let person = "http://dbpedia.org/ontology/Person";
    let file = File::open("tests/resources/persondata_en.hdt").expect("error opening file");
    //let file = File::open("tests/resources/lscomplete2015.hdt").expect("error opening file");
    //let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
    let vincent_id = hdt.dict.string_to_id(vincent, &IdKind::Subject);
    let type_id = hdt.dict.string_to_id(type_, &IdKind::Predicate);
    let person_id = hdt.dict.string_to_id(person, &IdKind::Object);
    let graph = HdtGraph::new(hdt);
    let vincent_term = SimpleTerm::Iri(IriRef::new_unchecked(vincent.into()));
    let type_term = SimpleTerm::Iri(IriRef::new_unchecked(type_.into()));
    let person_term = SimpleTerm::Iri(IriRef::new_unchecked(person.into()));
    let twp = |s, p, o| graph.hdt.triples_with_pattern(s, p, o);
    let triples = &graph.hdt.triples;

    // count to prevent optimizing away function call
    let mut group = c.benchmark_group("S??");
    //let mut group = c.benchmark_group("query");
    group.bench_function("1.1 (vincent, ?, ?) triple IDs", |b| {
        b.iter(|| SubjectIter::with_pattern(triples, &TripleId::new(vincent_id, 0, 0)).count())
    });
    group.bench_function("1.2 (vincent, ?, ?) str triples", |b| b.iter(|| twp(Some(vincent), None, None).count()));
    group.bench_function("1.3 (vincent, ?, ?) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Some(&vincent_term), Any, Any).count())
    });
    group.finish();

    let mut group = c.benchmark_group(format!("?P? {} triples", PredicateIter::new(triples, type_id).count()));
    group.sample_size(10);
    group.bench_function("2.1 (?, type, ?) triple IDs", |b| {
        b.iter(|| PredicateIter::new(triples, type_id).count())
    });
    group.bench_function("2.2 (?, type, ?) str triples", |b| b.iter(|| twp(None, Some(type_), None).count()));
    group.bench_function("2.3 (?, type, ?) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Any, Some(&type_term), Any).count())
    });
    group.finish();
    let mut group = c.benchmark_group(format!("??O {} triples", ObjectIter::new(triples, person_id).count()));
    group.bench_function("3.1 (?, ?, person) triple IDs", |b| {
        b.iter(|| ObjectIter::new(triples, person_id).count())
    });
    group.bench_function("3.2 (?, ?, person) str triples", |b| b.iter(|| twp(None, None, Some(person)).count()));
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
        b.iter(|| graph.hdt.subjects_with_po(type_, person).count())
    });
    group.bench_function("4.3 (?, type, person) str triples", |b| {
        b.iter(|| twp(None, Some(type_), Some(person)).count())
    });
    group.bench_function("4.4 (?, type, person) Sophia triples", |b| {
        b.iter(|| graph.triples_matching(Any, Some(&type_term), Some(&person_term)).count())
    });
    group.finish();
}

criterion_group!(benches, bench_query);
criterion_main!(benches);
