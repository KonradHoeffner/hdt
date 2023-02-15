use criterion::{criterion_group, criterion_main, Criterion};
use hdt::triples::*;
use hdt::Hdt;
use hdt::IdKind;
use std::fs::File;

fn bench_query(c: &mut Criterion) {
    let vincent = "http://dbpedia.org/resource/Vincent_Descombes_Sevoie";
    let type_ = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    let person = "http://dbpedia.org/ontology/Person";
    let file = File::open("tests/resources/persondata_en.hdt").expect("error opening file");
    //let file = File::open("tests/resources/lscomplete2015.hdt").expect("error opening file");
    //let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
    let triples = &hdt.triples;
    let vincent_id = hdt.dict.string_to_id(vincent, &IdKind::Subject);
    let type_id = hdt.dict.string_to_id(type_, &IdKind::Predicate);
    let person_id = hdt.dict.string_to_id(person, &IdKind::Object);
    let twp = |s, p, o| hdt.triples_with_pattern(s, p, o);

    // count to prevent optimizing away function call
    //let mut group = c.benchmark_group("S??");
    let mut group = c.benchmark_group("query");
    group.bench_function("1 triple IDs (vincent, ?, ?)", |b| {
        b.iter(|| SubjectIter::with_pattern(triples, &TripleId::new(vincent_id, 0, 0)).count())
    });
    group.bench_function("2 str triples (vincent, ?, ?)", |b| b.iter(|| twp(Some(vincent), None, None).count()));
    //group.finish();
    //let mut group = c.benchmark_group("?P?");
    group.sample_size(10);
    group.bench_function("3 triple IDs (?, type, ?)", |b| b.iter(|| PredicateIter::new(triples, type_id).count()));
    group.bench_function("4 str triples (?, type, ?)", |b| b.iter(|| twp(None, Some(type_), None).count()));
    //group.finish();
    //let mut group = c.benchmark_group("??O");
    group
        .bench_function("5 triple IDs (?, ?, person)", |b| b.iter(|| ObjectIter::new(triples, person_id).count()));
    group.bench_function("6 str triples (?, ?, person)", |b| b.iter(|| twp(None, None, Some(person)).count()));
    //group.finish();
    //let mut group = c.benchmark_group("?PO");
    group.sample_size(10);
    group.bench_function("7 triple IDs (?, type, person)", |b| {
        b.iter(|| PredicateObjectIter::new(triples, type_id, person_id).count())
    });
    group.bench_function("8 str subjects (?, type, person)", |b| {
        b.iter(|| hdt.subjects_with_po(type_, person).count())
    });
    group.bench_function("9 str triples (?, type, person)", |b| {
        b.iter(|| twp(None, Some(type_), Some(person)).count())
    });
    group.finish();
}

criterion_group!(benches, bench_query);
criterion_main!(benches);
