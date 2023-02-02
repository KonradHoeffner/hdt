use criterion::{criterion_group, criterion_main, Criterion};
use hdt::triples::*;
use hdt::Hdt;
use sophia::api::ns::rdf;
use std::fs::File;

fn bench_subject_iter(c: &mut Criterion) {
    let dbo_person = "http://dbpedia.org/ontology/Person";
    let dbr_vincent = "http://dbpedia.org/resource/Vincent_Descombes_Sevoie";
    let type_ = rdf::type_.to_string();
    let file = File::open("tests/resources/persondata_en.hdt").expect("error opening file");
    //let file = File::open("tests/resources/lscomplete2015.hdt").expect("error opening file");
    //let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).unwrap();
    let triples = &hdt.triples;
    // count to prevent optimizing away function call
    /*
    let mut group = c.benchmark_group("query triple IDs");
    group.bench_function("S??", |b| {
        b.iter(|| SubjectIter::with_pattern(&triples, &TripleId::new(1, 0, 0)).count())
    });
    group.sample_size(10);
    group.bench_function("?P?", |b| {
        b.iter(|| SubjectIter::with_pattern(&triples, &TripleId::new(0, 1, 0)).count())
    });
    group.bench_function("??O", |b| {
        b.iter(|| SubjectIter::with_pattern(&triples, &TripleId::new(0, 0, 1)).count())
    });
    group.finish();
    */
    let mut group = c.benchmark_group("query string triples");
    group.bench_function("(vincent, ?, ?)", |b| {
        b.iter(|| hdt.triples_with_pattern(Some(dbr_vincent), None, None).count())
    });
    group.sample_size(10);
    group
        .bench_function("(?, type, ?)", |b| b.iter(|| hdt.triples_with_pattern(None, Some(&type_), None).count()));
    group.bench_function("(?, ?, dbo_person)", |b| {
        b.iter(|| hdt.triples_with_pattern(None, None, Some(dbo_person)).count())
    });
    group.bench_function("(?, type, person)", |b| {
        b.iter(|| hdt.triples_with_pattern(None, Some(&type_), Some(dbo_person)).count())
    });
    group.finish();
}

criterion_group!(benches, bench_subject_iter);
criterion_main!(benches);
