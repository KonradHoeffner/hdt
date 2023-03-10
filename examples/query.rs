use hdt::Hdt;

fn main() {
    let filename = "tests/resources/snikmeta.hdt";
    let file = std::fs::File::open(filename).expect("error opening file");
    let hdt = Hdt::new(std::io::BufReader::new(file)).expect("error loading HDT");
    let meta_top = "http://www.snik.eu/ontology/meta/Top";
    let rdfs_label = "http://www.w3.org/2000/01/rdf-schema#label";
    // SP? pattern
    let labels = hdt.triples_with_pattern(Some(meta_top), Some(rdfs_label), None);
    println!("{:?}", labels.collect::<Vec<_>>());
}
