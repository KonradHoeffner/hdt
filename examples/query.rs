use hdt::Hdt;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let path = std::path::Path::new("tests/resources/snikmeta.hdt");
    let file = std::fs::File::open(path)?;
    let meta_top = "http://www.snik.eu/ontology/meta/Top";
    let rdfs_label = "http://www.w3.org/2000/01/rdf-schema#label";
    #[allow(unused_mut)]
    let mut hdts = vec![Hdt::read(std::io::BufReader::new(file))?];
    #[cfg(feature = "cache")]
    hdts.push(Hdt::read_from_path(path)?);
    for hdt in hdts {
        // SP? pattern
        let labels = hdt.triples_with_pattern(Some(meta_top), Some(rdfs_label), None);
        println!("{:?}", labels.collect::<Vec<_>>());
    }
    Ok(())
}
