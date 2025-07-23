#![cfg(feature = "sophia")]
use hdt::Hdt;
use sophia::api::prelude::{Stringifier, TripleSerializer};
use sophia::turtle::serializer::turtle::{TurtleConfig, TurtleSerializer};
use std::fs::File;
use std::io::Write;

fn main() -> color_eyre::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let path = std::path::Path::new("tests/resources/snikmeta.hdt");
    let file = File::open(path)?;
    let hdt = Hdt::read(std::io::BufReader::new(file))?;
    let mut writer = std::io::BufWriter::new(File::create("/tmp/out.hdt")?);

    // write as HDT
    hdt.write(&mut writer)?;

    // write in other formats using Sophia
    // N-Triples
    let mut nt_writer = std::io::BufWriter::new(File::create("/tmp/out.nt")?);
    hdt.write_nt(&mut nt_writer)?;
    // Turtle
    let mut turtle_writer = std::io::BufWriter::new(File::create("/tmp/out.ttl")?);
    // disable pretty printing for large files as it is very slow
    let config = TurtleConfig::new().with_pretty(true);
    //.with_own_prefix_map(prefixes().clone()); // if you have a prefix map
    let turtle = TurtleSerializer::new_stringifier_with_config(config).serialize_graph(&hdt)?.to_string();
    writeln!(turtle_writer, "{turtle}")?;
    // other formats: see Sophia docs https://docs.rs/sophia/latest/sophia
    Ok(())
}
