#[cfg(feature = "sophia")]
#[cfg(feature = "cli")]
/// *This module is available only if HDT is built with the `"sophia"` feature.*
/// Under development, parameters may change.
use clap::Parser;
use color_eyre::config::HookBuilder;
use color_eyre::eyre::{Report, WrapErr};
use hdt::Hdt;
use log::info;
use sophia::api::prelude::{Stringifier, TripleSerializer};
use sophia::turtle::serializer::nt::NtSerializer;
use sophia::turtle::serializer::turtle::{TurtleConfig, TurtleSerializer};
use std::fs::File;
use std::io::{BufReader, stdin};

/*enum Format {
    NTriples,
    RdfXml,
    Turtle,
}*/

/// convert HDT to N-Triples
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    // /// RDF Format of the output
    //format: Format,
    #[arg(short, long, default_value_t = false)]
    /// export as RDF Turtle, default is N-Triples
    turtle: bool,

    // /// verbose output
    //verbose: bool,
    /// the HDT file to load from, if not given it is read from stdin
    hdt_input_file: Option<String>,
    /// the RDF file to create, if not given it is written to stdout
    rdf_output_file: Option<String>,
}

fn main() -> Result<(), Report> {
    HookBuilder::default().display_env_section(false).install()?;
    //env_logger::init();
    //env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let hdt = match args.hdt_input_file {
        Some(filename) => {
            let file =
                File::open(filename.clone()).wrap_err_with(|| format!("Error opening input file {}", filename))?;
            let hdt = Hdt::read(std::io::BufReader::new(file))
                .wrap_err_with(|| format!("Error loading HDT from {}", filename))?;
            //info!("Loaded from file {filename} {hdt:?}");
            hdt
        }
        None => {
            let reader = BufReader::new(stdin());
            let hdt = Hdt::read(reader).wrap_err("Error loading HDT from standard input")?;
            info!("Loaded from stdin {hdt:?}");
            hdt
        }
    };
    let s = match args.turtle {
        true => {
            let config = TurtleConfig::new().with_pretty(true);
            //.with_own_prefix_map(prefixes().clone());
            TurtleSerializer::new_stringifier_with_config(config)
                .serialize_graph(&hdt)
                .wrap_err("error serializing graph as RDF Turtle")?
                .to_string()
        }
        false => {
            // Default: export the complete graph as N-Triples.
            NtSerializer::new_stringifier()
                .serialize_graph(&hdt)
                .wrap_err("error serializing graph as N-Triples")?
                .to_string()
        }
    };
    println!("{s}");
    Ok(())
}
