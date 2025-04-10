//! *This module is available only if HDT is built with the `"sophia"` feature.*
//! Under development, parameters may change.
#[cfg(feature = "sophia")]
use argh::FromArgs;
use hdt::{Hdt, HdtGraph};
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

#[derive(FromArgs)]
/// convert HDT to N-Triples
struct Args {
    // #[argh(option, short = 'f')]
    // /// RDF Format of the output
    //format: Format,
    #[argh(switch, short = 't')]
    /// export as RDF Turtle, default is N-Triples
    turtle: bool,

    // /// verbose output
    // #[argh(switch, short = 'v')]
    //verbose: bool,
    /// the file to load, if not given it is read from stdin
    #[argh(positional)]
    filename: Option<String>,
}

fn main() {
    color_eyre::install().unwrap();
    //env_logger::init();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args: Args = argh::from_env();
    let hdt = match args.filename {
        Some(filename) => {
            let file = File::open(filename.clone()).expect("error opening file");
            let hdt = Hdt::new(std::io::BufReader::new(file)).expect("Error loading HDT from {filename}");
            info!("Loaded from file {filename} {hdt:?}");
            hdt
        }
        None => {
            let reader = BufReader::new(stdin());
            let hdt = Hdt::new(reader).expect("Error loading HDT from standard input");
            info!("Loaded from stdin {hdt:?}");
            hdt
        }
    };
    let graph = HdtGraph::new(hdt);
    let s = match args.turtle {
        true => {
            let config = TurtleConfig::new().with_pretty(true);
            //.with_own_prefix_map(prefixes().clone());
            TurtleSerializer::new_stringifier_with_config(config)
                .serialize_graph(&graph)
                .expect("error serializing graph as RDF Turtle")
                .to_string()
        }
        false => {
            // Default: export the complete graph as N-Triples.
            NtSerializer::new_stringifier()
                .serialize_graph(&graph)
                .expect("error serializing graph as N-Triples")
                .to_string()
        }
    };
    println!("{s}");
}
