/// *This module is available only if HDT is built with the `"cli"` feature.*
/// Under development, parameters may change.
use bytesize::ByteSize;
use clap::{Parser, Subcommand};
use color_eyre::config::HookBuilder;
use color_eyre::eyre::{Report, WrapErr};
//use log::info;
use fs_err::{File, metadata};
use hdt::Hdt;
use hdt::containers::ControlInfo;
use hdt::header::Header;
use sophia::api::graph::Graph;
use sophia::api::prelude::{TripleSerializer, TripleSource};
//use sophia::api::prelude::Stringifier;
use sophia::inmem::graph::LightGraph;
use sophia::turtle::parser::{nt, turtle};
use sophia::turtle::serializer::nt::NtSerializer;
use sophia::turtle::serializer::turtle::{TurtleConfig, TurtleSerializer};
use std::ffi::OsStr;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::time::Instant;
//use std::io::{BufReader, stdin};

/*enum Format {
    NTriples,
    RdfXml,
    Turtle,
}*/

/// convert HDT to N-Triples
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Command,
    // /// RDF Format of the output
    //format: Format,

    // /// verbose output
    //verbose: bool,
    // disable std reading for now because of usability downside for new users when started with no parameter // the HDT file to load from, if not given it is read from stdin
}

#[derive(Subcommand)]
enum Command {
    Info {
        input_path: PathBuf,
    },
    Convert {
        // #[arg(short, long)]
        // /// export as RDF Turtle, default is N-Triples
        // turtle: bool,
        /*
        #[arg(short, long)]
        /// Count triples only, do not print them
        count: bool,
        */
        /// the HDT file to load from
        input_path: PathBuf,
        // /// the RDF file to create, if not given it is written to stdout
        // rdf_output_path: Option<String>,
        output_path: PathBuf,
    },
}

fn main() -> Result<(), Report> {
    HookBuilder::default().display_env_section(false).install()?;
    //env_logger::init();
    //env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    /*let hdt = match args.input_path {
        Some(filename) => {
            let file =
                File::open(filename.clone()).wrap_err_with(|| format!("Error opening input file {}", filename))?;
            Hdt::read(BufReader::new(file))
                .wrap_err_with(|| format!("Error loading HDT from {}", filename))?
            //info!("Loaded from file {filename} {hdt:?}");
        }
        None => {
            let reader = BufReader::new(stdin());
            Hdt::read(reader).wrap_err("Error loading HDT from standard input")?
            //info!("Loaded from stdin {hdt:?}");
        }
    };*/
    match args.command {
        Command::Info { input_path } => {
            let file = File::open(input_path.clone())
                .with_context(|| format!("Error opening input HDT file {input_path:?}"))?;
            let mut reader = BufReader::new(file);
            match input_path.extension().and_then(OsStr::to_str) {
                Some("nt") => {
                    let triples = nt::parse_bufread(reader).collect_triples();
                    let g: LightGraph = triples.unwrap();
                    println!("RDF N-Triples File with ~{} triples", g.triples().size_hint().0);
                }
                Some("ttl") => {
                    let triples = turtle::parse_bufread(reader).collect_triples();
                    let g: LightGraph = triples.unwrap();
                    println!("RDF Turtle with ~{} triples", g.triples().size_hint().0);
                }
                Some("hdt") => {
                    ControlInfo::read(&mut reader)?;
                    let header = Header::read(&mut reader)?;
                    //println!("{}",ByteSize(hdt.size_in_bytes() as u64).to_string());
                    println!("HDT File: {:#?}", header.body);
                }
                Some(x) => {
                    eprintln!("Unknown RDF extension {x:?}, aborting.");
                }
                None => {
                    println!("File has no extension, RDF format cannot be determined, aborting.");
                }
            }
        }
        Command::Convert { input_path, output_path /* turtle*/ } => {
            let t = Instant::now();
            let file = File::open(input_path.clone())
                .with_context(|| format!("Error opening input HDT file {input_path:?}"))?;
            let reader = BufReader::new(file);

            let hdt = match input_path.extension().and_then(OsStr::to_str) {
                Some("hdt") => {
                    Hdt::read(reader).with_context(|| format!("Error loading input HDT from {input_path:?}"))?
                }
                Some("nt") => Hdt::read_nt(&input_path)
                    .with_context(|| format!("Error loading input N-Triples file from {input_path:?}"))?,
                _ => {
                    panic!(
                        "Input file has unsupported or no extension, RDF format cannot be determined, aborting."
                    );
                }
            };
            // let count = hdt.triples.len();
            /*if args.count {
                println!("Parsing returned {} triples", count);
                return Ok(());
            }*/
            let output_file = File::create(&output_path)?;
            let mut writer = BufWriter::new(output_file);
            match output_path.extension().and_then(OsStr::to_str) {
                Some("ttl") => {
                    let config = TurtleConfig::new().with_pretty(true);
                    //.with_own_prefix_map(prefixes().clone());
                    //TurtleSerializer::new_stringifier_with_config(config)
                    TurtleSerializer::new_with_config(writer, config)
                        .serialize_graph(&hdt)
                        .wrap_err("error serializing graph as RDF Turtle")?;
                    //.to_string()
                }
                Some("nt") => {
                    // Default: export the complete graph as N-Triples.
                    //NtSerializer::new_stringifier()
                    NtSerializer::new(writer)
                        .serialize_graph(&hdt)
                        .wrap_err("error serializing graph as N-Triples")?;
                    //.to_string()
                }
                Some("hdt") => {
                    hdt.write(&mut writer)?;
                }
                _ => {
                    panic!(
                        "Output file has no extension or one signifying an unsupported export format, aborting."
                    );
                }
            };
            let in_size = ByteSize(metadata(&input_path)?.len());
            let out_size = ByteSize(metadata(&output_path)?.len());
            println!(
                "Successfully converted {input_path:?} ({in_size}) to {output_path:?} ({out_size}) in {:.2}s",
                t.elapsed().as_secs_f32()
            );
            // println!("{s}");
        }
    }
    Ok(())
}
