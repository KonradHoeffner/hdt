// Copyright (c) 2024-2025, Decisym, LLC
fn main() {}
/*
//! # rdf2hdt Converter
//!
//! This is a Rust-based tool that converts RDF data into HDT format. It uses the `oxrdfio` crate
//! for RDF parsing and conversion, and then generates and saves the data as HDT.
//! Implementation is based on the [HDT specification](https://www.w3.org/submissions/2011/SUBM-HDT-20110330)
//! and the output HDT is intended to be consumed by one of [hdt crate](https://github.com/KonradHoeffner/hdt),
//! [hdt-cpp](https://github.com/rdfhdt/hdt-cpp), or [hdt-java](https://github.com/rdfhdt/hdt-java).
//!
//! ## Features
//! - Parses RDF input and converts it to RDF triples
//! - Convert NTriple data into HDT format
//!
//! ## Usage
//! Run the rdf2hdt converter from the command line. For detailed usage information, run:
//! ```
//! rdf2hdt --help
//! ```
//!
//! ## Example
//! To convert a RDF file to HDT format and write to the specified output file:
//! ```
//! rdf2hdt convert --input data.ttl --output result.hdt
//! ```
//! This will take `data.ttl`, convert to NTriple, and generate and save the HDT output to `result.hdt`.

use clap::{Parser, Subcommand};
use hdt::rdf2hdt::builder::{Options, build_hdt};

/// Command-line interface for rdf2hdt Converter
///
/// This struct defines the command-line interface (CLI) for interacting with the rdf2hdt converter.
#[derive(Parser)]
#[command(version, about = "Converts RDF data into HDT format.")]
struct Cli {
    /// CLI command selection
    #[command(subcommand)]
    command: Option<Commands>,
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity,
}

/// Supported Commands
///
/// Contains the available commands for the rdf2hdt converter.
#[derive(Subcommand)]
enum Commands {
    /// Convert RDF to HDT.
    ///
    /// The `convert` command parses RDF files, converts it to RDF triples using `oxrdfio` for parsing
    /// and conversion, and then generates and saves the data as HDT.
    Convert {
        /// Path to input RDF file(s).
        ///
        /// Provide the path to one or more RDF files that will be parsed and converted.
        /// Support file formats: https://crates.io/crates/oxrdfio
        #[arg(short, long, num_args = 1..)]
        input: Vec<String>,

        /// Path to output file.
        ///
        /// Specify the path to save the generated HDT.
        #[arg(short, long)]
        output: String,

        /// Block size used during term compression
        ///
        /// Every Nth term will be stored fully while others will only contain everything besides the
        /// longest common prefix of the last Nth term
        #[arg(short, long, default_value_t = 16)]
        block_size: usize,
    },
}

fn main() {
    let cli = Cli::parse();

    env_logger::Builder::new().filter_level(cli.verbose.log_level_filter()).init();

    match &cli.command {
        Some(Commands::Convert { input, output, block_size }) => {
            match build_hdt(input.clone(), output, Options { block_size: *block_size, order: "SPO".to_string() }) {
                Ok(_) => {}
                Err(e) => eprintln!("Error writing: {}", e),
            }
        }
        None => {}
    }
}
*/
