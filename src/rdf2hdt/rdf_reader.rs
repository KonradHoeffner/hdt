// Copyright (c) 2024-2025, Decisym, LLC

use log::{debug, error, warn};
use oxrdfio::RdfSerializer;
use oxrdfio::{
    RdfFormat::{self, NTriples},
    RdfParseError, RdfParser,
};
use std::io::Write;
use std::{
    error::Error,
    io::{BufReader, BufWriter},
    path::Path,
};

pub fn convert_to_nt(
    file_paths: Vec<String>,
    output_file: std::fs::File,
) -> Result<(), Box<dyn Error>> {
    let mut dest_writer = BufWriter::new(output_file);
    for file in file_paths {
        let source = match std::fs::File::open(&file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", file, e);
                return Err(e.into());
            }
        };
        let source_reader = BufReader::new(source);

        debug!("converting {} to nt format", &file);

        let mut serializer = RdfSerializer::from_format(NTriples).for_writer(dest_writer.by_ref());
        let v = std::time::Instant::now();
        let rdf_format = if let Some(t) =
            RdfFormat::from_extension(Path::new(&file).extension().unwrap().to_str().unwrap())
        {
            t
        } else {
            error!("unrecognized file extension for {file}");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unrecognized file extension for {file}"),
            )
            .into());
        };
        let quads = RdfParser::from_format(rdf_format).for_reader(source_reader);
        let mut warned = false;
        for q in quads {
            let q = match q {
                Ok(v) => v,
                Err(e) => {
                    match e {
                        RdfParseError::Io(v) => {
                            // I/O error while reading file
                            error!("Error reading file {file}: {v}");
                            return Err(v.into());
                        }
                        RdfParseError::Syntax(syn_err) => {
                            error!("syntax error for RDF file {file}: {syn_err}");
                            return Err(syn_err.into());
                        }
                    }
                }
            };
            if !warned && q.graph_name != oxrdf::GraphName::DefaultGraph {
                warned = true;
                warn!("HDT does not support named graphs, merging triples for {file}");
            }
            serializer.serialize_triple(oxrdf::TripleRef {
                subject: q.subject.as_ref(),
                predicate: q.predicate.as_ref(),
                object: q.object.as_ref(),
            })?
        }

        serializer.finish()?;
        debug!("RDF to NTriple convert time: {:?}", v.elapsed());
    }
    dest_writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_rdf() {
        let tmp_file = tempfile::Builder::new().suffix(".nt").tempfile().expect("");
        assert!(
            (convert_to_nt(
                vec!["tests/resources/apple.ttl".to_string()],
                tmp_file.reopen().expect("error opening tmp file")
            ))
            .is_ok()
        );
        let source_reader = BufReader::new(tmp_file.reopen().expect("error opening tmp file"));
        let quads = RdfParser::from_format(NTriples)
            .for_reader(source_reader)
            .collect::<Result<Vec<_>, _>>();

        assert!(quads.is_ok());
        assert_eq!(quads.unwrap().len(), 9)
    }
}
