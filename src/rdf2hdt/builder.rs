// Copyright (c) 2024-2025, Decisym, LLC

use super::{bitmap_triples::BitmapTriplesBuilder, dictionary::FourSectDictBuilder};
use crate::{
    containers::{self, ControlType},
    rdf2hdt::{rdf_reader::convert_to_nt, vocab::*},
};
use log::{debug, error};
use oxrdf::{BlankNodeRef, Literal, NamedNodeRef, Triple, vocab::rdf};
use std::{
    collections::HashSet,
    error::Error,
    fs::File,
    io::{BufWriter, Write},
};

#[derive(Clone, Debug)]
pub struct Options {
    pub block_size: usize,
    pub order: String,
}
impl Default for Options {
    fn default() -> Self {
        Options { block_size: 16, order: "SPO".to_string() }
    }
}

pub fn build_hdt(file_paths: Vec<String>, dest_file: &str, opts: Options) -> Result<ConvertedHDT, Box<dyn Error>> {
    if file_paths.is_empty() {
        error!("no files provided");
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "no files provided to convert").into());
    }

    let timer = std::time::Instant::now();
    // TODO
    // implement an RDF reader trait
    // 1. for larger datasets, read from source files everytime since storing all triples in memory may OOM kill process
    // 2. build Vec<Triple> in memory from source files
    let nt_file = if file_paths.len() == 1 && file_paths[0].ends_with(".nt") {
        file_paths[0].clone()
    } else {
        let tmp_file = tempfile::Builder::new().suffix(".nt").keep(true).tempfile()?;
        convert_to_nt(file_paths, tmp_file.reopen()?)?;
        tmp_file.path().to_str().unwrap().to_string()
    };

    let converted_hdt = ConvertedHDT::load(&nt_file, opts)?;
    debug!("HDT build time: {:?}", timer.elapsed());

    converted_hdt.save(dest_file)?;

    debug!("Total execution time: {:?}", timer.elapsed());
    Ok(converted_hdt)
}

impl ConvertedHDT {
    fn load(nt_file: &str, opts: Options) -> Result<Self, Box<dyn Error>> {
        let (dictionary, encoded_triples) = FourSectDictBuilder::load(nt_file, opts.clone())?;
        let num_triples = encoded_triples.len();
        let bmap_triples = BitmapTriplesBuilder::load(encoded_triples)?;

        let mut converted_hdt =
            ConvertedHDT { dict: dictionary, triples: bmap_triples, num_triples, ..Default::default() };
        converted_hdt.build_header(nt_file, opts)?;

        Ok(converted_hdt)
    }

    pub fn save(&self, dest_file: &str) -> Result<(), Box<dyn Error>> {
        let timer = std::time::Instant::now();

        let file = File::create(dest_file)?;
        let mut dest_writer = BufWriter::new(file);

        // libhdt/src/hdt/BasicHDT.cpp::saveToHDT
        let ci = containers::ControlInfo {
            control_type: ControlType::Global,
            format: HDT_CONTAINER.to_string(),
            ..Default::default()
        };
        ci.save(&mut dest_writer)?;

        let mut ci = containers::ControlInfo {
            control_type: ControlType::Header,
            format: "ntriples".to_string(),
            ..Default::default()
        };
        let mut graph: oxrdf::Graph = oxrdf::Graph::new();
        for t in &self.header {
            graph.insert(t);
        }
        let graph_string = graph.to_string();
        ci.properties.insert("length".to_string(), graph_string.len().to_string());
        ci.save(&mut dest_writer)?;
        let _ = dest_writer.write(graph_string.as_bytes())?;

        self.dict.save(&mut dest_writer)?;

        self.triples.save(&mut dest_writer)?;
        dest_writer.flush()?;
        debug!("HDT file output time: {:?}", timer.elapsed());
        Ok(())
    }

    fn build_header(&mut self, source_file: &str, opts: Options) -> Result<(), Box<dyn Error>> {
        let mut header = HashSet::new();
        // libhdt/src/hdt/BasicHDT.cpp::fillHeader()

        // uint64_t origSize = header->getPropertyLong(statisticsNode.c_str(), HDTVocabulary::ORIGINAL_SIZE.c_str());

        // header->clear();
        let file_iri = format!("file://{}", std::path::Path::new(source_file).canonicalize()?.display());
        let base_iri = NamedNodeRef::new(&file_iri)?;
        // // BASE
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::HDT_DATASET);
        header.insert(Triple::new(base_iri, rdf::TYPE, HDT_CONTAINER));

        // // VOID
        // header->insert(baseUri, HDTVocabulary::RDF_TYPE, HDTVocabulary::VOID_DATASET);
        header.insert(Triple::new(base_iri, rdf::TYPE, VOID_DATASET));
        // header->insert(baseUri, HDTVocabulary::VOID_TRIPLES, triples->getNumberOfElements());
        header.insert(Triple::new(
            base_iri,
            VOID_TRIPLES,
            Literal::new_simple_literal(self.num_triples.to_string()),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_PROPERTIES, dictionary->getNpredicates());
        header.insert(Triple::new(
            base_iri,
            VOID_PROPERTIES,
            Literal::new_simple_literal(self.dict.predicate_terms.len().to_string()),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_SUBJECTS, dictionary->getNsubjects());
        header.insert(Triple::new(
            base_iri,
            VOID_DISTINCT_SUBJECTS,
            Literal::new_simple_literal(
                (self.dict.subject_terms.len() + self.dict.shared_terms.len()).to_string(),
            ),
        ));
        // header->insert(baseUri, HDTVocabulary::VOID_DISTINCT_OBJECTS, dictionary->getNobjects());
        header.insert(Triple::new(
            base_iri,
            VOID_DISTINCT_OBJECTS,
            Literal::new_simple_literal((self.dict.object_terms.len() + self.dict.shared_terms.len()).to_string()),
        ));
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = BlankNodeRef::new("statistics")?;
        let pub_id = BlankNodeRef::new("publicationInformation")?;
        let format_id = BlankNodeRef::new("format")?;
        let dict_id = BlankNodeRef::new("dictionary")?;
        let triples_id = BlankNodeRef::new("triples")?;
        // header->insert(baseUri, HDTVocabulary::HDT_STATISTICAL_INFORMATION,	statisticsNode);
        header.insert(Triple::new(base_iri, HDT_STATISTICAL_INFORMATION, stats_id));
        // header->insert(baseUri, HDTVocabulary::HDT_PUBLICATION_INFORMATION,	publicationInfoNode);
        header.insert(Triple::new(base_iri, HDT_STATISTICAL_INFORMATION, pub_id));
        // header->insert(baseUri, HDTVocabulary::HDT_FORMAT_INFORMATION, formatNode);
        header.insert(Triple::new(base_iri, HDT_FORMAT_INFORMATION, format_id));
        // header->insert(formatNode, HDTVocabulary::HDT_DICTIONARY, dictNode);
        header.insert(Triple::new(format_id, HDT_DICTIONARY, dict_id));
        // header->insert(formatNode, HDTVocabulary::HDT_TRIPLES, triplesNode);
        header.insert(Triple::new(format_id, HDT_TRIPLES, triples_id));

        // DICTIONARY
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_NUMSHARED, getNshared());
        header.insert(Triple::new(
            dict_id,
            HDT_DICT_SHARED_SO,
            Literal::new_simple_literal(self.dict.shared_terms.len().to_string()),
        ));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_MAPPING, this->mapping);
        header.insert(Triple::new(dict_id, HDT_DICT_MAPPING, Literal::new_simple_literal("1")));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_SIZE_STRINGS, size());
        header.insert(Triple::new(dict_id, HDT_DICT_SIZE_STRINGS, Literal::new_simple_literal("FIXME")));
        // header.insert(rootNode, HDTVocabulary::DICTIONARY_BLOCK_SIZE, this->blocksize);
        header.insert(Triple::new(
            dict_id,
            HDT_DICT_BLOCK_SIZE,
            Literal::new_simple_literal(opts.block_size.to_string()), // TODO is this always 16?
        ));

        // TRIPLES
        // header.insert(rootNode, HDTVocabulary::TRIPLES_TYPE, getType());
        header.insert(Triple::new(triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_NUM_TRIPLES, getNumberOfElements() );
        header.insert(Triple::new(
            triples_id,
            HDT_NUM_TRIPLES,
            Literal::new_simple_literal(self.num_triples.to_string()),
        ));
        // header.insert(rootNode, HDTVocabulary::TRIPLES_ORDER, getOrderStr(order) );
        header.insert(Triple::new(triples_id, HDT_TRIPLES_ORDER, Literal::new_simple_literal(opts.order)));

        // // Sizes
        let meta = File::open(std::path::Path::new(source_file))?.metadata().unwrap();
        // header->insert(statisticsNode, HDTVocabulary::ORIGINAL_SIZE, origSize);
        header.insert(Triple::new(
            stats_id,
            HDT_ORIGINAL_SIZE,
            Literal::new_simple_literal(meta.len().to_string()),
        ));
        // header->insert(statisticsNode, HDTVocabulary::HDT_SIZE, getDictionary()->size() + getTriples()->size());
        header.insert(Triple::new(stats_id, HDT_SIZE, Literal::new_simple_literal("FIXME")));

        // // Current time
        // struct tm* today = localtime(&now);
        // strftime(date, 40, "%Y-%m-%dT%H:%M:%S%z", today);
        // header->insert(publicationInfoNode, HDTVocabulary::DUBLIN_CORE_ISSUED, date);
        let now = chrono::Utc::now(); // Get current local datetime
        let datetime_str = now.format("%Y-%m-%dT%H:%M:%S%z").to_string(); // Format as string
        header.insert(Triple::new(pub_id, DC_TERMS_ISSUED, Literal::new_simple_literal(datetime_str)));

        self.header = header;

        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct ConvertedHDT {
    pub dict: FourSectDictBuilder,
    pub triples: BitmapTriplesBuilder,
    header: HashSet<oxrdf::Triple>,
    num_triples: usize,
}

#[cfg(test)]
mod tests {

    use std::{
        fs::remove_file,
        io::{BufReader, Read},
        path::Path,
    };

    use crate::{Hdt, containers::ControlInfo, four_sect_dict, header::Header, triples};

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_build_hdt() {
        let output_file = "test.hdt";
        let _ = remove_file(output_file);

        let res = build_hdt(vec!["tests/resources/apple.ttl".to_string()], output_file, Options::default());
        assert!(res.is_ok());
        let conv_hdt = res.unwrap();

        let p = Path::new(output_file);
        assert!(p.exists());
        let source = std::fs::File::open(p).expect("failed to open hdt file");
        let mut hdt_reader = BufReader::new(source);

        let _ci = ControlInfo::read(&mut hdt_reader).expect("failed to read HDT control info");
        let _h = Header::read(&mut hdt_reader).expect("failed to read HDT Header");

        let unvalidated_dict =
            four_sect_dict::FourSectDict::read(&mut hdt_reader).expect("failed to read dictionary");
        let dict = unvalidated_dict.validate().expect("invalid 4 section dictionary");
        assert_eq!(dict.objects.num_strings(), conv_hdt.dict.object_terms.len());
        assert_eq!(dict.subjects.num_strings(), conv_hdt.dict.subject_terms.len());
        assert_eq!(dict.predicates.num_strings(), conv_hdt.dict.predicate_terms.len());
        assert_eq!(dict.shared.num_strings(), conv_hdt.dict.shared_terms.len());

        let _triples = triples::TriplesBitmap::read_sect(&mut hdt_reader).expect("invalid bitmap triples");
        let mut buffer = [0; 1024];
        assert!(hdt_reader.read(&mut buffer).expect("failed to read") == 0);

        let source = std::fs::File::open(p).expect("failed to open hdt file");
        let hdt_reader = BufReader::new(source);
        let h = Hdt::new(hdt_reader).expect("failed to load HDT file");
        let t: Vec<(Arc<str>, Arc<str>, Arc<str>)> = h.triples().collect();
        println!("{:?}", t);
        assert_eq!(t.len(), 9);

        // http://example.org/apple#Apple,http://example.org/apple#color,Red
        let s = "http://example.org/apple#Apple";
        let p = "http://example.org/apple#color";
        let o = "\"Red\"";
        let triple_vec = vec![(Arc::from(s), Arc::from(p), Arc::from(o))];

        let res = h.triples_with_pattern(None, Some(p), Some(o)).collect::<Vec<_>>();
        assert_eq!(triple_vec, res)
    }
}
