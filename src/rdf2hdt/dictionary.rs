// Copyright (c) 2024-2025, Decisym, LLC

use log::{debug, error};
use oxrdf::Term;
use oxrdfio::RdfFormat::NTriples;
use oxrdfio::RdfParser;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    error::Error,
    fs::File,
    io::{BufReader, BufWriter},
    sync::Arc,
};

use crate::{
    containers::{self, ControlType, Sequence, vbyte::encode_vbyte},
    dict_sect_pfc::DictSectPFC,
};

use super::{builder::Options, vocab::HDT_DICTIONARY_TYPE_FOUR};

#[derive(Default, Debug)]
pub struct FourSectDictBuilder {
    so_id_map: HashMap<String, u32>,
    pred_id_map: HashMap<String, u32>,
    subject_id_map: HashMap<String, u32>,
    object_id_map: HashMap<String, u32>,

    pub shared_terms: BTreeSet<String>,
    pub subject_terms: BTreeSet<String>,
    pub object_terms: BTreeSet<String>,
    pub predicate_terms: BTreeSet<String>,

    pub size_strings: usize,
    options: Options,
}

#[derive(PartialEq)]
enum DictionaryRole {
    Subject,
    Predicate,
    Object,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EncodedTripleId {
    pub subject: u32,
    pub predicate: u32,
    pub object: u32,
}

impl FourSectDictBuilder {
    // fn number_of_elements(&self) -> usize {
    //     self.so_terms.len()
    //         + self.subject_terms.len()
    //         + self.predicate_terms.len()
    //         + self.object_terms.len()
    // }

    pub fn load(nt_file: &str, opts: Options) -> Result<(Self, Vec<EncodedTripleId>), Box<dyn Error>> {
        let source = match std::fs::File::open(nt_file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", nt_file, e);
                return Err(e.into());
            }
        };
        let source_reader = BufReader::new(source);
        // use Hashset on triples to remove duplicates
        let mut triples = HashSet::new();
        let quads = RdfParser::from_format(NTriples).for_reader(source_reader);
        let timer = std::time::Instant::now();

        // TODO: compare times with Vec followed by parallel sort vs times with BTreeSet
        let mut subject_terms = BTreeSet::new();
        let mut object_terms = BTreeSet::new();
        let mut dict = FourSectDictBuilder { options: opts, ..Default::default() };
        for q in quads {
            let q = q?; //propagate the error  

            subject_terms.insert(term_to_hdt_bgp_str(&q.subject.into())?);
            dict.predicate_terms.insert(term_to_hdt_bgp_str(&q.predicate.into())?);
            object_terms.insert(term_to_hdt_bgp_str(&q.object)?);
        }

        dict.shared_terms = subject_terms.intersection(&object_terms).cloned().collect();
        dict.subject_terms = subject_terms.difference(&dict.shared_terms).cloned().collect();
        dict.object_terms = object_terms.difference(&dict.shared_terms).cloned().collect();

        /*
        https://www.w3.org/submissions/2011/SUBM-HDT-20110330/#dictionaryEncoding
        Four subsets are mapped as follows (for a graph G with SG, PG, OG the different subjects, predicates and objects):
        1. Common subject-objects (SOG) with IDs from 1 to |SOG|
        2. Non common subjects (SG-SOG), mapped to [|SOG| +1, |SG|]
        3. Non common objects (OG-SOG), in [|SOG|+1, |OG|]
        4. Predicates, mapped to [1, |PG|].
         */

        // Shared subject-objects: 1..=|SOG|
        let mut shared_id = 1;
        for term in &dict.shared_terms {
            dict.so_id_map.insert(term.clone(), shared_id);
            shared_id += 1;
        }

        // TODO run these 3 dictionary builds in parallel?

        // Subject-only: |SOG|+1 ..= |SG|
        let mut id = shared_id;
        for term in &dict.subject_terms {
            dict.subject_id_map.insert(term.clone(), id);
            id += 1;
        }

        // Object-only: |SOG|+1 ..= |OG|
        let mut id = shared_id;
        for term in &dict.object_terms {
            dict.object_id_map.insert(term.clone(), id);
            id += 1;
        }

        // Predicates: 1..=|PG|
        for (i, term) in dict.predicate_terms.iter().enumerate() {
            dict.pred_id_map.insert(term.clone(), (i + 1) as u32);
        }
        debug!("Four Section Dictions sort time: {:?}", timer.elapsed());

        let source = match std::fs::File::open(nt_file) {
            Ok(f) => f,
            Err(e) => {
                error!("Error opening file {:?}: {:?}", nt_file, e);
                return Err(e.into());
            }
        };
        let triple_encoder_timer = std::time::Instant::now();
        let source_reader = BufReader::new(source);
        let quads = RdfParser::from_format(NTriples).for_reader(source_reader);
        for q in quads {
            let q = q?; //propagate the error  
            triples.insert(EncodedTripleId {
                subject: dict.term_to_id(&term_to_hdt_bgp_str(&q.subject.into())?, DictionaryRole::Subject),
                predicate: dict.term_to_id(&term_to_hdt_bgp_str(&q.predicate.into())?, DictionaryRole::Predicate),
                object: dict.term_to_id(&term_to_hdt_bgp_str(&q.object)?, DictionaryRole::Object),
            });
        }
        debug!("Encoding triples time: {:?}", triple_encoder_timer.elapsed());
        debug!("Dictionary build time: {:?}", timer.elapsed());
        // println!("triples: {:?}", triples);
        Ok((dict, triples.into_iter().collect()))
    }

    fn term_to_id(&self, term: &str, role: DictionaryRole) -> u32 {
        match role {
            DictionaryRole::Predicate => *self.pred_id_map.get(term).unwrap(),
            DictionaryRole::Subject => {
                if let Some(id) = self.so_id_map.get(term) {
                    *id
                } else {
                    *self.subject_id_map.get(term).unwrap()
                }
            }
            DictionaryRole::Object => {
                if let Some(id) = self.so_id_map.get(term) {
                    *id
                } else {
                    *self.object_id_map.get(term).unwrap()
                }
            }
        }
    }

    pub fn save(&self, dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        // libhdt/src/dictionary/FourSectionDictionary.cpp::save()
        let mut ci = containers::ControlInfo {
            control_type: ControlType::Dictionary,
            format: HDT_DICTIONARY_TYPE_FOUR.to_string(),
            ..Default::default()
        };
        ci.properties.insert("mappings".to_string(), "1".to_string());
        ci.properties.insert("sizeStrings".to_string(), self.size_strings.to_string());
        ci.save(dest_writer)?;
        //shared
        // let log_seq = LogSequence2::compress(&self.dict.shared_terms)?;
        // log_seq.save(&mut dest_writer)?;
        let pfc = compress(&self.shared_terms, self.options.block_size)?;
        pfc.save(dest_writer)?;
        //subjects
        // let log_seq: LogSequence2 = LogSequence2::compress(&self.dict.subject_terms)?;
        // log_seq.save(&mut dest_writer)?;
        let pfc = compress(&self.subject_terms, self.options.block_size)?;
        pfc.save(dest_writer)?;
        //predicates
        // let log_seq = LogSequence2::compress(&self.dict.predicate_terms)?;
        // log_seq.save(&mut dest_writer)?;
        let pfc = compress(&self.predicate_terms, self.options.block_size)?;
        pfc.save(dest_writer)?;
        //objects
        // let log_seq = LogSequence2::compress(&self.dict.object_terms)?;
        // log_seq.save(&mut dest_writer)?;
        let pfc = compress(&self.object_terms, self.options.block_size)?;
        pfc.save(dest_writer)?;
        Ok(())
    }
}

/// Convert triple string formats from OxRDF to HDT.
fn term_to_hdt_bgp_str(term: &Term) -> Result<String, Box<dyn Error>> {
    let hdt_str = match term {
        // hdt terms should not include < >'s from IRIs
        Term::NamedNode(named_node) => named_node.clone().into_string(),

        Term::Literal(literal) => literal.to_string(),

        Term::BlankNode(_s) => term.to_string(),
    };

    Ok(hdt_str)
}

pub fn compress(set: &BTreeSet<String>, block_size: usize) -> Result<DictSectPFC, Box<dyn Error>> {
    let mut terms: Vec<String> = set.iter().to_owned().cloned().collect();
    terms.sort(); // Ensure lexicographic order
    // println!("{:?}", terms);
    let mut compressed_terms = Vec::new();
    let mut offsets = Vec::new();
    let mut last_term = "";

    let num_terms = terms.len();
    for (i, term) in terms.iter().enumerate() {
        if i % block_size == 0 {
            offsets.push(compressed_terms.len() as u32);
            compressed_terms.extend_from_slice(term.as_bytes());
            // Every block stores a full term
        } else {
            let common_prefix_len = last_term.chars().zip(term.chars()).take_while(|(a, b)| a == b).count();
            compressed_terms.extend_from_slice(&encode_vbyte(common_prefix_len));
            compressed_terms.extend_from_slice(term[common_prefix_len..].as_bytes());
        };

        compressed_terms.push(0); // Null separator

        last_term = term;
    }
    offsets.push(compressed_terms.len() as u32);

    // offsets are an increasing list of array indices, therefore the last one will be the largest
    // potential off by 1 in comparison with hdt-cpp implementation
    let bits_per_entry = (offsets.last().unwrap().ilog2() + 1) as usize;

    Ok(DictSectPFC {
        num_strings: num_terms,
        block_size,
        sequence: Sequence {
            entries: offsets.len(),
            bits_per_entry,
            data: offsets.iter().map(|v| *v as usize).collect(),
            crc_handle: None,
        },
        packed_data: Arc::from(compressed_terms),
    })
}
