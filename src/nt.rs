use crate::{DictSectPFC, FourSectDict, IdKind};
use bitset_core::BitSet;
use lasso::{Key, Spur, ThreadedRodeo};
use oxttl::NTriplesParser;
use rayon::prelude::*;
use std::path::Path;
use std::sync::Arc;

//pub type Result<T> = core::result::Result<T, Error>;
pub type Result<T> = std::io::Result<T>;
/*
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO Error")]
    Io(#[from] std::io::Error),
}
*/
type Simd = [u64; 4];
type Indices = Vec<Simd>;

/// read N-Triples and convert them to a dictionary and triple IDs
pub fn read_nt(
    path: &std::path::Path, block_size: usize,
) -> Result<(FourSectDict, Vec<crate::triples::TripleId>)> {
    use log::info;

    // 1. Parse N-Triples and collect terms using string interning
    let timer = std::time::Instant::now();
    let (mut raw_triple_indices, subject_indices, object_indices, predicate_indices, string_pool) =
        parse_nt_terms(path)?;
    let parse_time = timer.elapsed();

    // Sort and deduplicate triples in parallel with dictionary building
    let sorter = std::thread::Builder::new().name("sorter".to_owned()).spawn(move || {
        raw_triple_indices.sort_unstable();
        raw_triple_indices.dedup();
        raw_triple_indices
    })?;

    // 2. Build dictionary from term indices
    let timer = std::time::Instant::now();
    let dict =
        build_dict_from_terms(&subject_indices, &object_indices, &predicate_indices, &string_pool, block_size);
    let dict_build_time = timer.elapsed();

    // 3. Encode triples to IDs using dictionary
    let timer = std::time::Instant::now();
    let sorted_triple_indices = sorter.join().unwrap();
    let encoded_triples = encode_triples(&dict, &sorted_triple_indices, &string_pool);
    info!("{parse_time:?},{dict_build_time:?},{:?}", timer.elapsed());

    Ok((dict, encoded_triples))
}

/// Parse N-Triples and collect terms into sets
pub fn parse_nt_terms(path: &Path) -> Result<(Vec<[usize; 3]>, Indices, Indices, Indices, Vec<String>)> {
    let lasso: Arc<ThreadedRodeo<Spur>> = Arc::new(ThreadedRodeo::new());
    // Store triple indices instead of strings
    let readers = NTriplesParser::new()
        .split_file_for_parallel_parsing(path, std::thread::available_parallelism()?.get())?;
    let raw_triple_indices: Vec<[usize; 3]> = readers
        .into_par_iter()
        .flat_map_iter(|reader| {
            //for q in reader {
            reader.map(|q| {
                let clean = |s: &mut String| {
                    let mut chars = s.chars();
                    if chars.nth(0) == Some('<') && chars.nth_back(0) == Some('>') {
                        s.remove(0);
                        s.pop();
                    }
                };
                let q = q.unwrap(); // TODO: error handling
                let mut subj_str = q.subject.to_string();
                clean(&mut subj_str);
                let mut pred_str = q.predicate.to_string();
                clean(&mut pred_str);
                let mut obj_str = q.object.to_string();
                clean(&mut obj_str);

                let s_idx = lasso.get_or_intern(subj_str).into_usize();
                let p_idx = lasso.get_or_intern(pred_str).into_usize();
                let o_idx = lasso.get_or_intern(obj_str).into_usize();

                [s_idx, p_idx, o_idx]
            })
        })
        .collect();
    // TODO: error handling
    //.map_err(|e| Error::Other(format!("Error reading N-Triples: {e:?}")))?;
    let lasso = Arc::try_unwrap(lasso).unwrap(); // no parallel usage anymore
    // Track which indices are subjects/objects/predicates
    let block = [0u64; 4];
    let blocks = lasso.len().div_ceil(256);
    let mut subject_indices = vec![block; blocks];
    let mut object_indices = vec![block; blocks];
    let mut predicate_indices = vec![block; blocks];

    for [s, p, o] in &raw_triple_indices {
        subject_indices.bit_set(*s);
        predicate_indices.bit_set(*p);
        object_indices.bit_set(*o);
    }

    let string_pool: Vec<String> = lasso.into_resolver().strings().map(String::from).collect();
    Ok((raw_triple_indices, subject_indices, object_indices, predicate_indices, string_pool))
}

/// Build dictionary from collected terms using string pool indices
pub fn build_dict_from_terms(
    subject_indices: &Indices, object_indices: &Indices, predicate_indices: &Indices, string_pool: &[String],
    block_size: usize,
) -> FourSectDict {
    use log::warn;
    use std::collections::BTreeSet;

    if predicate_indices.is_empty() {
        warn!("no triples found in provided RDF");
    }
    // can this be optimized? the bitvec lib does not seem to have an iterator for 1-bits
    let externalize = |idx: &Indices| {
        let mut v = BTreeSet::<&str>::new();
        for i in 0..idx.bit_len() {
            if idx.bit_test(i) {
                v.insert(&string_pool[i]);
            }
        }
        v
    };
    macro_rules! nspawn {
        ($s:expr, $n:expr, $f:expr) => {
            std::thread::Builder::new().name($n.to_owned()).spawn_scoped($s, $f).unwrap()
        };
    }
    let [shared, subjects, predicates, objects]: [DictSectPFC; 4] = std::thread::scope(|s| {
        [
            nspawn!(s, "shared", || {
                let mut shared_indices: Indices = subject_indices.clone();
                shared_indices.bit_and(object_indices); // intersection
                DictSectPFC::compress(&externalize(&shared_indices), block_size)
            }),
            nspawn!(s, "unique subjects", || {
                let mut unique_subject_indices: Indices = subject_indices.clone();
                unique_subject_indices.bit_andnot(object_indices);
                DictSectPFC::compress(&externalize(&unique_subject_indices), block_size)
            }),
            nspawn!(s, "predicates", || DictSectPFC::compress(&externalize(predicate_indices), block_size)),
            nspawn!(s, "unique objects", || {
                let mut unique_object_indices = object_indices.clone();
                unique_object_indices.bit_andnot(subject_indices);
                DictSectPFC::compress(&externalize(&unique_object_indices), block_size)
            }),
        ]
        .map(|t| t.join().unwrap())
    });
    FourSectDict { shared, subjects, predicates, objects }
}

/// Encode raw triples (as indices into string pool) to dictionary IDs
fn encode_triples(
    dict: &FourSectDict, raw_triple_indices: &[[usize; 3]], string_pool: &[String],
) -> Vec<crate::triples::TripleId> {
    use log::error;
    use rayon::prelude::*;

    raw_triple_indices
        .par_iter()
        .map(|[s_idx, p_idx, o_idx]| {
            let s = &string_pool[*s_idx as usize];
            let p = &string_pool[*p_idx as usize];
            let o = &string_pool[*o_idx as usize];
            let triple = [
                dict.string_to_id(s, IdKind::Subject),
                dict.string_to_id(p, IdKind::Predicate),
                dict.string_to_id(o, IdKind::Object),
            ];
            if triple[0] == 0 || triple[1] == 0 || triple[2] == 0 {
                error!("{triple:?} contains 0, part of ({s}, {p}, {o}) not found in the dictionary");
            }
            triple
        })
        .collect()
}
