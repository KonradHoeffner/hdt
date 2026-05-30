// //! *This module is available only if HDT is built with the experimental `"nt"` feature.*
use super::concurrent_interner::{Interner, Terms};
use crate::header::Header;
use crate::triples::{Id, TripleId, TriplesBitmap};
use crate::{DictSectPFC, FourSectDict, Hdt};
use bitset_core::BitSet;
use bytesize::ByteSize;
use log::{debug, error};
use oxttl::NTriplesParser;
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::thread;

pub type Result<T> = std::io::Result<T>;
type Simd = [u64; 4];
type Indices = Vec<Simd>;

impl Hdt {
    /// Converts RDF N-Triples to HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order.
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```no_run
    /// let path = std::path::Path::new("example.nt");
    /// let hdt = hdt::Hdt::read_nt(path).unwrap();
    /// ```
    pub fn read_nt(f: &Path) -> Result<Self> {
        const BLOCK_SIZE: usize = 16;

        let (dict, mut encoded_triples) = read_dict_triples(f, BLOCK_SIZE)?;
        let num_triples = encoded_triples.len();
        // Sort by final HDT ID (SPO order) before feeding into TriplesBitmap.
        encoded_triples.par_sort_unstable();
        let triples = TriplesBitmap::from_triples(&encoded_triples);

        let header = Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() };
        let mut hdt = Hdt { header, dict, triples };
        hdt.fill_header(f, BLOCK_SIZE, num_triples)?;

        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// Populate HDT header fields.
    /// Some fields may be optional, populating same triples as those in C++ version for now.
    fn fill_header(&mut self, path: &Path, block_size: usize, num_triples: usize) -> Result<()> {
        use crate::containers::rdf::Term::Literal as Lit;
        use crate::containers::rdf::{Id, Literal, Term, Triple};
        use crate::vocab::*;
        use std::io::Write;

        const ORDER: &str = "SPO";

        macro_rules! literal {
            ($s:expr, $p:expr, $o:expr) => {
                self.header.body.insert(Triple::new($s.clone(), $p.to_owned(), Lit(Literal::new($o.to_string()))));
            };
        }
        macro_rules! insert_id {
            ($s:expr, $p:expr, $o:expr) => {
                self.header.body.insert(Triple::new($s.clone(), $p.to_owned(), Term::Id($o.clone())));
            };
        }
        // as this is "just" metadata, we could also add a fallback if there ever is a valid use case, e.g. loading from stream instead of file
        let file_iri = format!("file://{}", path.canonicalize()?.display());
        let base = Id::Named(file_iri);

        literal!(base, RDF_TYPE, HDT_CONTAINER);
        literal!(base, RDF_TYPE, VOID_DATASET);
        literal!(base, VOID_TRIPLES, num_triples);
        literal!(base, VOID_PROPERTIES, self.dict.predicates.num_strings);
        let [d_s, d_o] =
            [&self.dict.subjects, &self.dict.objects].map(|s| s.num_strings + self.dict.shared.num_strings);
        literal!(base, VOID_DISTINCT_SUBJECTS, d_s);
        literal!(base, VOID_DISTINCT_OBJECTS, d_o);
        // // TODO: Add more VOID Properties. E.g. void:classes

        // // Structure
        let stats_id = Id::Blank("statistics".to_owned());
        let pub_id = Id::Blank("publicationInformation".to_owned());
        let format_id = Id::Blank("format".to_owned());
        let dict_id = Id::Blank("dictionary".to_owned());
        let triples_id = Id::Blank("triples".to_owned());
        insert_id!(base, HDT_STATISTICAL_INFORMATION, stats_id);
        insert_id!(base, HDT_STATISTICAL_INFORMATION, pub_id);
        insert_id!(base, HDT_FORMAT_INFORMATION, format_id);
        insert_id!(format_id, HDT_DICTIONARY, dict_id);
        insert_id!(format_id, HDT_TRIPLES, triples_id);
        // DICTIONARY
        literal!(dict_id, HDT_DICT_SHARED_SO, self.dict.shared.num_strings);
        literal!(dict_id, HDT_DICT_MAPPING, "1");
        literal!(dict_id, HDT_DICT_SIZE_STRINGS, ByteSize(self.dict.size_in_bytes() as u64));
        literal!(dict_id, HDT_DICT_BLOCK_SIZE, block_size);
        // TRIPLES
        literal!(triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP);
        literal!(triples_id, HDT_NUM_TRIPLES, num_triples);
        literal!(triples_id, HDT_TRIPLES_ORDER, ORDER);
        // // Sizes
        let meta = std::fs::File::open(path)?.metadata()?;
        literal!(stats_id, HDT_ORIGINAL_SIZE, meta.len());
        // a few bytes off because that literal itself is not counted
        literal!(stats_id, HDT_SIZE, ByteSize(self.size_in_bytes() as u64));
        // exclude for now to skip dependency on chrono
        //let datetime_str = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
        //literal!(pub_id,DC_TERMS_ISSUED,datetime_str);
        let mut buf = Vec::<u8>::new();
        for triple in &self.header.body {
            writeln!(buf, "{triple}")?;
        }
        self.header.length = buf.len();
        Ok(())
    }
}

/// Output of [`parse_nt_terms`]. All term strings live inside the `Interner`;
/// the triples hold `u32` term indices (4 bytes each) instead of full strings,
/// and the three bitsets track which indices appear as subject / predicate /
/// object.
struct ParsedTerms {
    triples: Vec<[u32; 3]>,
    interner: Interner,
    subjects: Indices,
    predicates: Indices,
    objects: Indices,
}

/// ID map: indexed by term index (`u32` as `usize`), holds the final HDT id for
/// a term in a given role (subject/predicate/object), or 0 if it has no id in
/// that role. u32 fits: HDT ids are at most `num_strings` ≤ u32::MAX.
type IdMap = Vec<u32>;

/// read N-Triples and convert them to a dictionary and triple IDs
fn read_dict_triples(path: &Path, block_size: usize) -> Result<(FourSectDict, Vec<TripleId>)> {
    // 1. Parse N-Triples, interning each unique term exactly once in the `Interner`.
    let ParsedTerms { triples, interner, subjects, predicates, objects } = parse_nt_terms(path)?;

    // 2. In parallel with dictionary build: sort + dedup triples (by term index
    //    — this removes exact duplicate triples; the final SPO-ID sort happens
    //    later, once we've assigned HDT ids).
    let sorter = thread::Builder::new().name("sorter".to_owned()).spawn(move || {
        let mut t = triples;
        t.par_sort_unstable();
        t.dedup();
        t
    })?;

    // 3. Assign HDT ids in sorted-string order and build the compressed dict.
    //    Returns three `index -> u32 id` lookup tables — direct array indexing
    //    during encoding, no more binary-search-through-PFC.
    let (dict, subj_map, pred_map, obj_map) = {
        // Consume the interner into an arena-backed, index-addressable view (no
        // per-term copy), then drop it at the end of this block so the term
        // bytes are freed before the encoding peak.
        let terms = interner.into_terms();
        build_dict_and_id_maps(&terms, &subjects, &predicates, &objects, block_size)
    };
    // Bitsets served their purpose; drop before the encoding peak.
    drop(subjects);
    drop(predicates);
    drop(objects);

    // 4. Drain the sorted index triples directly into HDT-id triples via the
    //    ID maps. `into_par_iter` consumes the Vec so the index triples are
    //    freed before `read_dict_triples` returns — only `Vec<TripleId>`
    //    survives into `TriplesBitmap::from_triples`.
    let sorted_triples = sorter.join().expect("NT sorter thread panicked");
    let encoded_triples: Vec<TripleId> = sorted_triples
        .into_par_iter()
        .map(|[s_idx, p_idx, o_idx]| {
            let s = subj_map[s_idx as usize] as Id;
            let p = pred_map[p_idx as usize] as Id;
            let o = obj_map[o_idx as usize] as Id;
            if s == 0 || p == 0 || o == 0 {
                error!("encoded triple [{s}, {p}, {o}] contains 0; term missing from dictionary");
            }
            [s, p, o]
        })
        .collect();

    drop(subj_map);
    drop(pred_map);
    drop(obj_map);

    Ok((dict, encoded_triples))
}

/// Parse N-Triples and collect terms into the interning pool + role bitsets.
fn parse_nt_terms(path: &Path) -> Result<ParsedTerms> {
    let interner: Arc<Interner> = Arc::new(Interner::new());
    // use two threads when available parallelism cannot be determined as going to a single thread is around 38% slower
    // 16 chosen as a sane upper limit
    let num_parsers = std::cmp::min(16, thread::available_parallelism().map_or(2, std::num::NonZero::get));
    // Store triple indices instead of strings
    let readers = NTriplesParser::new().split_file_for_parallel_parsing(path, num_parsers)?;
    let triples: Vec<[u32; 3]> = readers
        .into_par_iter()
        .flat_map_iter(|reader| {
            reader.map(|q| {
                let clean = |s: &mut String| {
                    let mut chars = s.chars();
                    if chars.next() == Some('<') && chars.nth_back(0) == Some('>') {
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

                let s = interner.get_or_intern(&subj_str);
                let p = interner.get_or_intern(&pred_str);
                let o = interner.get_or_intern(&obj_str);

                [s, p, o]
            })
        })
        .collect();

    let interner = Arc::try_unwrap(interner).expect("interner Arc still has outstanding references");

    // Role bitsets indexed by term index (0-based, dense).
    let block = [0u64; 4];
    let blocks = interner.len().div_ceil(256);
    let mut subjects: Indices = vec![block; blocks];
    let mut objects: Indices = vec![block; blocks];
    let mut predicates: Indices = vec![block; blocks];

    for [s, p, o] in &triples {
        subjects.bit_set(*s as usize);
        predicates.bit_set(*p as usize);
        objects.bit_set(*o as usize);
    }

    Ok(ParsedTerms { triples, interner, subjects, predicates, objects })
}

/// Enumerate the set-bit positions (term indices) of a bitset. Uses
/// `trailing_zeros` per word — far cheaper than iterating every bit and
/// calling `bit_test` (the old `externalize` pattern).
fn collect_set_indices(bitset: &Indices) -> Vec<u32> {
    // Estimate capacity from popcount to avoid Vec grow allocations.
    let popcount: usize = bitset.iter().flat_map(|block| block.iter()).map(|w| w.count_ones() as usize).sum();
    let mut out = Vec::with_capacity(popcount);
    for (block_idx, block) in bitset.iter().enumerate() {
        for (word_idx, &word) in block.iter().enumerate() {
            let base_bit = block_idx * 256 + word_idx * 64;
            let mut w = word;
            while w != 0 {
                let bit_offset = w.trailing_zeros() as usize;
                out.push(u32::try_from(base_bit + bit_offset).expect("term index overflow (>u32::MAX)"));
                w &= w - 1;
            }
        }
    }
    out
}

/// Build the four compressed dictionary sections and the three per-role
/// `index -> HDT id` lookup tables.
///
/// Sections follow the standard HDT MAPPING2 layout:
/// - shared: terms that appear as both subject and object (ids 1..=N_shared for both roles)
/// - unique subjects: subject-only terms (subject ids N_shared+1..=N_shared+N_subj)
/// - unique objects: object-only terms (object ids N_shared+1..=N_shared+N_obj)
/// - predicates: all predicate terms (ids 1..=N_pred)
fn build_dict_and_id_maps(
    terms: &Terms, subjects_bs: &Indices, predicates_bs: &Indices, objects_bs: &Indices, block_size: usize,
) -> (FourSectDict, IdMap, IdMap, IdMap) {
    use log::warn;

    if predicates_bs.is_empty() {
        warn!("no triples found in provided RDF");
    }

    // Compute section membership via bitset ops.
    let mut shared_bs = subjects_bs.clone();
    shared_bs.bit_and(objects_bs);
    let mut unique_subj_bs = subjects_bs.clone();
    unique_subj_bs.bit_andnot(objects_bs);
    let mut unique_obj_bs = objects_bs.clone();
    unique_obj_bs.bit_andnot(subjects_bs);

    // Collect the term indices in each section.
    let mut shared_keys = collect_set_indices(&shared_bs);
    let mut unique_subj_keys = collect_set_indices(&unique_subj_bs);
    let mut pred_keys = collect_set_indices(predicates_bs);
    let mut unique_obj_keys = collect_set_indices(&unique_obj_bs);
    drop(shared_bs);
    drop(unique_subj_bs);
    drop(unique_obj_bs);

    // Sort each section by the resolved string. Each `par_sort_unstable_by`
    // uses the rayon thread pool, so running the four sorts back-to-back lets
    // each one use every core; spawning them all in parallel would just fight
    // over the same workers.
    let cmp = |a: &u32, b: &u32| terms.cmp(*a, *b);
    shared_keys.par_sort_unstable_by(cmp);
    unique_subj_keys.par_sort_unstable_by(cmp);
    pred_keys.par_sort_unstable_by(cmp);
    unique_obj_keys.par_sort_unstable_by(cmp);

    // Allocate ID maps sized by the interner's term count (also the bit
    // length of the role bitsets).
    let map_len = terms.len();
    let mut subj_map: IdMap = vec![0u32; map_len];
    let mut pred_map: IdMap = vec![0u32; map_len];
    let mut obj_map: IdMap = vec![0u32; map_len];

    let n_shared = shared_keys.len();
    let shared_id_ceiling = u32::try_from(n_shared).expect("too many shared terms (>u32::MAX)");
    for (i, &key) in shared_keys.iter().enumerate() {
        let id = (i as u32) + 1; // ids are 1-indexed
        let slot = key as usize;
        subj_map[slot] = id;
        obj_map[slot] = id;
    }
    for (i, &key) in unique_subj_keys.iter().enumerate() {
        subj_map[key as usize] = shared_id_ceiling + (i as u32) + 1;
    }
    for (i, &key) in unique_obj_keys.iter().enumerate() {
        obj_map[key as usize] = shared_id_ceiling + (i as u32) + 1;
    }
    for (i, &key) in pred_keys.iter().enumerate() {
        pred_map[key as usize] = (i as u32) + 1;
    }

    // Compress the four sections concurrently. Each thread pulls its strings
    // straight from the term arena (no intermediate `Vec<&str>` or `BTreeSet`).
    let shared_ref = &shared_keys;
    let unique_subj_ref = &unique_subj_keys;
    let pred_ref = &pred_keys;
    let unique_obj_ref = &unique_obj_keys;
    let (shared, subjects, predicates, objects) = thread::scope(|s| {
        let h_shared = thread::Builder::new()
            .name("shared".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(shared_ref.iter().map(|&k| terms.get(k)), shared_ref.len(), block_size)
            })
            .unwrap();
        let h_subj = thread::Builder::new()
            .name("unique subjects".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(
                    unique_subj_ref.iter().map(|&k| terms.get(k)),
                    unique_subj_ref.len(),
                    block_size,
                )
            })
            .unwrap();
        let h_pred = thread::Builder::new()
            .name("predicates".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(pred_ref.iter().map(|&k| terms.get(k)), pred_ref.len(), block_size)
            })
            .unwrap();
        let h_obj = thread::Builder::new()
            .name("unique objects".into())
            .spawn_scoped(s, || {
                DictSectPFC::compress_iter(
                    unique_obj_ref.iter().map(|&k| terms.get(k)),
                    unique_obj_ref.len(),
                    block_size,
                )
            })
            .unwrap();
        (h_shared.join().unwrap(), h_subj.join().unwrap(), h_pred.join().unwrap(), h_obj.join().unwrap())
    });

    (FourSectDict { shared, subjects, predicates, objects }, subj_map, pred_map, obj_map)
}

#[cfg(test)]
pub mod tests {
    use super::super::StringTriple;
    use super::super::tests::snikmeta_check;
    use super::Hdt;
    use crate::hdt::tests::snikmeta;
    use crate::tests::init;
    use color_eyre::Result;
    use fs_err::File;
    use std::path::Path;

    #[test]
    fn read_nt() -> Result<()> {
        init();
        let path = Path::new("tests/resources/snikmeta.nt");
        if !path.exists() {
            log::info!("Creating test resource snikmeta.nt.");
            let mut writer = std::io::BufWriter::new(File::create(path)?);
            snikmeta()?.write_nt(&mut writer)?;
        }
        let snikmeta_nt = Hdt::read_nt(path)?;

        let snikmeta = snikmeta()?;
        let hdt_triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let nt_triples: Vec<StringTriple> = snikmeta_nt.triples_all().collect();

        assert_eq!(nt_triples, hdt_triples);
        assert_eq!(snikmeta.triples.bitmap_y.dict, snikmeta_nt.triples.bitmap_y.dict);
        snikmeta_check(&snikmeta_nt)?;
        let path = Path::new("tests/resources/empty.nt");
        let hdt_empty = Hdt::read_nt(path)?;
        let mut buf = Vec::<u8>::new();
        hdt_empty.write(&mut buf)?;
        Hdt::read(std::io::Cursor::new(buf))?;
        Ok(())
    }
}
