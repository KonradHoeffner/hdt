// //! *This module is available only if HDT is built with the experimental `"nt"` feature.*
use super::concurrent_interner::{Interner, Terms};
use crate::containers::ControlInfo;
use crate::containers::rdf::Id;
use crate::header::Header;
use crate::triples::{Id as HdtId, TripleId, TriplesBitmap, TriplesBitmapSource};
use crate::{DictSectPFC, FourSectDict, Hdt};
use bitset_core::BitSet;
use bytesize::ByteSize;
use log::{debug, error};
use oxrdf::{Term, vocab::xsd};
use oxttl::NTriplesParser;
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::io::{Error, ErrorKind::InvalidData};
use std::path::Path;
use std::sync::Arc;
use std::thread;

pub type Result<T> = std::io::Result<T>;
type Simd = [u64; 4];
type Indices = Vec<Simd>;

impl Hdt {
    /// Converts RDF N-Triples to HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order.
    /// Literal escapes are decoded, so the dictionary holds term values, not N-Triples syntax.
    /// All triples including nul chars (U+0000), whether written `\u0000`, `\U00000000` or as a raw byte, are dropped as the dictionary cannot store them.
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```
    /// let hdt = hdt::Hdt::read_nt("tests/resources/empty.nt").unwrap();
    /// ```
    pub fn read_nt(f: impl AsRef<Path>) -> Result<Self> {
        let f = f.as_ref();
        let base = Id::Named(format!("file://{}", f.canonicalize()?.display()));
        let original_size = std::fs::File::open(f)?.metadata()?.len();
        let pool = parse_nt_terms(f)?;
        Self::from_parsed_terms(pool, &base, Some(original_size))
    }

    /// Builds an HDT with a FourSectionDictionary with DictionarySectionPlainFrontCoding and SPO order
    /// from triples in memory, e.g. to write an existing RDF graph as HDT without going through a file.
    /// Terms are given in the HDT dictionary string format: IRIs without enclosing angle brackets,
    /// literals including quotes, e.g. `"example"@en` or `"123"^^<http://www.w3.org/2001/XMLSchema#integer>`,
    /// and blank nodes as `_:b1`. This is the same format that [`Hdt::triples_all`] returns.
    /// The base IRI denotes the dataset in the header.
    /// All triples including nul chars (U+0000), whether written `\u0000`, `\U00000000` or as a raw byte, are dropped as the dictionary cannot store them.
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```
    /// let triples = [["http://ex.org/s", "http://ex.org/p", "http://ex.org/o"]];
    /// let hdt = hdt::Hdt::from_triples(triples, "http://example.org/mydataset").unwrap();
    /// assert_eq!(hdt.triples_with_pattern(None, None, Some("http://ex.org/o")).count(), 1);
    /// ```
    pub fn from_triples<S: AsRef<str>>(triples: impl IntoIterator<Item = [S; 3]>, base_iri: &str) -> Result<Self> {
        Self::from_parsed_terms(intern_terms(triples), &Id::Named(base_iri.to_owned()), None)
    }

    /// Convert triples to an HDT file, writing them straight to `dest`
    /// without building the query indexes.
    ///
    /// Equivalent to [`Hdt::from_triples`] followed by [`Hdt::write`], but
    /// it never constructs the `op_index` or the `wavelet_y` that a
    /// queryable [`Hdt`] carries. Neither is part of the HDT file, so the
    /// output is identical except for the `hdt:hdtSize` header statistic,
    /// which reports the size of what was written rather than the size of
    /// an in-memory `Hdt` including its indexes.
    ///
    /// Use this when converting; use [`Hdt::from_triples`] when the result
    /// will be queried.
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    /// # Example
    /// ```
    /// let triples = [["http://example.org/s", "http://example.org/p", "\"o\"@en"]];
    /// let mut out = Vec::new();
    /// hdt::Hdt::write_triples(triples, "http://example.org/mydataset", &mut out).unwrap();
    /// assert!(out.starts_with(b"$HDT"));
    /// ```
    pub fn write_triples<S: AsRef<str>>(
        triples: impl IntoIterator<Item = [S; 3]>, base_iri: &str, dest: &mut impl std::io::Write,
    ) -> Result<()> {
        Self::write_parsed_terms(intern_terms(triples), &Id::Named(base_iri.to_owned()), None, dest)
    }

    /// Convert an N-Triples file to an HDT file without building the query
    /// indexes. The file counterpart of [`Hdt::write_triples`]; see there
    /// for what differs from [`Hdt::read_nt`] followed by [`Hdt::write`].
    /// *This function is available only if HDT is built with the experimental `"nt"` feature.*
    pub fn convert_nt(f: impl AsRef<Path>, dest: &mut impl std::io::Write) -> Result<()> {
        let f = f.as_ref();
        let base = Id::Named(format!("file://{}", f.canonicalize()?.display()));
        let original_size = std::fs::File::open(f)?.metadata()?.len();
        let pool = parse_nt_terms(f)?;
        Self::write_parsed_terms(pool, &base, Some(original_size), dest)
    }

    fn write_parsed_terms(
        pool: ParsedTerms, base: &Id, original_size: Option<u64>, dest: &mut impl std::io::Write,
    ) -> Result<()> {
        const BLOCK_SIZE: usize = 16;

        let (dict, mut encoded_triples) = dict_triples(pool, BLOCK_SIZE)?;
        let num_triples = encoded_triples.len();
        encoded_triples.par_sort_unstable();
        // Moved in so the ids are freed once the sequences are built, before serializing.
        let triples = TriplesBitmapSource::from_triples(encoded_triples);

        let hdt_size = dict.size_in_bytes() + triples.size_in_bytes();
        let header = build_header(&dict, base, BLOCK_SIZE, num_triples, original_size, hdt_size);
        debug!("HDT size {}, written without query indexes", ByteSize(hdt_size as u64));

        ControlInfo::global().write(dest).map_err(|e| std::io::Error::other(e.to_string()))?;
        header.write(dest).map_err(|e| std::io::Error::other(e.to_string()))?;
        dict.write(dest).map_err(|e| std::io::Error::other(e.to_string()))?;
        triples.write(dest).map_err(|e| std::io::Error::other(e.to_string()))?;
        dest.flush()?;
        Ok(())
    }

    fn from_parsed_terms(pool: ParsedTerms, base: &Id, original_size: Option<u64>) -> Result<Self> {
        const BLOCK_SIZE: usize = 16;

        let (dict, mut encoded_triples) = dict_triples(pool, BLOCK_SIZE)?;
        let num_triples = encoded_triples.len();
        // Sort by final HDT ID (SPO order) before feeding into TriplesBitmap.
        encoded_triples.par_sort_unstable();
        // Move (don't borrow) the encoded triples in so TriplesBitmap can free
        // them after its build loop, before its op-index peak.
        let triples = TriplesBitmap::from_triples(encoded_triples);

        let header = Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() };
        let mut hdt = Hdt { header, dict, triples };
        hdt.fill_header(base, BLOCK_SIZE, num_triples, original_size);

        debug!("HDT size in memory {}, details:", ByteSize(hdt.size_in_bytes() as u64));
        debug!("{hdt:#?}");
        Ok(hdt)
    }

    /// Populate HDT header fields.
    /// Some fields may be optional, populating same triples as those in C++ version for now.
    fn fill_header(&mut self, base: &Id, block_size: usize, num_triples: usize, original_size: Option<u64>) {
        let hdt_size = self.size_in_bytes();
        self.header = build_header(&self.dict, base, block_size, num_triples, original_size, hdt_size);
    }
}

/// Build the HDT header.
///
/// `hdt_size` is what `hdt:hdtSize` reports. It is passed in rather than
/// measured here because the write-only path
/// ([`Hdt::write_triples`]) never materializes the query indexes that
/// [`Hdt::size_in_bytes`] counts.
fn build_header(
    dict: &FourSectDict, base: &Id, block_size: usize, num_triples: usize, original_size: Option<u64>,
    hdt_size: usize,
) -> Header {
    let mut header = Header { format: "ntriples".to_owned(), length: 0, body: BTreeSet::new() };
    {
        use crate::containers::rdf::Term::Literal as Lit;
        use crate::containers::rdf::{Literal, Term, Triple};
        use crate::vocab::*;

        const ORDER: &str = "SPO";

        macro_rules! literal {
            ($s:expr, $p:expr, $o:expr) => {
                header.body.insert(Triple::new($s.clone(), $p.to_owned(), Lit(Literal::new($o.to_string()))));
            };
        }
        macro_rules! insert_id {
            ($s:expr, $p:expr, $o:expr) => {
                header.body.insert(Triple::new($s.clone(), $p.to_owned(), Term::Id($o.clone())));
            };
        }
        literal!(base, RDF_TYPE, HDT_CONTAINER);
        literal!(base, RDF_TYPE, VOID_DATASET);
        literal!(base, VOID_TRIPLES, num_triples);
        literal!(base, VOID_PROPERTIES, dict.predicates.num_strings);
        let [d_s, d_o] = [&dict.subjects, &dict.objects].map(|s| s.num_strings + dict.shared.num_strings);
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
        literal!(dict_id, HDT_DICT_SHARED_SO, dict.shared.num_strings);
        literal!(dict_id, HDT_DICT_MAPPING, "1");
        literal!(dict_id, HDT_DICT_SIZE_STRINGS, ByteSize(dict.size_in_bytes() as u64));
        literal!(dict_id, HDT_DICT_BLOCK_SIZE, block_size);
        // TRIPLES
        literal!(triples_id, DC_TERMS_FORMAT, HDT_TYPE_BITMAP);
        literal!(triples_id, HDT_NUM_TRIPLES, num_triples);
        literal!(triples_id, HDT_TRIPLES_ORDER, ORDER);
        // // Sizes
        if let Some(size) = original_size {
            literal!(stats_id, HDT_ORIGINAL_SIZE, size);
        }
        // a few bytes off because that literal itself is not counted
        literal!(stats_id, HDT_SIZE, ByteSize(hdt_size as u64));
        // exclude for now to skip dependency on chrono
        //let datetime_str = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%z").to_string();
        //literal!(pub_id,DC_TERMS_ISSUED,datetime_str);
    }
    header
}

/// Output of [`parse_nt_terms`] (file path) and [`intern_terms`] (in-memory).
/// All term strings live inside the `Interner`; the triples hold `u32` term
/// indices (4 bytes each) instead of full strings, and the three bitsets track
/// which indices appear as subject / predicate / object.
struct ParsedTerms {
    triples: Vec<[u32; 3]>,
    interner: Interner,
    subjects: Indices,
    predicates: Indices,
    objects: Indices,
}

impl ParsedTerms {
    /// Derive the role bitsets (subject / predicate / object) from the interned
    /// triples. Indices are 0-based and dense, sized by the interner's term count.
    fn new(interner: Interner, triples: Vec<[u32; 3]>) -> Self {
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

        ParsedTerms { triples, interner, subjects, predicates, objects }
    }
}

/// ID map: indexed by term index (`u32` as `usize`), holds the final HDT id for
/// a term in a given role (subject/predicate/object), or 0 if it has no id in
/// that role. u32 fits: HDT ids are at most `num_strings` ≤ u32::MAX.
type IdMap = Vec<u32>;

fn is_valid_triple(s: &str, p: &str, o: &str) -> bool {
    if s.contains('\0') || p.contains('\0') || o.contains('\0') {
        log::warn!("invalid triple ({s},{p},{o}) with unsupported NUL characters");
        return false;
    }
    true
}

/// Intern in-memory string triples into a [`ParsedTerms`]. Single-threaded — the
/// input is one sequential iterator, so there is no parser-level parallelism to
/// exploit here (dictionary compression below still runs on four threads).
fn intern_terms<S: AsRef<str>>(triples: impl IntoIterator<Item = [S; 3]>) -> ParsedTerms {
    let interner = Interner::new();
    let triples: Vec<[u32; 3]> = triples
        .into_iter()
        .filter(|t| is_valid_triple(t[0].as_ref(), t[1].as_ref(), t[2].as_ref()))
        .map(|t| t.map(|term| interner.get_or_intern(term.as_ref())))
        .collect();
    ParsedTerms::new(interner, triples)
}

/// Convert a parsed/interned term pool to a dictionary and encoded triple IDs.
fn dict_triples(pool: ParsedTerms, block_size: usize) -> Result<(FourSectDict, Vec<TripleId>)> {
    let ParsedTerms { triples, interner, subjects, predicates, objects } = pool;

    // In parallel with dictionary build: sort + dedup triples (by term index
    // — this removes exact duplicate triples; the final SPO-ID sort happens
    // later, once we've assigned HDT ids).
    let sorter = thread::Builder::new().name("sorter".to_owned()).spawn(move || {
        let mut t = triples;
        t.par_sort_unstable();
        t.dedup();
        t
    })?;

    // Assign HDT ids in sorted-string order and build the compressed dict.
    // Returns three `index -> u32 id` lookup tables — direct array indexing
    // during encoding, no more binary-search-through-PFC.
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

    // Drain the sorted index triples directly into HDT-id triples via the ID
    // maps. `into_par_iter` consumes the Vec so the index triples are freed
    // before this function returns — only `Vec<TripleId>` survives into
    // `TriplesBitmap::from_triples`.
    let sorted_triples = sorter.join().expect("NT sorter thread panicked");
    let encoded_triples: Vec<TripleId> = sorted_triples
        .into_par_iter()
        .map(|[s_idx, p_idx, o_idx]| {
            let s = subj_map[s_idx as usize] as HdtId;
            let p = pred_map[p_idx as usize] as HdtId;
            let o = obj_map[o_idx as usize] as HdtId;
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

/// Render a parsed term in HDT dictionary form: an IRI unbracketed, a blank
/// node as `_:id`, a literal quoted with its language tag or datatype.
///
/// A literal's body is its *decoded* value. N-Triples escapes are syntax,
/// not part of the value: `"tab\there"` in a file denotes a string holding
/// one tab, and that tab is what belongs in the dictionary. Rendering the
/// term with `to_string()` instead put the escape back in, so serializing
/// the HDT escaped it a second time and a tab came back out as a backslash
/// (#131).
///
/// The HDT format itself never pins this down, but decoded values are the
/// de facto convention: hdt-cpp (through serd) and hdt-java (through Jena)
/// both store them and escape only on serialization, as do
/// [`Hdt::from_triples`] and the Sophia adapter in this crate.
fn dict_string(term: &Term) -> String {
    match term {
        Term::NamedNode(n) => n.as_str().to_owned(),
        Term::BlankNode(b) => format!("_:{}", b.as_str()),
        Term::Literal(l) => {
            let value = l.value();
            match l.language() {
                Some(language) => format!("\"{value}\"@{language}"),
                // A simple literal is an xsd:string literal; both spell the
                // same term and the bare form is canonical, which is what
                // this path has always written.
                None if l.datatype() == xsd::STRING => format!("\"{value}\""),
                None => format!("\"{value}\"^^<{}>", l.datatype().as_str()),
            }
        }
        // RDF-star quoted triples: oxrdf only builds these with its rdf-12
        // feature, which is not enabled here.
        #[allow(unreachable_patterns)]
        other => other.to_string(),
    }
}

/// Parse N-Triples in parallel and collect terms into the interning pool + role bitsets.
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
            reader.filter_map(|q| {
                let q = match q.map_err(|e| Error::new(InvalidData, format!("Error reading N-Triples: {e}"))) {
                    Ok(q) => q,
                    Err(e) => return Some(Err(e)),
                };
                let [ss, ps, os] =
                    [&dict_string(&q.subject.into()), q.predicate.as_str(), &dict_string(&q.object)];
                if !is_valid_triple(ss, ps, os) {
                    return None;
                }
                let triple = [interner.get_or_intern(ss), interner.get_or_intern(ps), interner.get_or_intern(os)];
                Some(Ok(triple))
            })
        })
        .collect::<Result<Vec<[u32; 3]>>>()?;

    let interner = Arc::try_unwrap(interner).expect("interner Arc still has outstanding references");
    Ok(ParsedTerms::new(interner, triples))
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
    use pretty_assertions::assert_eq;
    use std::io::{Cursor, Write};
    use std::path::Path;
    use std::sync::Arc;

    /// Regression test for #131. Escapes are N-Triples syntax, not part of a term's value,
    /// so the dictionary must hold the decoded value and each serialization must escape it exactly once.
    #[test]
    fn read_nt_escapes() -> Result<()> {
        init();
        // tests/resources/escapes.nt in dictionary string format
        let want: Vec<StringTriple> = [
            ["_:b0", "urn:x:bnode", "\"x\""],
            ["urn:x:s", "urn:x:backslash", "\"back\\slash\"^^<urn:x:dt>"],
            ["urn:x:s", "urn:x:cr", "\"carriage\rreturn\""],
            ["urn:x:s", "urn:x:iri", "urn:x:oé"],
            ["urn:x:s", "urn:x:newline", "\"line\nbreak\"@en"],
            ["urn:x:s", "urn:x:nul1", "\"firsta\""],
            ["urn:x:s", "urn:x:nul4", "\"aa\""],
            ["urn:x:s", "urn:x:nul4", "\"ab\""],
            ["urn:x:s", "urn:x:plain", "\"nothing to escape\""],
            ["urn:x:s", "urn:x:quote", "\"say \"hi\"\""],
            ["urn:x:s", "urn:x:raw", "\"café 😀\""],
            ["urn:x:s", "urn:x:tab", "\"tab\there\""],
            ["urn:x:s", "urn:x:unicode", "\"café 😀\""],
        ]
        .map(|t| t.map(Arc::from))
        .into();

        let from_nt = Hdt::read_nt("tests/resources/escapes.nt")?;
        assert_eq!(from_nt.triples_all().collect::<Vec<_>>(), want, "dictionary must hold decoded values");

        // the same graph given as decoded strings must build the same HDT
        let mut wantmore = want.clone();
        wantmore.push(["urn:x:s", "urn:x:nul4", "\"a\u{0000}\"bc"].map(Arc::from));
        let from_triples = Hdt::from_triples(wantmore, "urn:x:escapes")?;
        assert_eq!(from_triples.triple_ids_with_pattern(Some("urn:x:s"), None, Some("\"aa\"")).count(), 1);
        assert_eq!(from_triples.triples_all().collect::<Vec<_>>(), want);
        assert_eq!(from_triples.triples.bitmap_y.dict, from_nt.triples.bitmap_y.dict);

        // the reported symptom: out to N-Triples and back in must not change a value
        fs_err::create_dir_all("tests/resources/generated")?;
        let path = Path::new("tests/resources/generated/escapes.nt");
        let mut writer = std::io::BufWriter::new(File::create(path)?);
        from_nt.write_nt(&mut writer)?;
        writer.flush()?;
        assert_eq!(Hdt::read_nt(path)?.triples_all().collect::<Vec<_>>(), want, "NT must escape once");

        let mut buf = Vec::<u8>::new();
        from_nt.write(&mut buf)?;
        let again = Hdt::read(Cursor::new(buf))?.triples_all().collect::<Vec<_>>();
        assert_eq!(again, want, "HDT must preserve values");
        Ok(())
    }

    /// Offsets of the four `$HDT` control blocks: global, header,
    /// dictionary, triples.
    fn control_block_offsets(buf: &[u8]) -> Vec<usize> {
        (0..buf.len().saturating_sub(4)).filter(|&i| &buf[i..i + 4] == b"$HDT").collect()
    }

    /// `write_triples` must produce the same HDT as `from_triples` + `write`.
    /// The query indexes it skips are not part of the file, so the only
    /// permitted difference is the `hdt:hdtSize` header statistic, which
    /// counts them in the full path.
    #[test]
    fn write_triples_matches_from_triples_apart_from_the_size_statistic() -> Result<()> {
        init();
        let base = "http://www.snik.eu/ontology/meta";
        let triples: Vec<StringTriple> = snikmeta()?.triples_all().collect();

        let mut full = Vec::new();
        Hdt::from_triples(triples.clone(), base)?.write(&mut full)?;
        let mut lean = Vec::new();
        Hdt::write_triples(triples.clone(), base, &mut lean)?;

        // Same graph.
        let from_lean: Vec<StringTriple> = Hdt::read(std::io::Cursor::new(lean.clone()))?.triples_all().collect();
        assert_eq!(from_lean, triples, "write_triples lost or changed a triple");
        snikmeta_check(&Hdt::read(std::io::Cursor::new(lean.clone()))?)?;

        // Dictionary and triples sections byte-identical.
        let (of, ol) = (control_block_offsets(&full), control_block_offsets(&lean));
        assert!(of.len() >= 4 && ol.len() >= 4, "expected four control blocks");
        assert_eq!(&full[of[2]..], &lean[ol[2]..], "dictionary and triples sections must be identical");

        // The headers differ only in the hdtSize literal.
        let head_of = |b: &[u8], o: &[usize]| String::from_utf8_lossy(&b[o[1]..o[2]]).to_string();
        let (hf, hl) = (head_of(&full, &of), head_of(&lean, &ol));
        let strip = |h: &str| h.lines().filter(|l| !l.contains("hdtSize")).collect::<Vec<_>>().join("\n");
        assert_eq!(strip(&hf), strip(&hl), "headers differ beyond hdtSize");
        assert!(hf.contains("hdtSize") && hl.contains("hdtSize"));
        Ok(())
    }

    #[test]
    fn read_nt() -> Result<()> {
        init();
        let path = Path::new("tests/resources/snikmeta.nt");
        if !path.exists() {
            log::info!("Creating test resource snikmeta.nt.");
            let mut writer = std::io::BufWriter::new(File::create(path)?);
            snikmeta()?.write_nt(&mut writer)?;
        }
        let invalid = path.join("doesnotexist");
        assert!(Hdt::read_nt(invalid).is_err(), "invalid N-Triples path should result in error");

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
        Hdt::read(Cursor::new(buf))?;
        Ok(())
    }

    #[test]
    fn from_triples() -> Result<()> {
        init();
        let snikmeta = snikmeta()?;
        let triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let from_triples = Hdt::from_triples(triples, "http://www.snik.eu/ontology/meta")?;

        let hdt_triples: Vec<StringTriple> = snikmeta.triples_all().collect();
        let mem_triples: Vec<StringTriple> = from_triples.triples_all().collect();
        assert_eq!(mem_triples, hdt_triples);
        assert_eq!(snikmeta.triples.bitmap_y.dict, from_triples.triples.bitmap_y.dict);
        snikmeta_check(&from_triples)?;
        let mut buf = Vec::<u8>::new();
        from_triples.write(&mut buf)?;
        snikmeta_check(&Hdt::read(Cursor::new(buf))?)?;
        let hdt_empty = Hdt::from_triples(std::iter::empty::<[&str; 3]>(), "http://example.org/empty")?;
        let mut buf = Vec::<u8>::new();
        hdt_empty.write(&mut buf)?;
        Hdt::read(Cursor::new(buf))?;
        Ok(())
    }
}
