// //! *This module is available only if HDT is built with the experimental `"nt"` feature.*
use crate::Hdt;
use crate::header::Header;
use crate::triples::{TripleId, TriplesBitmap};
use crate::{DictSectPFC, FourSectDict, IdKind};
use bitset_core::BitSet;
use bytesize::ByteSize;
use lasso::{Key, Spur, ThreadedRodeo};
use log::{debug, error, info};
use oxttl::NTriplesParser;
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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
        encoded_triples.sort_unstable();
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

struct IndexPool {
    triples: Vec<[usize; 3]>,
    subjects: Indices,
    objects: Indices,
    predicates: Indices,
    strings: Vec<String>,
}

/// read N-Triples and convert them to a dictionary and triple IDs
fn read_dict_triples(path: &Path, block_size: usize) -> Result<(FourSectDict, Vec<TripleId>)> {
    // 1. Parse N-Triples and collect terms using string interning
    let timer = Instant::now();
    let mut pool = parse_nt_terms(path)?;
    let parse_time = timer.elapsed();

    // Sort and deduplicate triples in parallel with dictionary building
    let mut triples = std::mem::take(&mut pool.triples); // not needed anymore
    let sorter = thread::Builder::new().name("sorter".to_owned()).spawn(move || {
        triples.sort_unstable();
        triples.dedup();
        triples
    })?;

    // 2. Build dictionary from term indices
    let timer = Instant::now();
    let dict = build_dict_from_terms(&pool, block_size);
    let dict_build_time = timer.elapsed();

    // 3. Encode triples to IDs using dictionary
    let timer = Instant::now();
    let sorted_triple_indices = sorter.join().unwrap();
    let refs: &[[usize; 3]] = &sorted_triple_indices;
    let encoded_triples: Vec<TripleId> = refs
        .par_iter()
        .map(|[s_idx, p_idx, o_idx]| {
            let s = &pool.strings[*s_idx as usize];
            let p = &pool.strings[*p_idx as usize];
            let o = &pool.strings[*o_idx as usize];
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
        .collect();

    info!("{parse_time:?},{dict_build_time:?},{:?}", timer.elapsed());

    Ok((dict, encoded_triples))
}

/// Parse N-Triples and collect terms into sets
//pub fn parse_nt_terms(path: &Path) -> Result<(Vec<[usize; 3]>, Indices, Indices, Indices, Vec<String>)> {
fn parse_nt_terms(path: &Path) -> Result<IndexPool> {
    let lasso: Arc<ThreadedRodeo<Spur>> = Arc::new(ThreadedRodeo::new());
    // Store triple indices instead of strings
    let readers =
        NTriplesParser::new().split_file_for_parallel_parsing(path, thread::available_parallelism()?.get())?;
    let triples: Vec<[usize; 3]> = readers
        .into_par_iter()
        .flat_map_iter(|reader| {
            //for q in reader {
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
    let mut subjects = vec![block; blocks];
    let mut objects = vec![block; blocks];
    let mut predicates = vec![block; blocks];

    for [s, p, o] in &triples {
        subjects.bit_set(*s);
        predicates.bit_set(*p);
        objects.bit_set(*o);
    }

    let strings: Vec<String> = lasso.into_resolver().strings().map(String::from).collect();
    Ok(IndexPool { triples, subjects, objects, predicates, strings })
}

/// Build dictionary from collected terms using string pool indices
fn build_dict_from_terms(pool: &IndexPool, block_size: usize) -> FourSectDict {
    use log::warn;
    use std::collections::BTreeSet;

    if pool.predicates.is_empty() {
        warn!("no triples found in provided RDF");
    }
    // can this be optimized? the bitvec lib does not seem to have an iterator for 1-bits
    let externalize = |idx: &Indices| {
        let mut v = BTreeSet::<&str>::new();
        #[allow(clippy::needless_range_loop)]
        for i in 0..idx.bit_len() {
            if idx.bit_test(i) {
                v.insert(&pool.strings[i]);
            }
        }
        v
    };
    macro_rules! nspawn {
        ($s:expr, $n:expr, $f:expr) => {
            thread::Builder::new().name($n.to_owned()).spawn_scoped($s, $f).unwrap()
        };
    }
    let [shared, subjects, predicates, objects]: [DictSectPFC; 4] = thread::scope(|s| {
        [
            nspawn!(s, "shared", || {
                let mut shared_indices: Indices = pool.subjects.clone();
                shared_indices.bit_and(&pool.objects); // intersection
                DictSectPFC::compress(&externalize(&shared_indices), block_size)
            }),
            nspawn!(s, "unique subjects", || {
                let mut unique_subject_indices: Indices = pool.subjects.clone();
                unique_subject_indices.bit_andnot(&pool.objects);
                DictSectPFC::compress(&externalize(&unique_subject_indices), block_size)
            }),
            nspawn!(s, "predicates", || DictSectPFC::compress(&externalize(&pool.predicates), block_size)),
            nspawn!(s, "unique objects", || {
                let mut unique_object_indices = pool.objects.clone();
                unique_object_indices.bit_andnot(&pool.subjects);
                DictSectPFC::compress(&externalize(&unique_object_indices), block_size)
            }),
        ]
        .map(|t| t.join().unwrap())
    });
    FourSectDict { shared, subjects, predicates, objects }
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
