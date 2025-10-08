#![allow(missing_docs)]
// temporary while we figure out what should be public in the end
/// Four section dictionary.
use crate::dict_sect_pfc;
use crate::triples::Id;
use crate::{ControlInfo, DictSectPFC};
#[cfg(feature = "sophia")]
use std::collections::HashSet;
use std::io::BufRead;
use std::thread::JoinHandle;
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

/// Position in an RDF triple.
#[derive(Debug, Clone, Copy)]
pub enum IdKind {
    /// IRI or blank node in the first position of a triple.
    Subject,
    /// IRI in the second position of a triple.
    Predicate,
    /// IRI, blank node or literal in the third position of a triple.
    Object,
}

impl IdKind {
    pub const KINDS: [IdKind; 3] = [IdKind::Subject, IdKind::Predicate, IdKind::Object];
}

/// Four section dictionary with plain front coding.
/// Dictionary with shared, subject, predicate and object sections.
/// Types specified as <http://purl.org/HDT/hdt#dictionaryFour>.
/// See <https://www.rdfhdt.org/hdt-internals/#dictionary>.
#[cfg_attr(test, derive(PartialEq))]
#[derive(Debug)]
pub struct FourSectDict {
    /// The shared section contains URIs that occur both in subject and object position. Its IDs start at one.
    pub shared: DictSectPFC,
    /// URIs that only occur as subjects. Their IDs start at the last ID of the shared section + 1.
    pub subjects: DictSectPFC,
    /// The predicate section has its own separate numbering starting from 1.
    pub predicates: DictSectPFC,
    /// URIs and literals that only occur as objects . Their IDs start at the last ID of the shared section + 1.
    pub objects: DictSectPFC,
}

/// Designates one of the four sections.
#[derive(Debug)]
pub enum SectKind {
    /// section for terms that appear as both subject and object
    Shared,
    /// section for terms that only appear as subjects
    Subject,
    /// section for terms that only appear as predicates
    Predicate,
    /// sections for terms that only appear as objects
    Object,
}

/// Wraps an extraction error with additional information on which dictionary section it occurred in.
#[derive(Error, Debug)]
#[error("four sect dict error id_to_string({id},IdKind::{id_kind:?}) in the {sect_kind:?} section, caused by {e}")]
pub struct ExtractError {
    #[source]
    e: dict_sect_pfc::ExtractError,
    id: Id,
    id_kind: IdKind,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
#[error("four sect dict section error in the {sect_kind:?} section")]
pub struct DictSectError {
    #[source]
    e: dict_sect_pfc::Error,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to read FourSectDict control info")]
    ControlInfo(#[from] crate::containers::control_info::Error),
    #[error("failed to read FourSectDict section")]
    DictSect(#[from] DictSectError),
    #[error("failed to read FourSectDict: {0}")]
    Other(String),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct EncodedTripleId {
    pub subject: Id,
    pub predicate: Id,
    pub object: Id,
}

impl FourSectDict {
    /// Get the string value of a given ID of a given type.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn id_to_string(&self, id: Id, id_kind: IdKind) -> core::result::Result<String, ExtractError> {
        use SectKind::*;
        let shared_size = self.shared.num_strings() as Id;
        let d = id.saturating_sub(shared_size);
        match id_kind {
            IdKind::Subject => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.subjects.extract(d).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Subject })
                }
            }
            IdKind::Predicate => {
                self.predicates.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Predicate })
            }
            IdKind::Object => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.objects.extract(d).map_err(|e| ExtractError { e, id, id_kind, sect_kind: Object })
                }
            }
        }
    }

    /// Get the ID for a given string or 0 if not found.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn string_to_id(&self, s: &str, id_kind: IdKind) -> Id {
        let shared_size = self.shared.num_strings();
        match id_kind {
            IdKind::Subject => {
                let mut id = self.shared.string_to_id(s);
                if id == 0 {
                    id = self.subjects.string_to_id(s);
                    if id > 0 {
                        id += shared_size as Id;
                    }
                }
                id
            }
            IdKind::Predicate => self.predicates.string_to_id(s),
            IdKind::Object => {
                let mut id = self.shared.string_to_id(s);
                if id == 0 {
                    id = self.objects.string_to_id(s);
                    if id > 0 {
                        id += shared_size as Id;
                    }
                }
                id
            }
        }
    }

    /// read the whole dictionary section including control information
    pub fn read<R: BufRead>(reader: &mut R) -> Result<UnvalidatedFourSectDict> {
        use SectKind::*;
        let dict_ci = ControlInfo::read(reader)?;
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            return Err(Error::Other("Implementation only supports four section dictionaries".to_owned()));
        }
        let mut f = |sect_kind| DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind });
        Ok(UnvalidatedFourSectDict([f(Shared)?, f(Subject)?, f(Predicate)?, f(Object)?]))
    }

    /// Parse N-Triples and collect terms into sets
    /// *This function is available only if HDT is built with the `"sophia"` feature, included by default.*
    #[cfg(feature = "sophia")]
    pub fn parse_nt_terms<R: BufRead + Send>(
        r: &mut R,
    ) -> Result<(Vec<[usize; 3]>, HashSet<usize>, HashSet<usize>, HashSet<usize>, Vec<String>)> {
        use sophia::api::prelude::TripleSource;
        use sophia::turtle::parser::nt;
        use std::collections::HashMap;

        // String pool: each unique string stored once
        let mut string_pool = Vec::<String>::new();
        let mut string_to_idx = HashMap::<String, usize>::new();

        // Store triple indices instead of strings
        let mut raw_triple_indices = Vec::<[usize; 3]>::new();

        // Track which indices are subjects/objects/predicates
        let mut subject_indices = HashSet::<usize>::new();
        let mut object_indices = HashSet::<usize>::new();
        let mut predicate_indices = HashSet::<usize>::new();

        let (tx, rx) = std::sync::mpsc::channel();
        use std::thread;
        thread::scope(|s| {
            // move tx to drop it automatically, otherwise it will freeze
            s.spawn(move || {
                nt::parse_bufread(r).for_each_triple(|q| {
                    let clean = |s: &mut String| {
                        let mut chars = s.chars();
                        if chars.nth(0) == Some('<') && chars.nth_back(0) == Some('>') {
                            s.remove(0);
                            s.pop();
                        }
                    };
                    let mut subj_str = q.subject.to_string();
                    clean(&mut subj_str);
                    let mut pred_str = q.predicate.to_string();
                    clean(&mut pred_str);
                    let mut obj_str = q.object.to_string();
                    clean(&mut obj_str);
                    tx.send((subj_str, pred_str, obj_str)).unwrap();
                })
            });
            // todo: how to handle errors?
        });

        // Helper closure to intern a string
        let mut intern = |s: String| -> usize {
            if let Some(&idx) = string_to_idx.get(&s) {
                idx
            } else {
                let idx = string_pool.len();
                string_to_idx.insert(s.clone(), idx);
                string_pool.push(s);
                idx
            }
        };

        for (subj_str, pred_str, obj_str) in rx {
            let s_idx = intern(subj_str);
            let p_idx = intern(pred_str);
            let o_idx = intern(obj_str);

            subject_indices.insert(s_idx);
            predicate_indices.insert(p_idx);
            object_indices.insert(o_idx);

            raw_triple_indices.push([s_idx, p_idx, o_idx]);
        }

        Ok((raw_triple_indices, subject_indices, object_indices, predicate_indices, string_pool))
    }

    /// Build dictionary from collected terms using string pool indices
    /// *This function is available only if HDT is built with the `"sophia"` feature, included by default.*
    #[cfg(feature = "sophia")]
    pub fn build_dict_from_terms(
        subject_indices: &HashSet<usize>, object_indices: &HashSet<usize>, predicate_indices: &HashSet<usize>,
        string_pool: &[String], block_size: usize,
    ) -> Self {
        use log::warn;
        use std::collections::BTreeSet;

        if predicate_indices.is_empty() {
            warn!("no triples found in provided RDF");
        }

        let [shared, subjects, predicates, objects]: [DictSectPFC; 4] = std::thread::scope(|s| {
            [
                s.spawn(|| {
                    let shared_indices: BTreeSet<usize> =
                        subject_indices.intersection(object_indices).copied().collect();
                    DictSectPFC::compress(
                        &shared_indices.into_iter().map(|i| string_pool[i].as_str()).collect(),
                        block_size,
                    )
                }),
                s.spawn(|| {
                    let unique_subject_indices: BTreeSet<usize> =
                        subject_indices.difference(object_indices).copied().collect();
                    DictSectPFC::compress(
                        &unique_subject_indices.into_iter().map(|i| string_pool[i].as_str()).collect(),
                        block_size,
                    )
                }),
                s.spawn(|| {
                    DictSectPFC::compress(
                        &predicate_indices.into_iter().map(|&i| string_pool[i].as_str()).collect(),
                        block_size,
                    )
                }),
                s.spawn(|| {
                    let unique_object_indices: BTreeSet<usize> =
                        object_indices.difference(subject_indices).copied().collect();
                    DictSectPFC::compress(
                        &unique_object_indices.into_iter().map(|i| string_pool[i].as_str()).collect(),
                        block_size,
                    )
                }),
            ]
            .map(|t| t.join().unwrap())
        });
        FourSectDict { shared, subjects, predicates, objects }
    }

    /// Encode raw triples (as indices into string pool) to dictionary IDs
    /// *This function is available only if HDT is built with the `"sophia"` feature, included by default.*
    #[cfg(feature = "sophia")]
    pub fn encode_triples(
        &self, raw_triple_indices: &[[usize; 3]], string_pool: &[String],
    ) -> Vec<crate::triples::TripleId> {
        use log::error;
        use rayon::prelude::*;

        raw_triple_indices
            .par_iter()
            .map(|[s_idx, p_idx, o_idx]| {
                let s = &string_pool[*s_idx];
                let p = &string_pool[*p_idx];
                let o = &string_pool[*o_idx];
                let triple = [
                    self.string_to_id(s, IdKind::Subject),
                    self.string_to_id(p, IdKind::Predicate),
                    self.string_to_id(o, IdKind::Object),
                ];
                if triple[0] == 0 || triple[1] == 0 || triple[2] == 0 {
                    error!("{triple:?} contains 0, part of ({s}, {p}, {o}) not found in the dictionary");
                }
                triple
            })
            .collect()
    }

    /// read N-Triples and convert them to a dictionary and triple IDs
    /// *This function is available only if HDT is built with the `"sophia"` feature, included by default.*
    #[cfg(feature = "sophia")]
    pub fn read_nt<R: BufRead + Send>(
        r: &mut R, block_size: usize,
    ) -> Result<(Self, Vec<crate::triples::TripleId>)> {
        use log::info;

        // 1. Parse N-Triples and collect terms using string interning
        let timer = std::time::Instant::now();
        let (mut raw_triple_indices, subject_indices, object_indices, predicate_indices, string_pool) =
            Self::parse_nt_terms(r)?;
        let parse_time = timer.elapsed();

        // Sort and deduplicate triples in parallel with dictionary building
        let sorter = std::thread::Builder::new()
            .name("sorter".to_owned())
            .spawn(move || {
                raw_triple_indices.sort_unstable();
                raw_triple_indices.dedup();
                raw_triple_indices
            })
            .unwrap();

        // 2. Build dictionary from term indices
        let timer = std::time::Instant::now();
        let dict = Self::build_dict_from_terms(
            &subject_indices, &object_indices, &predicate_indices, &string_pool, block_size,
        );
        let dict_build_time = timer.elapsed();

        // 3. Encode triples to IDs using dictionary
        let timer = std::time::Instant::now();
        let sorted_triple_indices = sorter.join().unwrap();
        let encoded_triples = dict.encode_triples(&sorted_triple_indices, &string_pool);
        info!("{parse_time:?},{dict_build_time:?},{:?}", timer.elapsed());

        Ok((dict, encoded_triples))
    }

    /// write the whole Dictionary including control info and all sections
    pub fn write(&self, write: &mut impl std::io::Write) -> Result<()> {
        use SectKind::*;
        ControlInfo::four_sect_dict().write(write)?;
        self.shared.write(write).map_err(|e| DictSectError { e, sect_kind: Shared })?;
        self.subjects.write(write).map_err(|e| DictSectError { e, sect_kind: Subject })?;
        self.predicates.write(write).map_err(|e| DictSectError { e, sect_kind: Predicate })?;
        self.objects.write(write).map_err(|e| DictSectError { e, sect_kind: Object })?;
        Ok(())
    }

    /// size in bytes of the in memory four section dictionary
    pub fn size_in_bytes(&self) -> usize {
        self.shared.size_in_bytes()
            + self.subjects.size_in_bytes()
            + self.predicates.size_in_bytes()
            + self.objects.size_in_bytes()
    }
}

/// A wrapper to ensure prevent using FourSectDict before its checksums have been validated
pub struct UnvalidatedFourSectDict([JoinHandle<dict_sect_pfc::Result<DictSectPFC>>; 4]);

impl UnvalidatedFourSectDict {
    /// Validates the checksums of all dictionary sections in parallel.
    /// Dict validation takes around 1200 ms on a single thread with an 1.5 GB HDT file on an i9-12900k.
    pub fn validate(self) -> Result<FourSectDict> {
        use SectKind::*;
        let r: Vec<_> = [Shared, Subject, Predicate, Object]
            .into_iter()
            .zip(self.0)
            .map(|(sect_kind, handle)| handle.join().unwrap().map_err(|e| DictSectError { e, sect_kind }))
            .collect::<std::result::Result<Vec<DictSectPFC>, DictSectError>>()?;
        let [shared, subjects, predicates, objects]: [DictSectPFC; 4] = r.try_into().unwrap();
        Ok(FourSectDict { shared, subjects, predicates, objects })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::tests::init;
    use fs_err::File;
    use pretty_assertions::assert_eq;
    use std::io::BufReader;

    #[test]
    fn read_write_dict() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;

        let dict = FourSectDict::read(&mut reader)?.validate()?;
        assert_eq!(dict.shared.num_strings(), 43, "wrong number of strings in the shared section");
        assert_eq!(dict.subjects.num_strings(), 6, "wrong number of strings in the subject section");
        assert_eq!(dict.predicates.num_strings(), 23, "wrong number of strings in the predicates section");
        assert_eq!(dict.objects.num_strings(), 133, "wrong number of strings in the objects section");
        assert_eq!(dict.string_to_id("_:b1", IdKind::Subject), 1);
        assert_eq!("http://www.snik.eu/ontology/meta/uses", dict.id_to_string(43, IdKind::Subject)?);
        assert_eq!("http://www.snik.eu/ontology/meta/Chapter", dict.id_to_string(3, IdKind::Subject)?);
        assert_eq!("http://www.snik.eu/ontology/meta/DataSetType", dict.id_to_string(5, IdKind::Subject)?);
        for id in 1..dict.shared.num_strings() {
            let s = dict.id_to_string(id, IdKind::Subject)?;
            let back = dict.string_to_id(&s, IdKind::Subject);
            assert_eq!(id, back, "shared id {} -> subject {} -> id {}", id, s, back);

            let s = dict.id_to_string(id, IdKind::Object)?;
            let back = dict.string_to_id(&s, IdKind::Object);
            assert_eq!(id, back, "shared id {} -> object {} -> id {}", id, s, back);
        }
        for (sect, kind, name, offset) in [
            (&dict.subjects, IdKind::Subject, "subject", dict.shared.num_strings()),
            (&dict.objects, IdKind::Object, "object", dict.shared.num_strings()),
            (&dict.predicates, IdKind::Predicate, "predicate", 0),
        ] {
            for id in offset + 1..offset + sect.num_strings() {
                let s = dict.id_to_string(id, kind)?;
                let back = dict.string_to_id(&s, kind);
                assert_eq!(id, back, "{} id {} -> {} {} -> id {}", name, id, name, s, back);
            }
        }
        let mut buf = Vec::new();
        dict.write(&mut buf)?;
        let dict2 = FourSectDict::read(&mut std::io::Cursor::new(buf))?.validate()?;
        assert_eq!(dict, dict2);
        Ok(())
    }
}
