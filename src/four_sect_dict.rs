#![allow(missing_docs)] // temporariy while we figure out what should be public in the end
use crate::ControlInfo;
use crate::DictSectPFC;
/// Four section dictionary.
use crate::dict_sect_pfc::{DictSectReadError, ExtractError};
use crate::triples::Id;
//use eyre::{Result, WrapErr, eyre};
use std::io;
use std::io::{BufRead, Error, ErrorKind};
use std::thread::JoinHandle;
use thiserror::Error;

/// Position in an RDF triple.
#[derive(Debug, Clone)]
pub enum IdKind {
    /// IRI or blank node in the first position of a triple.
    Subject,
    /// IRI in the second position of a triple.
    Predicate,
    /// IRI, blank node or literal in the third position of a triple.
    Object,
}

/// Four section dictionary with plain front coding.
/// Dictionary with shared, subject, predicate and object sections.
/// Types specified as <http://purl.org/HDT/hdt#dictionaryFour>.
/// See <https://www.rdfhdt.org/hdt-internals/#dictionary>.
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
pub struct DictError {
    #[source]
    e: ExtractError,
    id: Id,
    id_kind: &'static IdKind,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
#[error("four sect dict section error in the {sect_kind:?} section")]
pub struct DictSectError {
    #[source]
    e: DictSectReadError,
    sect_kind: SectKind,
}

#[derive(Error, Debug)]
#[error("error reading four section dictionary")]
pub enum DictReadError {
    ControlInfo(#[from] crate::containers::ControlInfoReadError),
    DictSect(#[from] DictSectError),
    Other(String),
}

impl FourSectDict {
    /// Get the string value of a given ID of a given type.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn id_to_string(&self, id: Id, id_kind: &'static IdKind) -> Result<String, DictError> {
        use SectKind::*;
        let shared_size = self.shared.num_strings() as Id;
        let d = id.saturating_sub(shared_size);
        match id_kind {
            IdKind::Subject => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| DictError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.subjects.extract(d).map_err(|e| DictError { e, id, id_kind, sect_kind: Subject })
                }
            }
            IdKind::Predicate => {
                self.predicates.extract(id).map_err(|e| DictError { e, id, id_kind, sect_kind: Predicate })
            }
            IdKind::Object => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| DictError { e, id, id_kind, sect_kind: Shared })
                } else {
                    self.objects.extract(d).map_err(|e| DictError { e, id, id_kind, sect_kind: Object })
                }
            }
        }
    }

    /// Get the string value of an ID.
    /// String representation of URIs, literals and blank nodes is defined in <https://www.w3.org/Submission/2011/SUBM-HDT-20110330/#dictionaryEncoding>>..
    pub fn string_to_id(&self, s: &str, id_kind: &IdKind) -> Id {
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
    pub fn read<R: BufRead>(reader: &mut R) -> Result<UnvalidatedFourSectDict, DictReadError> {
        use SectKind::*;
        let dict_ci = ControlInfo::read(reader)?;
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            return Err(DictReadError::Other("Implementation only supports four section dictionaries".to_owned()));
        }
        let (shared, shared_crc) =
            DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind: Shared })?;
        let (subjects, subjects_crc) =
            DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind: Subject })?;
        let (predicates, predicates_crc) =
            DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind: Predicate })?;
        let (objects, objects_crc) =
            DictSectPFC::read(reader).map_err(|e| DictSectError { e, sect_kind: Object })?;

        Ok(UnvalidatedFourSectDict {
            four_sect_dict: FourSectDict { shared, subjects, predicates, objects },
            crc_handles: [shared_crc, subjects_crc, predicates_crc, objects_crc],
        })
    }

    /// write the whole Dictionary including control info and all sections
    pub fn write(&self, write: &mut impl std::io::Write) -> Result<(), DictReadError> {
        use SectKind::*;
        ControlInfo::four_sect_dict().write(write)?;
        self.shared.write(write).map_err(|e| DictSectError { e, sect_kind: Shared })?;
        self.shared.write(write).map_err(|e| DictSectError { e, sect_kind: Shared })?;
        //self.subjects.write(write).map_err(|e| DictSectError { e, sect_kind: Subject })?;
        //self.predicates.write(write).map_err(|e| DictSectError { e, sect_kind: Predicate })?;
        //self.objects.write(write).map_err(|e| DictSectError { e, sect_kind: Object })?;
        Ok(())
    }

    /*
    pub fn translate_all_ids(&self, triple_ids: &[TripleId]) -> Vec<(String, String, String)> {
        triple_ids
            .into_par_iter()
            .map(|id: &TripleId| {
                let subject = self.id_to_string(id.subject_id, IdKind::Subject).unwrap();
                let predicate = self.id_to_string(id.predicate_id, IdKind::Predicate).unwrap();
                let object = self.id_to_string(id.object_id, IdKind::Object).unwrap();
                (subject, predicate, object)
            })
            .collect()
    }
    */
    /// size in bytes of the in memory four section dictionary
    pub fn size_in_bytes(&self) -> usize {
        self.shared.size_in_bytes()
            + self.subjects.size_in_bytes()
            + self.predicates.size_in_bytes()
            + self.objects.size_in_bytes()
    }
}

/// A wrapper to ensure prevent using FourSectDict before its checksum have been validated
pub struct UnvalidatedFourSectDict {
    four_sect_dict: FourSectDict,
    crc_handles: [JoinHandle<bool>; 4],
}

impl UnvalidatedFourSectDict {
    /// Validates the checksums of all dictionary sections in parallel.
    /// Dict validation takes around 1200 ms on a single thread with an 1.5 GB HDT file on an i9-12900k.
    /// This function must NOT be called more than once.
    // TODO can this be simplified?
    pub fn validate(self) -> io::Result<FourSectDict> {
        let names = ["shared", "subject", "predicate", "object"];
        for (name, handle) in names.iter().zip(self.crc_handles) {
            if !handle.join().unwrap() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("CRC Error in {name} dictionary section."),
                ));
            }
        }
        Ok(self.four_sect_dict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::tests::init;
    use pretty_assertions::assert_eq;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_dict() -> color_eyre::Result<()> {
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
        assert_eq!(dict.string_to_id("_:b1", &IdKind::Subject), 1);
        assert_eq!("http://www.snik.eu/ontology/meta/uses", dict.id_to_string(43, &IdKind::Subject)?);
        assert_eq!("http://www.snik.eu/ontology/meta/Chapter", dict.id_to_string(3, &IdKind::Subject)?);
        assert_eq!("http://www.snik.eu/ontology/meta/DataSetType", dict.id_to_string(5, &IdKind::Subject)?);
        for id in 1..dict.shared.num_strings() {
            let s = dict.id_to_string(id, &IdKind::Subject)?;
            let back = dict.string_to_id(&s, &IdKind::Subject);
            assert_eq!(id, back, "shared id {} -> subject {} -> id {}", id, s, back);

            let s = dict.id_to_string(id, &IdKind::Object)?;
            let back = dict.string_to_id(&s, &IdKind::Object);
            assert_eq!(id, back, "shared id {} -> object {} -> id {}", id, s, back);
        }
        for (sect, kind, name, offset) in [
            (&dict.subjects, &IdKind::Subject, "subject", dict.shared.num_strings()),
            (&dict.objects, &IdKind::Object, "object", dict.shared.num_strings()),
            (&dict.predicates, &IdKind::Predicate, "predicate", 0),
        ] {
            for id in offset + 1..offset + sect.num_strings() {
                let s = dict.id_to_string(id, kind)?;
                let back = dict.string_to_id(&s, kind);
                assert_eq!(id, back, "{} id {} -> {} {} -> id {}", name, id, name, s, back);
            }
        }
        Ok(())
    }
}
