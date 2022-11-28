use crate::dict_sect_pfc::ExtractError;
use crate::ControlInfo;
use crate::DictSectPFC;
use std::io;
use std::io::BufRead;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum IdKind {
    Subject,
    Predicate,
    Object,
}

#[derive(Debug, Clone)]
pub struct FourSectDict {
    pub shared: DictSectPFC,
    pub subjects: DictSectPFC,
    pub predicates: DictSectPFC,
    pub objects: DictSectPFC,
}

#[derive(Debug)]
pub enum SectKind {
    Shared,
    Subject,
    Predicate,
    Object,
}

#[derive(Error, Debug)]
#[error("four sect dict error id_to_string({id},IdKind::{id_kind:?}) in the {sect_kind:?} section, caused by {e}")]
pub struct DictErr {
    #[source]
    e: ExtractError,
    id: usize,
    id_kind: &'static IdKind,
    sect_kind: SectKind,
}

impl FourSectDict {
    pub fn id_to_string(&self, id: usize, id_kind: &'static IdKind) -> Result<String, DictErr> {
        let shared_size = self.shared.num_strings();
        let d = id.saturating_sub(shared_size);
        match id_kind {
            IdKind::Subject => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| DictErr { e, id, id_kind, sect_kind: SectKind::Shared })
                } else {
                    self.subjects.extract(d).map_err(|e| DictErr { e, id, id_kind, sect_kind: SectKind::Subject })
                }
            }
            IdKind::Predicate => {
                self.predicates.extract(id).map_err(|e| DictErr { e, id, id_kind, sect_kind: SectKind::Predicate })
            }
            IdKind::Object => {
                if id <= shared_size {
                    self.shared.extract(id).map_err(|e| DictErr { e, id, id_kind, sect_kind: SectKind::Shared })
                } else {
                    self.objects.extract(d).map_err(|e| DictErr { e, id, id_kind, sect_kind: SectKind::Object })
                }
            }
        }
    }

    pub fn string_to_id(&self, s: &str, id_kind: &IdKind) -> u32 {
        let shared_size = self.shared.num_strings();
        match id_kind {
            IdKind::Subject => {
                let mut id = self.shared.string_to_id(s);
                if id == 0 {
                    id = self.subjects.string_to_id(s);
                    if id > 0 {
                        id += shared_size;
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
                        id += shared_size;
                    }
                }
                id
            }
        }
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        let dict_ci = ControlInfo::read(reader)?;
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            return Err(Error::new(InvalidData, "Implementation only supports four section dictionaries"));
        }

        Ok(FourSectDict {
            shared: DictSectPFC::read(reader)?,
            subjects: DictSectPFC::read(reader)?,
            predicates: DictSectPFC::read(reader)?,
            objects: DictSectPFC::read(reader)?,
        })
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
    pub fn size_in_bytes(&self) -> usize {
        self.shared.size_in_bytes()
            + self.subjects.size_in_bytes()
            + self.predicates.size_in_bytes()
            + self.objects.size_in_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::ControlInfo;
    use pretty_assertions::assert_eq;
    use std::fs::File;
    use std::io::BufReader;

    #[test]
    fn read_dict() {
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();

        let dict = FourSectDict::read(&mut reader).unwrap();
        assert_eq!(dict.shared.num_strings(), 43, "wrong number of strings in the shared section");
        assert_eq!(dict.subjects.num_strings(), 5, "wrong number of strings in the subject section");
        assert_eq!(dict.predicates.num_strings(), 23, "wrong number of strings in the predicates section");
        assert_eq!(dict.objects.num_strings(), 132, "wrong number of strings in the objects section");
        assert_eq!(dict.string_to_id("_:b1", &IdKind::Subject), 1);
        assert_eq!("http://www.snik.eu/ontology/meta/uses", dict.id_to_string(43, &IdKind::Subject).unwrap());
        assert_eq!("http://www.snik.eu/ontology/meta/Chapter", dict.id_to_string(3, &IdKind::Subject).unwrap());
        assert_eq!(
            "http://www.snik.eu/ontology/meta/DataSetType",
            dict.id_to_string(5, &IdKind::Subject).unwrap()
        );
        for id in 1..dict.shared.num_strings() {
            let s = dict.id_to_string(id, &IdKind::Subject).unwrap();
            let back = dict.string_to_id(&s, &IdKind::Subject);
            assert_eq!(id, back, "shared id {} -> subject {} -> id {}", id, s, back);

            let s = dict.id_to_string(id, &IdKind::Object).unwrap();
            let back = dict.string_to_id(&s, &IdKind::Object);
            assert_eq!(id, back, "shared id {} -> object {} -> id {}", id, s, back);
        }
        for (sect, kind, name, offset) in [
            (&dict.subjects, &IdKind::Subject, "subject", dict.shared.num_strings()),
            (&dict.objects, &IdKind::Object, "object", dict.shared.num_strings()),
            (&dict.predicates, &IdKind::Predicate, "predicate", 0),
        ] {
            for id in offset + 1..offset + sect.num_strings() {
                let s = dict.id_to_string(id, kind).unwrap();
                let back = dict.string_to_id(&s, kind);
                assert_eq!(id, back, "{} id {} -> {} {} -> id {}", name, id, name, s, back);
            }
        }
    }
}
