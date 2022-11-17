mod dict_sect_pfc;
mod four_sect_dict;

use crate::triples::TripleId;
use crate::ControlInfo;
pub use dict_sect_pfc::DictSectPFC;
pub use four_sect_dict::FourSectDict;
use rayon::prelude::*;
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub enum DictSect {
    PFC(DictSectPFC),
}

impl DictSect {
    pub fn id_to_string(&self, id: usize) -> String {
        match self {
            DictSect::PFC(pfc_dict) => pfc_dict.id_to_string(id),
        }
    }

    pub fn string_to_id(&self, s: &str) -> usize {
        match self {
            DictSect::PFC(pfc_dict) => pfc_dict.locate(s),
        }
    }

    pub fn num_strings(&self) -> usize {
        match self {
            DictSect::PFC(pfc_dict) => pfc_dict.num_strings(),
        }
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        let mut preamble = [0_u8];
        reader.read_exact(&mut preamble)?;
        if preamble[0] != 2 {
            return Err(Error::new(
                InvalidData, "Implementation only supports plain front coded dictionary sections.",
            ));
        }

        Ok(DictSect::PFC(DictSectPFC::read(reader)?))
    }
}

#[derive(Debug, Clone)]
pub enum Dict {
    FourSectDict(FourSectDict),
}

impl Dict {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        let dict_ci = ControlInfo::read(reader)?;
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            return Err(Error::new(InvalidData, "Implementation only supports four section dictionaries"));
        }

        Ok(Dict::FourSectDict(FourSectDict::read(reader)?))
    }

    pub fn translate_all_ids(&self, triple_ids: &[TripleId]) -> Vec<(String, String, String)> {
        triple_ids
            .into_par_iter()
            .map(|id: &TripleId| {
                let subject = self.id_to_string(id.subject_id, IdKind::Subject);
                let predicate = self.id_to_string(id.predicate_id, IdKind::Predicate);
                let object = self.id_to_string(id.object_id, IdKind::Object);
                (subject, predicate, object)
            })
            .collect()
    }

    pub fn id_to_string(&self, id: usize, id_kind: IdKind) -> String {
        match self {
            Dict::FourSectDict(dict) => dict.id_to_string(id, id_kind),
        }
    }

    pub fn string_to_id(&self, s: &str, id_kind: IdKind) -> usize {
        match self {
            Dict::FourSectDict(dict) => dict.string_to_id(s, id_kind),
        }
    }
}

#[derive(Debug, Clone)]
pub enum IdKind {
    Subject,
    Predicate,
    Object,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControlInfo, Header};
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Read;

    #[test]
    fn read_dict() {
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();

        match Dict::read(&mut reader).unwrap() {
            Dict::FourSectDict(dict) => {
                assert_eq!(dict.shared.num_strings(), 43, "wrong number of strings in the shared section");
                assert_eq!(dict.subjects.num_strings(), 5, "wrong number of strings in the subject section");
                assert_eq!(dict.predicates.num_strings(), 23, "wrong number of strings in the predicates section");
                assert_eq!(dict.objects.num_strings(), 132, "wrong number of strings in the objects section");
                assert_eq!(dict.string_to_id("_:b1", IdKind::Subject), 1);
                assert_eq!("http://www.snik.eu/ontology/meta/uses", dict.id_to_string(43, IdKind::Subject));
                assert_eq!("http://www.snik.eu/ontology/meta/Chapter", dict.id_to_string(3, IdKind::Subject));
                assert_eq!("http://www.snik.eu/ontology/meta/DataSetType", dict.id_to_string(5, IdKind::Subject));
                for id in 1..dict.shared.num_strings() {
                    let s = dict.id_to_string(id, IdKind::Subject);
                    let back = dict.string_to_id(&s, IdKind::Subject);
                    assert_eq!(id, back, "shared id {} -> subject {} -> id {}", id, s, back);

                    let s = dict.id_to_string(id, IdKind::Object);
                    let back = dict.string_to_id(&s, IdKind::Object);
                    assert_eq!(id, back, "shared id {} -> object {} -> id {}", id, s, back);
                }
                for (sect, kind, name, offset) in [
                    (&dict.subjects, IdKind::Subject, "subject", dict.shared.num_strings()),
                    (&dict.objects, IdKind::Object, "object", dict.shared.num_strings()),
                    (&dict.predicates, IdKind::Predicate, "predicate", 0),
                ] {
                    for id in offset + 1..offset + sect.num_strings() {
                        let s = dict.id_to_string(id, kind.clone());
                        let back = dict.string_to_id(&s, kind.clone());
                        assert_eq!(id, back, "{} id {} -> {} {} -> id {}", name, id, name, s, back);
                    }
                }
            }
        };
    }
}
