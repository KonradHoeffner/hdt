mod dict_sect_pfc;
mod four_sect_dict;

use crate::containers::rdf::Triple;
use crate::triples::TripleId;
use crate::ControlInfo;
pub use dict_sect_pfc::DictSectPFC;
pub use four_sect_dict::FourSectDict;
use std::collections::BTreeSet;
use std::io;
use std::io::BufRead;

use rayon::prelude::*;

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
                InvalidData,
                "Implementation only supports plain front coded dictionary sections.",
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
            return Err(Error::new(
                InvalidData,
                "Implementation only supports four section dictionaries",
            ));
        }

        Ok(Dict::FourSectDict(FourSectDict::read(reader)?))
    }

    pub fn translate_all_ids(&self, triple_ids: Vec<TripleId>) -> Vec<(String, String, String)> {
        triple_ids
            .par_iter()
            .map(|id: &TripleId| {
                let subject = self.id_to_string(id.subject_id, IdKind::Subject);
                let predicate = self.id_to_string(id.predicate_id, IdKind::Predicate);
                let object = self.id_to_string(id.object_id, IdKind::Object);
                (subject, predicate, object)
            })
            .collect()
    }

    fn id_to_string(&self, id: usize, id_kind: IdKind) -> String {
        match self {
            Dict::FourSectDict(dict) => dict.id_to_string(id, id_kind),
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
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Read;

    #[test]
    fn read_dict() {
        let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();
        match Dict::read(&mut reader).unwrap() {
            Dict::FourSectDict(dict) => {
                match dict.shared {
                    DictSect::PFC(sect) => assert_eq!(sect.num_strings(), 23128),
                };

                match dict.subjects {
                    DictSect::PFC(sect) => assert_eq!(sect.num_strings(), 182),
                };

                match dict.predicates {
                    DictSect::PFC(sect) => assert_eq!(sect.num_strings(), 170),
                };

                match dict.objects {
                    DictSect::PFC(sect) => assert_eq!(sect.num_strings(), 53401),
                };
            }
        };
    }
}
