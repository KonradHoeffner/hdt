mod dict_sect_pfc;
mod four_sect_dict;

use crate::rdf::Triple;
use crate::triple_sect::TripleId;
use crate::ControlInfo;
pub use dict_sect_pfc::DictSectPFC;
pub use four_sect_dict::FourSectDict;
use std::collections::BTreeSet;
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub enum DictSect {
    PFC(DictSectPFC),
}

impl DictSect {
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

    pub fn translate_all_ids(&mut self, triple_ids: BTreeSet<TripleId>) -> BTreeSet<Triple> {
        BTreeSet::new()
    }
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
