mod dict_sect_pfc;
mod four_sect_dict;

use crate::ControlInfo;
pub use dict_sect_pfc::DictSectPFC;
pub use four_sect_dict::FourSectDict;
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

        let mut preamble: [u8; 4] = [0; 4];
        reader.read_exact(&mut preamble)?;
        let preamble: u32 = u32::from_be_bytes(preamble);

        if preamble != 2 {
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
}
