use crate::dict::DictSect;
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub struct FourSectDict {
    pub shared: DictSect,
    pub subjects: DictSect,
    pub predicates: DictSect,
    pub objects: DictSect,
}

impl FourSectDict {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        Ok(FourSectDict {
            shared: DictSect::read(reader)?,
            subjects: DictSect::read(reader)?,
            predicates: DictSect::read(reader)?,
            objects: DictSect::read(reader)?,
        })
    }
}
