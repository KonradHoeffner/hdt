use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub struct DictSectPFC {}

impl DictSectPFC {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        unimplemented!();
    }
}
