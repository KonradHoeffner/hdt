use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub struct Dictionary {
    // TODO
}

impl Dictionary {
    pub fn new() -> Self {
        // TODO
        unimplemented!();
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        // TODO
        unimplemented!();
    }
}
