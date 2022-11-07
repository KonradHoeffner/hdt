use crate::containers::Bitmap;
use crate::containers::Sequence;

#[derive(Debug, Clone)]
pub struct AdjList {
    pub sequence: Sequence,
    pub bitmap: Bitmap,
}

impl AdjList {
    pub fn new(sequence: Sequence, bitmap: Bitmap) -> Self {
        AdjList { sequence, bitmap }
    }

    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.bitmap.at_last_sibling(word_index)
    }

    pub fn get_id(&self, word_index: usize) -> usize {
        self.sequence.get(word_index)
    }

    pub fn get_max(&self) -> usize {
        self.sequence.entries
    }

    pub fn find(&self, x: usize) -> usize {
        if (x <= 0) {
            return 0;
        }
        self.bitmap.dict.select1(x as u64).unwrap() as usize + 1
    }

    pub fn last(&self, x: usize) -> usize {
        self.bitmap.dict.select1(x as u64 + 1).unwrap() as usize
    }
}
