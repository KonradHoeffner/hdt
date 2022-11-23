//! Adjacency list containing an integer sequence and a bitmap with rank and select support.
use crate::containers::Bitmap;
use crate::containers::Sequence;

#[derive(Debug, Clone)]
pub struct AdjList {
    pub sequence: Sequence,
    pub bitmap: Bitmap,
}

impl AdjList {
    pub const fn new(sequence: Sequence, bitmap: Bitmap) -> Self {
        AdjList { sequence, bitmap }
    }

    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.bitmap.size_in_bytes()
    }

    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.bitmap.at_last_sibling(word_index)
    }

    pub fn get_id(&self, word_index: usize) -> usize {
        self.sequence.get(word_index)
    }

    pub const fn len(&self) -> usize {
        self.sequence.entries
    }

    pub fn is_empty(&self) -> bool {
        self.sequence.data.is_empty()
    }

    pub fn find(&self, x: usize) -> usize {
        if x == 0 {
            return 0;
        }
        // hdt counts from 1
        //self.bitmap.dict.select1(x as u64).unwrap()  as usize +1
        // rsdict has nonzero value for 0, is that correct? adjust for that.
        self.bitmap.dict.select1(x as u64 - 1).unwrap() as usize + 1
    }

    pub fn last(&self, x: usize) -> usize {
        self.find(x + 1) - 1
    }
}
