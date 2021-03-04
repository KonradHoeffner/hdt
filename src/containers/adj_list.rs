use crate::containers::Bitmap;
use crate::containers::Sequence;

#[derive(Debug, Clone)]
pub struct AdjList {
    sequence: Sequence,
    bitmap: Bitmap,
    subject_id: usize,
}

impl AdjList {
    pub fn new(sequence: Sequence, bitmap: Bitmap) -> Self {
        AdjList {
            sequence,
            bitmap,
            subject_id: 0,
        }
    }
}
