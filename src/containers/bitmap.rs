use crate::containers::vbyte::read_vbyte;
use crc_any::{CRCu32, CRCu8};
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::mem::size_of;

const USIZE_BITS: usize = usize::BITS as usize;
const BLOCKS_PER_SUPER: usize = 256 / USIZE_BITS;

#[derive(Debug, Clone)]
pub struct Bitmap {
    num_bits: usize,
    num_ones: usize,
    data: Vec<usize>,
    /// superblock counters
    superblocks: Vec<usize>,
    /// block counters
    blocks: Vec<u8>,
}

// could we achieve better results with using broadwords? see Broadword Implementation of Rank/Select Queries
// https://link.springer.com/chapter/10.1007/978-3-540-68552-4_12

impl Bitmap {
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        // Each block in the bitmap has `USIZE_BITS` many bits. If `usize` is 64 bits, and we are
        // looking for the 65th word in the sequence this means we need the first bit of the second
        // `usize` in `self.data`.

        // We can get the right usize `block` by dividing by the amount of bits in the usize.
        let block_index = word_index / USIZE_BITS;

        // We need to determine the value of the bit at a given `bit_index`
        let bit_index = word_index % USIZE_BITS;
        let bit_flag = 1_usize << bit_index;

        // If the `bit_flag` is set to one, the bitwise and will be equal to the `bit_flag`.
        self.data[block_index] & bit_flag == bit_flag
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind::{InvalidData, Other};

        let mut history: Vec<u8> = Vec::new();

        // read the type
        let mut bitmap_type = [0u8];
        reader.read_exact(&mut bitmap_type)?;
        history.extend_from_slice(&bitmap_type);
        if bitmap_type[0] != 1 {
            return Err(Error::new(InvalidData, "no support this type of bitmap"));
        }

        // read the number of bits
        let (num_bits, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);

        // read section CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate section CRC8
        let mut crc = CRCu8::crc8();
        crc.digest(&history[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC8-CCIT checksum"));
        }

        // reset history for CRC32
        history = Vec::new();
        let mut data: Vec<usize> = Vec::new();

        // read all but the last word, last word is byte aligned
        let full_byte_amount = ((num_bits - 1) >> 6) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        reader.read_exact(&mut full_words)?;
        history.extend_from_slice(&full_words);

        // turn the raw bytes into usize/u64 values
        for word in full_words.chunks_exact(size_of::<usize>()) {
            if let Ok(word_data) = <[u8; 8]>::try_from(word) {
                data.push(usize::from_le_bytes(word_data));
            } else {
                return Err(Error::new(Other, "failed to read usize"));
            }
        }

        let mut bits_read = 0;
        let mut last_value: usize = 0;
        let last_word_bits = if num_bits == 0 { 0 } else { ((num_bits - 1) % 64) + 1 };

        while bits_read < last_word_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            history.extend_from_slice(&buffer);
            last_value |= (buffer[0] as usize) << bits_read;
            bits_read += 8;
        }
        data.push(last_value);

        // read entry body CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let crc_code = u32::from_le_bytes(crc_code);

        // validate entry body CRC32
        let mut crc = CRCu32::crc32c();
        crc.digest(&history[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC32C checksum"));
        }

        // build index ***************************************************************
        // translated from https://github.com/rdfhdt/hdt-cpp/blob/develop/libhdt/src/bitsequence/BitSequence375.cpp
        // described in "Optimal lower bounds for rank and select indexes" by Alexander Glynski 2006 ?
        // unfinished, untested

        let mut block_pop = 0_usize;
        let mut superblock_pop = 0_usize;
        let mut block_index = 0_usize;
        let mut superblock_index = 0_usize;
        let num_words = Self::num_words(num_bits);
        let mut superblocks: Vec<usize> = Vec::with_capacity(num_words);
        let mut blocks: Vec<u8> = Vec::with_capacity(1 + (num_words - 1) / BLOCKS_PER_SUPER);

        while block_index < num_words {
            if !(block_index % BLOCKS_PER_SUPER) == 0 {
                superblock_pop += block_pop;
                if (superblock_index < superblocks.len()) {
                    superblock_index += 1;
                    superblocks.push(superblock_pop);
                }
                block_pop = 0
            }

            blocks[block_index] = block_pop as u8; // TODO: is this correct? can this overflow?
            block_pop += data[block_index].count_ones() as usize;
            block_index += 1;
        }

        let num_ones = superblock_pop + block_pop;

        let bitmap = Bitmap { num_bits, num_ones, data, superblocks, blocks };
        Ok(bitmap)
    }

    fn num_words(bits: usize) -> usize {
        if bits == 0 {
            return 1;
        }
        ((bits - 1) / USIZE_BITS) + 1
    }

    /** Return the first position starting at start that contains a 1.
     * In case there are no more ones in the bitsequence, the function
     * returns the length of the bitmap
     */
    pub fn select_next_1(&self, start: usize) -> usize {
        0
    }
}
