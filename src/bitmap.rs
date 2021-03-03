use crate::vbyte::read_vbyte;
use crc_any::{CRCu32, CRCu8};
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct Bitmap {
    num_bits: usize,
    data: Vec<usize>,
    super_blocks: Vec<usize>,
    blocks: Vec<u8>,
    pop: usize,
}

impl Bitmap {
    pub fn update_index(&mut self) {
        self.super_blocks = vec![0; 1 + (self.data.len() - 1) / 4];
        self.blocks = vec![0; self.data.len()];

        let mut block_index = 0;
        let mut block_amt = 0;
        let mut super_block_index = 0;
        let mut super_block_amt = 0;

        while block_index < self.data.len() {
            if block_index % 4 == 0 {
                super_block_amt += block_amt;

                if super_block_index < self.super_blocks.len() {
                    self.super_blocks[super_block_index] = super_block_amt;
                    super_block_index += 1;
                }

                block_amt = 0;
            }

            self.blocks[block_index] = (block_amt & 0xFF) as u8;
            block_amt += self.data[block_index].count_ones() as usize;
            block_index += 1;
        }

        self.pop = super_block_amt + block_amt;
    }

    pub fn rank1(&self, pos: usize) -> usize {
        if pos >= self.num_bits {
            return self.pop;
        }

        let super_block_index = pos / (4 * 64);
        let mut super_block_rank = self.super_blocks[super_block_index];

        let block_index = pos / 64;
        let block_rank = 0xFF & self.blocks[super_block_index] as usize;

        let chunk_index = 63 - pos % 64;
        let chunk_rank = (self.data[block_index] << chunk_index).count_ones() as usize;

        super_block_rank + block_rank + chunk_rank
    }

    pub fn select1(&self, pos: usize) -> usize {
        // TODO
        // if pos > self.pop {
        //     return self.num_bits;
        // }

        // if self.num_bits == 0 {
        //     return 0;
        // }

        // if let Ok(super_block_index) = self.super_blocks.binary_search(&pos) {

        // } else {

        // }

        0
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
        let last_word_bits = if num_bits == 0 {
            0
        } else {
            ((num_bits - 1) % 64) + 1
        };

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

        let mut bitmap = Bitmap {
            num_bits,
            data,
            super_blocks: Vec::new(),
            blocks: Vec::new(),
            pop: 0,
        };

        bitmap.update_index();
        Ok(bitmap)
    }
}
