//! Bitmap with rank and select support read from an HDT file.
use crate::containers::vbyte::read_vbyte;
use bytesize::ByteSize;
use eyre::{eyre, Result};
use rsdict::RsDict;
use std::convert::TryFrom;
use std::fmt;
use std::io::BufRead;
use std::mem::size_of;

//const USIZE_BITS: usize = usize::BITS as usize;

/// Compact bitmap representation with rank and select support.
#[derive(Clone)]
pub struct Bitmap {
    //num_bits: usize,
    // could also use sucds::rs_bit_vector::RsBitVector, that would be -1 dependency but that doesn't seem to have from_blocks
    /// Currently using the rsdict crate.
    pub dict: RsDict,
    //pub data: Vec<u64>,
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ByteSize(self.dict.heap_size() as u64))
    }
}

impl Bitmap {
    /// Construct a bitmap from an existing bitmap in form of a vector, which doesn't have rank and select support.
    pub fn new(data: Vec<u64>) -> Self {
        let dict = RsDict::from_blocks((data as Vec<u64>).into_iter());
        Bitmap { dict }
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.heap_size()
    }

    /// Whether the node given position is the last child of its parent.
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.dict.get_bit(word_index as u64)
    }

    /// Read bitmap from a suitable point within HDT file data and verify checksums.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        let mut history: Vec<u8> = Vec::with_capacity(5);

        // read the type
        let mut bitmap_type = [0u8];
        reader.read_exact(&mut bitmap_type)?;
        history.extend_from_slice(&bitmap_type);
        if bitmap_type[0] != 1 {
            return Err(eyre!("Read unsupported bitmap type {} != 1", bitmap_type[0]));
        }

        // read the number of bits
        let (num_bits, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);

        // read section CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate section CRC8
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        digest.update(&history);
        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(eyre!("Invalid CRC8-CCIT checksum {crc_calculated}, expected {crc_code}"));
        }

        // read all but the last word, last word is byte aligned
        let full_byte_amount = ((num_bits - 1) >> 6) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        // div_ceil is unstable
        let mut data: Vec<u64> = Vec::with_capacity(full_byte_amount / 8 + usize::from(full_byte_amount % 8 != 0));
        reader.read_exact(&mut full_words)?;

        for word in full_words.chunks_exact(size_of::<u64>()) {
            if let Ok(word_data) = <[u8; 8]>::try_from(word) {
                data.push(u64::from_le_bytes(word_data));
            } else {
                return Err(eyre!("Failed to turn raw bytes into u64"));
            }
        }

        // initiate computation of CRC32
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        digest.update(&full_words);

        let mut bits_read = 0;
        let mut last_value: u64 = 0;
        let last_word_bits = if num_bits == 0 { 0 } else { ((num_bits - 1) % 64) + 1 };

        while bits_read < last_word_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            digest.update(&buffer);
            last_value |= (buffer[0] as u64) << bits_read;
            bits_read += 8;
        }
        data.push(last_value);

        // read entry body CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let crc_code = u32::from_le_bytes(crc_code);

        // validate entry body CRC32
        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(eyre!("Invalid CRC32C checksum {crc_calculated}, expected {crc_code}"));
        }

        Ok(Self::new(data))
    }
}
