//! Bitmap with rank and select support read from an HDT file.
use crate::containers::vbyte::read_vbyte;
use bytesize::ByteSize;
use crc_any::{CRCu32, CRCu8};
use rsdict::RsDict;
use std::convert::TryFrom;
use std::fmt;
use std::io;
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
        //let dict = RsDict::from_blocks((data.clone() as Vec<u64>).into_iter()); // keep data. faster get_bit but more RAM.
        Bitmap { dict }
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.heap_size()
    }

    /// Whether the node given position is the last child of its parent.
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        // Each block in the bitmap has `USIZE_BITS` many bits. If `usize` is 64 bits, and we are
        // looking for the 65th word in the sequence this means we need the first bit of the second
        // `usize` in `self.data`.
        /*
        // We can get the right usize `block` by dividing by the amount of bits in the usize.
        let block_index = word_index / USIZE_BITS;

        // We need to determine the value of the bit at a given `bit_index`
        let bit_index = word_index % USIZE_BITS;
        let bit_flag = 1_u64 << bit_index;

        // If the `bit_flag` is set to one, the bitwise and will be equal to the `bit_flag`.
        self.data[block_index] & bit_flag == bit_flag
        */
        self.dict.get_bit(word_index as u64)
    }

    /// Read bitmap from a suitable point within HDT file data and verify checksums.
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind::{InvalidData, Other};

        let mut history: Vec<u8> = Vec::with_capacity(5);

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

        // read all but the last word, last word is byte aligned
        let full_byte_amount = ((num_bits - 1) >> 6) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        // div_ceil is unstable
        let mut data: Vec<u64> = Vec::with_capacity(full_byte_amount / 8 + usize::from(full_byte_amount % 8 != 0));
        reader.read_exact(&mut full_words)?;

        // turn the raw bytes into usize/u64 values
        for word in full_words.chunks_exact(size_of::<u64>()) {
            if let Ok(word_data) = <[u8; 8]>::try_from(word) {
                data.push(u64::from_le_bytes(word_data));
            } else {
                return Err(Error::new(Other, "failed to read u64"));
            }
        }

        // reset history for CRC32
        let mut history = full_words;

        let mut bits_read = 0;
        let mut last_value: u64 = 0;
        let last_word_bits = if num_bits == 0 { 0 } else { ((num_bits - 1) % 64) + 1 };

        while bits_read < last_word_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            history.extend_from_slice(&buffer);
            last_value |= (buffer[0] as u64) << bits_read;
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

        Ok(Self::new(data))
    }
}
