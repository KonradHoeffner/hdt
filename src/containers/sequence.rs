use crate::containers::vbyte::read_vbyte;
use crc_any::{CRCu32, CRCu8};
use std::io;
use std::io::BufRead;
use std::mem::size_of;

const USIZE_BITS: usize = usize::BITS as usize;

#[derive(Debug, Clone)]
pub struct Sequence {
    pub entries: usize,
    pub bits_per_entry: usize,
    pub data: Vec<usize>,
}

impl Sequence {
    pub fn get(&self, index: usize) -> usize {
        let scaled_index = index * self.bits_per_entry;
        let block_index = scaled_index / USIZE_BITS;
        let bit_index = scaled_index % USIZE_BITS;

        let mut result = 0;

        let result_shift = USIZE_BITS - self.bits_per_entry;
        if bit_index + self.bits_per_entry <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - self.bits_per_entry;
            result = (self.data[block_index] << block_shift) >> result_shift;
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - self.bits_per_entry;
            result = self.data[block_index] >> bit_index;
            result |= (self.data[(block_index + 1)] << block_shift) >> result_shift;
        }

        result
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        use io::ErrorKind::Other;
        use std::convert::TryFrom;

        // read entry metadata
        // keep track of history for CRC8
        let mut history: Vec<u8> = Vec::new();

        // read and validate type
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        if buffer[0] != 1 {
            return Err(Error::new(InvalidData, "Invalid LogArray type"));
        }

        // read number of bits per entry
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        let bits_per_entry = buffer[0] as usize;
        if bits_per_entry > USIZE_BITS {
            return Err(Error::new(InvalidData, "entry size too large (>64 bit)"));
        }

        // read number of entries
        let (entries, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);

        // read entry metadata CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate entry metadata CRC8
        let mut crc = CRCu8::crc8();
        crc.digest(&history[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC8-CCIT checksum"));
        }

        // read entry body
        // keep track of history for CRC32
        let mut history: Vec<u8> = Vec::new();
        // read body data
        let mut data: Vec<usize> = Vec::new();

        // read all but the last entry, since the last one is byte aligned
        let total_bits = bits_per_entry * entries;
        let full_byte_amount =
            (((total_bits + USIZE_BITS - 1) / USIZE_BITS) - 1) * size_of::<usize>();
        let mut full_words = vec![0_u8; full_byte_amount];
        reader.read_exact(&mut full_words);
        history.extend_from_slice(&full_words);

        // turn the raw bytes into usize values
        for word in full_words.chunks_exact(size_of::<usize>()) {
            if let Ok(word_data) = <[u8; 8]>::try_from(word) {
                data.push(usize::from_le_bytes(word_data));
            } else {
                return Err(Error::new(Other, "failed to read usize"));
            }
        }

        // read the last few bits, byte aligned
        let mut bits_read = 0;
        let mut last_value: usize = 0;
        let last_entry_bits = if total_bits == 0 {
            0
        } else {
            ((total_bits - 1) % USIZE_BITS) + 1
        };

        while bits_read < last_entry_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            history.extend_from_slice(&buffer);
            last_value |= (buffer[0] as usize) << bits_read;
            bits_read += size_of::<usize>();
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

        Ok(Sequence {
            entries,
            bits_per_entry,
            data,
        })
    }
}
