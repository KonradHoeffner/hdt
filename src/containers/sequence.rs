use super::vbyte::encode_vbyte;
use crate::containers::vbyte::read_vbyte;
use bytesize::ByteSize;
#[cfg(feature = "cache")]
use serde::{self, Deserialize, Serialize};
use std::fmt;
use std::io::{BufRead, Write};
use std::mem::size_of;
use std::thread;

const USIZE_BITS: usize = usize::BITS as usize;

/// Integer sequence with a given number of bits, which means numbers may be represented along byte boundaries.
/// Also called "array" in the HDT spec, only Log64 is supported.
//#[derive(Clone)]
#[cfg_attr(feature = "cache", derive(Deserialize, Serialize))]
pub struct Sequence {
    /// Number of integers in the sequence.
    pub entries: usize,
    /// Number of bits that each integer uses.
    pub bits_per_entry: usize,
    /// Data in blocks.
    pub data: Vec<usize>,
    /// whether CRC check was successful
    #[cfg_attr(feature = "cache", serde(skip))]
    pub crc_handle: Option<thread::JoinHandle<bool>>,
}

enum SequenceType {
    Log64 = 1,
    #[allow(dead_code)]
    UInt32 = 2,
    #[allow(dead_code)]
    UInt64 = 3,
}

impl TryFrom<u8> for SequenceType {
    type Error = SequenceReadError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(SequenceType::Log64),
            _ => Err(SequenceReadError::UnsupportedSequenceType(value)),
        }
    }
}

/// The error type for the sequence read function.
#[derive(thiserror::Error, Debug)]
pub enum SequenceReadError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Failed to turn raw bytes into usize")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("invalid LogArray type {0} != 1")]
    UnsupportedSequenceType(u8),
    #[error("entry size of {0} bit too large (>64 bit)")]
    EntrySizeTooLarge(usize),
}

impl fmt::Debug for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} with {} entries, {} bits per entry",
            ByteSize(self.size_in_bytes() as u64),
            self.entries,
            self.bits_per_entry
        )
    }
}

pub struct SequenceIter<'a> {
    sequence: &'a Sequence,
    i: usize,
}

impl Iterator for SequenceIter<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.sequence.entries {
            return None;
        }
        let e = self.sequence.get(self.i);
        self.i += 1;
        Some(e)
    }
}

impl<'a> IntoIterator for &'a Sequence {
    type Item = usize;
    type IntoIter = SequenceIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SequenceIter { sequence: self, i: 0 }
    }
}

impl Sequence {
    /// Get the integer at the given index, counting from 0.
    pub fn get(&self, index: usize) -> usize {
        let scaled_index = index * self.bits_per_entry;
        let block_index = scaled_index / USIZE_BITS;
        let bit_index = scaled_index % USIZE_BITS;

        let mut result;

        let result_shift = USIZE_BITS - self.bits_per_entry;
        if bit_index + self.bits_per_entry <= USIZE_BITS {
            let block_shift = USIZE_BITS - bit_index - self.bits_per_entry;
            result = (self.data[block_index] << block_shift) >> result_shift;
        } else {
            let block_shift = (USIZE_BITS << 1) - bit_index - self.bits_per_entry;
            result = self.data[block_index] >> bit_index;
            result |= (self.data[block_index + 1] << block_shift) >> result_shift;
        }
        result
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        (self.data.len() * USIZE_BITS) >> 3
    }

    /// Read sequence including metadata from HDT data.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self, SequenceReadError> {
        use SequenceReadError::*;
        // read entry metadata
        // keep track of history for CRC8
        let mut history = Vec::<u8>::new();

        // read and validate type
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        SequenceType::try_from(buffer[0])?;

        // read number of bits per entry
        let mut buffer = [0_u8];
        reader.read_exact(&mut buffer)?;
        history.extend_from_slice(&buffer);
        let bits_per_entry = buffer[0] as usize;
        if bits_per_entry > USIZE_BITS {
            return Err(EntrySizeTooLarge(bits_per_entry));
        }
        //println!("bits per entry {bits_per_entry}");

        // read number of entries
        let (entries, bytes_read) = read_vbyte(reader)?;
        history.extend_from_slice(&bytes_read);
        //println!("entries {entries} bytes read {bytes_read:?}");

        // read entry metadata CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate entry metadata CRC8
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        digest.update(&history);

        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(InvalidCrc8Checksum(crc_calculated, crc_code));
        }

        // read body data
        // read all but the last entry, since the last one is byte aligned
        let total_bits = bits_per_entry * entries;
        //println!("total_bits {total_bits}");
        let full_byte_amount = (total_bits.div_ceil(USIZE_BITS).saturating_sub(1)) * size_of::<usize>();
        //println!("full_byte_amount {full_byte_amount}");
        let mut full_words = vec![0_u8; full_byte_amount];
        reader.read_exact(&mut full_words)?;
        //println!("full_words {full_words:?}");
        let mut data: Vec<usize> = Vec::with_capacity(full_byte_amount / size_of::<usize>() + 2);
        // read entry body

        // turn the raw bytes into usize values
        for word in full_words.chunks_exact(size_of::<usize>()) {
            data.push(usize::from_le_bytes(<[u8; size_of::<usize>()]>::try_from(word)?));
        }

        // keep track of history for CRC32
        let mut history = full_words;
        // read the last few bits, byte aligned
        let mut bits_read = 0;
        let mut last_value: usize = 0;
        let last_entry_bits = if total_bits == 0 { 0 } else { ((total_bits - 1) % USIZE_BITS) + 1 };

        //println!("last_entry_bits {last_entry_bits}");
        while bits_read < last_entry_bits {
            let mut buffer = [0u8];
            reader.read_exact(&mut buffer)?;
            history.extend_from_slice(&buffer);
            last_value |= (buffer[0] as usize) << bits_read;
            //println!("last_value {last_value} {last_value:#b}");
            bits_read += size_of::<usize>();
        }
        data.push(last_value);
        // temporarily for bug fixing
        // return Ok(Sequence { entries, bits_per_entry, data, crc_handle: None});
        // read entry body CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let crc_handle = Some(thread::spawn(move || {
            let crc_code = u32::from_le_bytes(crc_code);

            // validate entry body CRC32
            let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
            let mut digest = crc32.digest();
            digest.update(&history);
            digest.finalize() == crc_code
        }));

        Ok(Sequence { entries, bits_per_entry, data, crc_handle })
    }

    /// save sequence per HDT spec using CRC
    pub fn write(&self, dest_writer: &mut impl Write) -> Result<(), SequenceReadError> {
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc8.digest();
        // libhdt/src/sequence/LogSequence2.cpp::save()
        // Write offsets using variable-length encoding
        let seq_type: [u8; 1] = [1];
        dest_writer.write_all(&seq_type)?;
        digest.update(&seq_type);
        // Write numbits
        let bits_per_entry: [u8; 1] = [self.bits_per_entry.try_into().unwrap()];
        //println!("bits_per_entry {bits_per_entry}");
        dest_writer.write_all(&bits_per_entry)?;
        digest.update(&bits_per_entry);
        // Write numentries
        let buf = &encode_vbyte(self.entries);
        dest_writer.write_all(buf)?;
        digest.update(buf);
        let checksum: u8 = digest.finalize();
        dest_writer.write_all(&[checksum])?;

        // Write data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        //let offset_data = self.pack_bits();
        let offset_data: Vec<u8> = self.data.iter().flat_map(|&val| val.to_le_bytes()).collect();
        //let offset_data  = self.data;
        dest_writer.write_all(&offset_data)?;
        digest.update(&offset_data);
        let checksum = digest.finalize();
        dest_writer.write_all(&checksum.to_le_bytes())?;
        dest_writer.flush()?;
        Ok(())
    }

    fn pack_bits(&self) -> Vec<u8> {
        println!("pack_bits");
        let mut output = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_offset = 0;

        for &value in &self.data {
            println!("pack value {value} bits_per_entry {}", self.bits_per_entry);
            let mut val = value & ((1 << self.bits_per_entry) - 1); // mask to get only relevant bits
            println!("mask {:#b}", (1 << self.bits_per_entry) - 1);
            println!("val {val}");
            let mut bits_left = self.bits_per_entry;

            while bits_left > 0 {
                let available = 8 - bit_offset;
                let to_write = bits_left.min(available);

                // Shift bits to align with current byte offset
                current_byte |= ((val & ((1 << to_write) - 1)) as u8) << bit_offset;

                bit_offset += to_write;
                val >>= to_write;
                bits_left -= to_write;

                if bit_offset == 8 {
                    output.push(current_byte);
                    current_byte = 0;
                    bit_offset = 0;
                }
            }
        }

        // Push final byte if there's remaining bits
        if bit_offset > 0 {
            output.push(current_byte);
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use pretty_assertions::assert_eq;

    impl PartialEq for Sequence {
        fn eq(&self, other: &Self) -> bool {
            self.entries == other.entries && self.bits_per_entry == other.bits_per_entry && self.data == other.data
        }
    }

    #[test]
    fn write_read() -> color_eyre::Result<()> {
        init();
        let mut data = Vec::<usize>::new();
        // little endian
        data.push((5 << 16) + (4 << 12) + (3 << 8) + (2 << 4) + 1);
        let s = Sequence { entries: 5, bits_per_entry: 4, data: data.clone(), crc_handle: None };
        let numbers: Vec<usize> = s.into_iter().collect();
        //let expected = vec![1];
        let expected = vec![1, 2, 3, 4, 5];
        assert_eq!(numbers, expected);
        let mut buf = Vec::<u8>::new();
        s.write(&mut buf)?;
        let s2 = Sequence::read(&mut std::io::Cursor::new(buf))?;
        assert_eq!(s, s2);
        let numbers2: Vec<usize> = s2.into_iter().collect();
        assert_eq!(numbers, numbers2);
        Ok(())
    }
}
