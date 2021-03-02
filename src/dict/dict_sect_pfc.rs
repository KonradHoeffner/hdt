use crc_any::{CRCu32, CRCu8};
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct DictSectPFC {
    num_strings: usize,
    packed_length: usize,
    block_size: usize,
    log_array: LogArray,
    packed_data: Vec<u8>,
}

impl DictSectPFC {
    pub fn num_strings(&self) -> usize {
        self.num_strings
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        // read section meta data
        // The CRC includes the type of the block, inaccuracy in the spec, careful.
        let mut buffer = vec![0x02_u8];
        // This was determined based on https://git.io/JthMG because the spec on this
        // https://www.rdfhdt.org/hdt-binary-format was inaccurate, it's 3 vbytes, not 2.
        let (num_strings, bytes_read) = read_vbyte(reader)?;
        buffer.extend_from_slice(&bytes_read);
        let (packed_length, bytes_read) = read_vbyte(reader)?;
        buffer.extend_from_slice(&bytes_read);
        let (block_size, bytes_read) = read_vbyte(reader)?;
        buffer.extend_from_slice(&bytes_read);

        // read section CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate section CRC8
        let mut crc = CRCu8::crc8();
        crc.digest(&buffer[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC8-CCIT checksum"));
        }

        // validate section size
        if packed_length > usize::MAX {
            return Err(Error::new(
                InvalidData,
                // We will probably die from global warming before we reach section sizes this
                // large; if we do, then color me surprised, you never know :).
                "Cannot address sections over 16 exabytes (EB) on 64-bit machines",
            ));
        }

        // read sequence log array
        let log_array = LogArray::read(reader)?;

        // read packed data
        let mut packed_data = vec![0u8; packed_length];
        reader.read_exact(&mut packed_data)?;

        // read packed data CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let crc_code = u32::from_le_bytes(crc_code);

        // validate packed data CRC32
        let mut crc = CRCu32::crc32c();
        crc.digest(&packed_data[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC32C checksum"));
        }

        Ok(DictSectPFC {
            num_strings,
            packed_length,
            block_size,
            log_array,
            packed_data,
        })
    }
}

#[derive(Debug, Clone)]
struct LogArray {
    entries: usize,
    bits_per_entry: usize,
    data: Vec<usize>,
}

impl LogArray {
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
        if bits_per_entry > 64 {
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
        let full_byte_amount = (((total_bits + 63) / 64) - 1) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        reader.read_exact(&mut full_words);
        history.extend_from_slice(&full_words);

        // turn the raw bytes into usize/u64 values
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
            ((total_bits - 1) % 64) + 1
        };

        while bits_read < last_entry_bits {
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

        Ok(LogArray {
            entries,
            bits_per_entry,
            data,
        })
    }
}

const MAX_VBYTE_BYTES: usize = size_of::<usize>() * 8 / 7 + 1;

// little endian
fn read_vbyte<R: BufRead>(reader: &mut R) -> io::Result<(usize, Vec<u8>)> {
    use io::Error;
    use io::ErrorKind::InvalidData;
    use std::convert::TryFrom;

    let mut n: u128 = 0;
    let mut shift = 0;
    let mut buffer = [0u8];
    let mut bytes_read = Vec::new();
    reader.read_exact(&mut buffer);
    bytes_read.extend_from_slice(&buffer);

    while (buffer[0] & 0x80) == 0 {
        if bytes_read.len() >= MAX_VBYTE_BYTES {
            return Err(Error::new(
                InvalidData,
                "Tried to read a VByte that does not fit into a usize",
            ));
        }

        n |= ((buffer[0] & 127) as u128) << shift;
        reader.read_exact(&mut buffer);
        bytes_read.extend_from_slice(&buffer);
        // IMPORTANT: The original implementation has an off-by-one error here, hence we
        // have to copy the same off-by-one error in order to read the file format.
        // The correct implementation is supposed to shift by 8! Look at the commented out
        // tests at the bottom of the file for proof.
        shift += 7;
    }

    n |= ((buffer[0] & 127) as u128) << shift;

    if let Ok(valid) = usize::try_from(n) {
        Ok((valid, bytes_read))
    } else {
        return Err(Error::new(
            InvalidData,
            "Tried to read a VByte that does not fit into a usize",
        ));
    }
}

// little endian
fn encode_vbyte(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut n = n;

    while n > 127 {
        bytes.push((n & 127) as u8);
        // IMPORTANT: The original implementation has an off-by-one error here, hence we
        // have to copy the same off-by-one error in order to read the file format.
        // The correct implementation is supposed to shift by 8! Look at the commented out
        // tests at the bottom of the file for proof.
        n >>= 7;
    }

    bytes.push((n | 0x80) as u8);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControlInfo, Header};
    use std::fs::File;
    use std::io::BufReader;
    use std::io::Read;

    #[test]
    fn test_section_read() {
        let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();

        // read dictionary control information
        let dict_ci = ControlInfo::read(&mut reader).unwrap();
        if dict_ci.format != "<http://purl.org/HDT/hdt#dictionaryFour>" {
            panic!("invalid dictionary type: {:?}", dict_ci.format);
        }

        // read section preamble
        let mut preamble: [u8; 1] = [0; 1];
        reader.read_exact(&mut preamble).unwrap();
        if preamble[0] != 2 {
            panic!("invalid section type: {:?}", preamble);
        }

        let dict_sect_pfc = DictSectPFC::read(&mut reader).unwrap();
        assert_eq!(dict_sect_pfc.num_strings, 23128);
        assert_eq!(dict_sect_pfc.packed_length, 396479);
        assert_eq!(dict_sect_pfc.block_size, 8);
        let log_array = dict_sect_pfc.log_array;
        let data_size = ((log_array.bits_per_entry * log_array.entries + 63) / 64);
        assert_eq!(log_array.data.len(), data_size);
        assert_eq!(dict_sect_pfc.packed_data.len(), dict_sect_pfc.packed_length);
    }

    #[test]
    fn test_encode_decode() {
        let buffer = encode_vbyte(824);
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, 824);
            assert_eq!(bytes_read, Vec::from(buffer));
        } else {
            panic!("Failed to read vbyte");
        }
    }

    #[test]
    fn test_max_value() {
        let buffer = encode_vbyte(usize::MAX);
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, usize::MAX);
            assert_eq!(bytes_read, Vec::from(buffer));
        } else {
            panic!("Failed to read vbyte");
        }
    }

    #[test]
    #[should_panic]
    fn test_decode_too_large() {
        let mut buffer = encode_vbyte(usize::MAX);
        buffer[MAX_VBYTE_BYTES - 1] &= 0x7F;
        buffer.push(0x7F);
        let mut reader = BufReader::new(&buffer[..]);
        let (val, buffer) = read_vbyte(&mut reader).unwrap();
        assert!(val > usize::MAX);
    }

    // These tests show the off-by-one bug in the current implementation, but
    // we need to keep the bug in order to read the current version of .hdt files.
    //
    // #[test]
    // fn test_encode() {
    //     assert_eq!(encode_vbyte(824), vec![0x38_u8, 0x83_u8])
    // }
    //
    // #[test]
    // fn test_decode() {
    //     // this represents 824
    //     // 0011 1000 1000 0011
    //     // 0x38      0x83
    //     let buffer = b"\x38\x83";
    //     let mut reader = BufReader::new(&buffer[..]);
    //     if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
    //         assert_eq!(number, 824);
    //         assert_eq!(bytes_read, vec![0x38_u8, 0x83_u8]);
    //     } else {
    //         panic!("Failed to read vbyte");
    //     }
    // }
}
