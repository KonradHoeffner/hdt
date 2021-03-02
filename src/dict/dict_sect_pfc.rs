use crc_any::CRCu8;
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct DictSectPFC {
    num_strings: usize,
    packed_length: usize,
    block_size: usize,
}

impl DictSectPFC {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

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

        let mut crc_code = [0_u8, 1];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        let mut crc = CRCu8::crc8();
        crc.digest(&buffer[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(
                InvalidData,
                format!(
                    "Invalid CRC8-CCIT checksum: {:?} vs {:?}",
                    crc.get_crc(),
                    crc_code
                ),
            ));
        }

        Ok(DictSectPFC {
            num_strings,
            packed_length,
            block_size,
        })
    }
}

const MAX_VBYTE_BYTES: usize = size_of::<usize>() * 8 / 7 + 1;

fn read_vbyte<R: BufRead>(reader: &mut R) -> io::Result<(usize, Vec<u8>)> {
    use io::Error;
    use io::ErrorKind::InvalidData;

    let mut n = 0;
    let mut buffer = [0_u8; 1];
    reader.read_exact(&mut buffer);
    let mut bytes_read = Vec::new();
    bytes_read.extend_from_slice(&buffer);

    while buffer[0] & 0x80 == 0x00 {
        if bytes_read.len() >= MAX_VBYTE_BYTES {
            return Err(Error::new(
                InvalidData,
                "Tried to read a VByte that does not fit into a usize",
            ));
        } else {
            n = n * 128 + buffer[0] as usize;
            reader.read_exact(&mut buffer);
            bytes_read.extend_from_slice(&buffer);
        }
    }

    Ok((n * 128 + (buffer[0] & 0x7F) as usize, bytes_read))
}

fn encode_vbyte(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut n = n;

    while n > 127 {
        bytes.push((n & 127) as u8);
        n >>= 7;
    }

    bytes.push((n & 127) as u8);
    bytes[0] |= 0x80;
    bytes.reverse();

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
        assert_eq!(dict_sect_pfc.num_strings, 1448449);
        assert_eq!(dict_sect_pfc.packed_length, 1035416);
        assert_eq!(dict_sect_pfc.block_size, 8);
    }

    #[test]
    fn test_decode() {
        // this represents 824
        let buffer = b"\x06\xB8";
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, 824);
            assert_eq!(bytes_read, vec![0x06_u8, 0xB8_u8]);
        } else {
            panic!("Failed to read vbyte");
        }
    }

    #[test]
    fn test_max_value() {
        // this represents usize::MAX
        let buffer = b"\x01\x7F\x7F\x7F\x7F\x7F\x7F\x7F\x7F\xFF";
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, usize::MAX);
            assert_eq!(
                bytes_read,
                vec![
                    0x01_u8, 0x7F_u8, 0x7F_u8, 0x7F_u8, 0x7F_u8, 0x7F_u8, 0x7F_u8, 0x7F_u8,
                    0x7F_u8, 0xFF_u8
                ]
            );
        } else {
            panic!("Failed to read vbyte");
        }
    }

    #[test]
    #[should_panic]
    fn test_decode_too_large() {
        // this represents usize::MAX + 1
        let buffer = b"\x02\x7F\x7F\x7F\x7F\x7F\x7F\x7F\x7F\xFF";
        let mut reader = BufReader::new(&buffer[..]);
        read_vbyte(&mut reader);
    }

    #[test]
    fn test_encode() {
        assert_eq!(encode_vbyte(824), vec![0x06, 0xB8])
    }
}
