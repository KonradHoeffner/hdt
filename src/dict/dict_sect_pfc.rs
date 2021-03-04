use crate::containers::vbyte::read_vbyte;
use crate::containers::Sequence;
use crc_any::{CRCu32, CRCu8};
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct DictSectPFC {
    num_strings: usize,
    packed_length: usize,
    block_size: usize,
    sequence: Sequence,
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
        let sequence = Sequence::read(reader)?;

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
            sequence,
            packed_data,
        })
    }
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
        let sequence = dict_sect_pfc.sequence;
        let data_size = ((sequence.bits_per_entry * sequence.entries + 63) / 64);
        assert_eq!(sequence.data.len(), data_size);
        assert_eq!(dict_sect_pfc.packed_data.len(), dict_sect_pfc.packed_length);
    }
}
