use crate::vbyte::read_vbyte;
use crate::ControlInfo;
use crc_any::{CRCu32, CRCu8};
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub enum TripleSect {
    Bitmap(TriplesBitmap),
    // List(TriplesList),
}

impl TripleSect {
    fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;
        let triples_ci = ControlInfo::read(reader)?;

        match &triples_ci.format[..] {
            "<http://purl.org/HDT/hdt#triplesBitmap>" => {
                Ok(TripleSect::Bitmap(TriplesBitmap::read(reader, triples_ci)?))
            }
            "<http://purl.org/HDT/hdt#triplesList>" => Err(Error::new(
                InvalidData,
                "Triples Lists are not supported yet.",
            )),
            _ => Err(Error::new(InvalidData, "Unknown triples listing format.")),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Order {
    Unknown = 0,
    SPO = 1,
    SOP = 2,
    PSO = 3,
    POS = 4,
    OSP = 5,
    OPS = 6,
}

impl TryFrom<u32> for Order {
    type Error = std::io::Error;

    fn try_from(original: u32) -> Result<Self, Self::Error> {
        match original {
            0 => Ok(Order::Unknown),
            1 => Ok(Order::SPO),
            2 => Ok(Order::SOP),
            3 => Ok(Order::PSO),
            4 => Ok(Order::POS),
            5 => Ok(Order::OSP),
            6 => Ok(Order::OPS),
            _ => Err(Self::Error::new(
                io::ErrorKind::InvalidData,
                "Unrecognized order",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TriplesBitmap {
    order: Order,
}

impl TriplesBitmap {
    fn read<R: BufRead>(reader: &mut R, triples_ci: ControlInfo) -> io::Result<Self> {
        use std::io::Error;
        use std::io::ErrorKind::InvalidData;

        let mut order: Order;
        if let Some(n) = triples_ci.get("order").and_then(|v| v.parse::<u32>().ok()) {
            order = Order::try_from(n)?;
        } else {
            return Err(Error::new(InvalidData, "Unrecognized order"));
        }

        // read bitmapY
        let bitmap_y = Bitmap::read(reader)?;

        // read bitmapZ
        let bitmap_z = Bitmap::read(reader)?;

        // read seqY
        unimplemented!();

        // read seqZ
        unimplemented!();

        // construct adjListY
        unimplemented!();

        // construct adjListZ
        unimplemented!();
    }
}

#[derive(Debug, Clone)]
pub struct Bitmap {
    num_bits: usize,
    data: Vec<usize>,
}

impl Bitmap {
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

        Ok(Bitmap { num_bits, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControlInfo, Dict, Header};
    use std::fs::File;
    use std::io::BufReader;

    // #[test]
    // fn read_triples() {
    //     let file = File::open("tests/resources/swdf.hdt").expect("error opening file");
    //     let mut reader = BufReader::new(file);
    //     ControlInfo::read(&mut reader).unwrap();
    //     Header::read(&mut reader).unwrap();
    //     Dict::read(&mut reader).unwrap();
    //     let triples = TripleSect::read(&mut reader).unwrap();
    // }
}
