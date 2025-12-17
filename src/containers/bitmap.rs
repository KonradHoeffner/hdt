//! Bitmap with rank and select support read from an HDT file.
use crate::containers::vbyte::{encode_vbyte, read_vbyte};
use bytesize::ByteSize;
use qwt::{
    AccessBin, AccessUnsigned, BitVector, BitVectorMut, RankBin, SelectBin, SpaceUsage,
    bitvector::rs_narrow::RSNarrow,
};
#[cfg(feature = "cache")]
use serde::ser::SerializeStruct;
use std::fmt;
use std::io::BufRead;
use std::mem::size_of;

/// Compact bitmap representation with rank and select support.
#[derive(Clone)]
pub struct Bitmap {
    /// should be private but is needed by containers/bitmap.rs, use methods provided by Bitmap
    pub dict: RSNarrow,
}

pub type Result<T> = core::result::Result<T, Error>;

/// The error type for the bitmap read function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Invalid CRC32C checksum {0}, expected {1}")]
    InvalidCrc32Checksum(u32, u32),
    #[error("Failed to turn raw bytes into u64")]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
    #[error("Read unsupported bitmap type {0} != 1")]
    UnsupportedBitmapType(u8),
}

#[cfg(feature = "cache")]
impl serde::Serialize for Bitmap {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut state: <S as serde::ser::Serializer>::SerializeStruct =
            serializer.serialize_struct("Bitmap", 1)?;

        //bitmap_y
        let mut dict_buffer = Vec::new();
        self.dict.serialize_into(&mut dict_buffer).map_err(serde::ser::Error::custom)?;
        state.serialize_field("dict", &dict_buffer)?;

        state.end()
    }
}

#[cfg(feature = "cache")]
impl<'de> serde::Deserialize<'de> for Bitmap {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct BitmapData {
            dict: Vec<u8>,
        }

        let data = BitmapData::deserialize(deserializer)?;

        // Deserialize `sucds` structures
        let mut bitmap_reader = std::io::BufReader::new(&data.dict[..]);
        let rank9sel = Rank9Sel::deserialize_from(&mut bitmap_reader).map_err(serde::de::Error::custom)?;

        let bitmap = Bitmap { dict: rank9sel };
        Ok(bitmap)
    }
}

impl fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {} bits", ByteSize(self.size_in_bytes() as u64), self.len())
    }
}

impl Bitmap {
    /// Construct a bitmap from an existing bitmap in form of a vector, which doesn't have rank and select support.
    pub fn new(data: Vec<u64>) -> Self {
        let mut v = BitVectorMut::new();
        for d in data {
            let _ = v.append_bits(d, std::mem::size_of::<usize>() * 8);
        }
        //let dict = Rank9Sel::new(v).select1_hints();
        let dict: BitVector = v.into();
        Bitmap { dict: dict.into() }
    }

    /// Size in bytes on the heap.
    pub fn size_in_bytes(&self) -> usize {
        self.dict.space_usage_byte()
    }

    /// Number of bits in the bitmap, multiple of 64
    pub const fn len(&self) -> usize {
        self.dict.n_zeros() + self.dict.n_ones() // RSNarrow.len() is not public
        // self.dict.bv_len() // only on RSWide
    }

    /// Number of bits set
    pub fn num_ones(&self) -> usize {
        self.dict.n_ones()
    }

    /// Returns the position of the k-1-th one bit or None if there aren't that many.
    pub fn select1(&self, k: usize) -> Option<usize> {
        self.dict.select1(k)
    }

    /// Returns the number of one bits from the 0-th bit to the k-1-th bit. Panics if self.len() < pos.
    pub fn rank(&self, k: usize) -> usize {
        self.dict.rank1(k).unwrap_or_else(|| panic!("Out of bounds position: {} >= {}", k, self.len()))
    }

    /// Whether the node given position is the last child of its parent.
    pub fn at_last_sibling(&self, word_index: usize) -> bool {
        self.dict.get(word_index).expect("word index out of bounds")
    }

    /// Read bitmap from a suitable point within HDT file data and verify checksums.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self> {
        use Error::*;
        let mut history: Vec<u8> = Vec::with_capacity(5);

        // read the type
        let mut bitmap_type = [0u8];
        reader.read_exact(&mut bitmap_type)?;
        history.extend_from_slice(&bitmap_type);
        if bitmap_type[0] != 1 {
            return Err(UnsupportedBitmapType(bitmap_type[0]));
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
            return Err(InvalidCrc8Checksum(crc_calculated, crc_code));
        }

        // read all but the last word, last word is byte aligned
        let full_byte_amount = ((num_bits - 1) >> 6) * 8;
        let mut full_words = vec![0_u8; full_byte_amount];
        // div_ceil is unstable
        let mut data: Vec<u64> = Vec::with_capacity(full_byte_amount / 8 + usize::from(full_byte_amount % 8 != 0));
        reader.read_exact(&mut full_words)?;

        for word in full_words.chunks_exact(size_of::<usize>()) {
            data.push(u64::from_le_bytes(<[u8; 8]>::try_from(word)?));
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
        // not worth it to spawn an extra thread as our bitmaps are comparatively small
        let crc_calculated = digest.finalize();
        if crc_calculated != crc_code {
            return Err(InvalidCrc32Checksum(crc_calculated, crc_code));
        }
        Ok(Self::new(data))
    }

    pub fn write(&self, w: &mut impl std::io::Write) -> Result<()> {
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // type
        let bitmap_type: [u8; 1] = [1];
        w.write_all(&bitmap_type)?;
        hasher.update(&bitmap_type);
        // number of bits
        let t = encode_vbyte(self.len());
        w.write_all(&t)?;
        hasher.update(&t);
        // crc8 checksum
        let checksum = hasher.finalize();
        w.write_all(&checksum.to_le_bytes())?;

        // write data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc32.digest();

        let words = self.dict.bit_vector().words();
        let bytes: Vec<u8> = words.iter().flat_map(|&val| val.to_le_bytes()).collect();
        w.write_all(&bytes)?;
        hasher.update(&bytes);
        let crc_code = hasher.finalize();
        let crc_code = crc_code.to_le_bytes();
        w.write_all(&crc_code)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use pretty_assertions::assert_eq;

    #[test]
    fn write() -> color_eyre::Result<()> {
        init();
        let bits: Vec<usize> = vec![0b10111];
        let bitmap = Bitmap::new(bits);
        assert_eq!(bitmap.len(), 64);
        // position of k-1th 1 bit
        // read bits from right to left, i.e. last one is pos 0
        assert_eq!(bitmap.select1(0).unwrap(), 0);
        assert_eq!(bitmap.select1(1).unwrap(), 1);
        assert_eq!(bitmap.select1(2).unwrap(), 2);
        assert_eq!(bitmap.select1(3).unwrap(), 4);
        assert_eq!(bitmap.select1(4), None);
        // number of one bits from the 0-th bit to the k-1-th bit
        assert_eq!(bitmap.rank(1), 1);
        assert_eq!(bitmap.rank(5), 4);
        let mut buf = Vec::<u8>::new();
        bitmap.write(&mut buf)?;
        let bitmap2 = Bitmap::read(&mut std::io::Cursor::new(buf))?;
        assert_eq!(bitmap.dict.bit_vector().words(), bitmap2.dict.bit_vector().words());
        Ok(())
    }
}
