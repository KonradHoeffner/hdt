use crate::containers::vbyte::read_vbyte;
//use bytesize::ByteSize;
//use std::fmt;
use std::io;
use std::io::BufRead;
use std::mem::size_of;
use std::thread;
use sucds::int_vectors::CompactVector;
use sucds::Serializable;

const USIZE_BITS: usize = usize::BITS as usize;

/*
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
*/

/// Read sequence including metadata from HDT data.
pub fn read_sequence<R: BufRead>(reader: &mut R) -> io::Result<(CompactVector, thread::JoinHandle<bool>)> {
    use io::Error;
    use io::ErrorKind::InvalidData;
    use io::ErrorKind::Other;

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
    let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
    let mut digest = crc8.digest();
    digest.update(&history);
    if digest.finalize() != crc_code {
        return Err(Error::new(InvalidData, "Invalid CRC8-CCIT checksum"));
    }

    // read body data
    // read all but the last entry, since the last one is byte aligned
    let total_bits = bits_per_entry * entries;
    let full_byte_amount = (((total_bits + USIZE_BITS - 1) / USIZE_BITS).saturating_sub(1)) * size_of::<usize>();
    let mut full_words = vec![0_u8; full_byte_amount];
    reader.read_exact(&mut full_words)?;
    let mut data: Vec<usize> = Vec::with_capacity(full_byte_amount / 8 + 2);
    // read entry body

    // turn the raw bytes into usize values
    for word in full_words.chunks_exact(size_of::<usize>()) {
        if let Ok(word_data) = <[u8; 8]>::try_from(word) {
            data.push(usize::from_le_bytes(word_data));
        } else {
            return Err(Error::new(Other, "failed to read usize"));
        }
    }

    let v = CompactVector::deserialize_from(full_words.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    // keep track of history for CRC32
    let mut history = full_words;

    // read the last few bits, byte aligned
    let mut bits_read = 0;
    let mut last_value: usize = 0;
    let last_entry_bits = if total_bits == 0 { 0 } else { ((total_bits - 1) % USIZE_BITS) + 1 };

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
    let crc_handle = thread::spawn(move || {
        let crc_code = u32::from_le_bytes(crc_code);

        // validate entry body CRC32
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest = crc32.digest();
        digest.update(&history);
        digest.finalize() == crc_code
    });
    Ok((v, crc_handle))
}
