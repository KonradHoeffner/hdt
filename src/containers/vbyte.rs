//! Encoding and decoding variable-length byte sequences.
//! Only values up to usize are supported.
//! # Important
//! The original HDT implementation has an off-by-one error and shifts by 7, hence we
//! have to copy the same off-by-one error in order to read the file format.
//! The correct implementation is supposed to shift by 8!
//! Look at the commented out tests at the bottom of the file for proof.
use std::io;
use std::io::BufRead;

const MAX_VBYTE_BYTES: usize = usize::BITS as usize / 7 + 1;

/// little endian
pub fn read_vbyte<R: BufRead>(reader: &mut R) -> io::Result<(usize, Vec<u8>)> {
    use io::Error;
    use io::ErrorKind::InvalidData;

    let mut n: u128 = 0;
    let mut shift = 0;
    let mut buffer = [0u8];
    let mut bytes_read = Vec::new();
    reader.read_exact(&mut buffer)?;
    bytes_read.extend_from_slice(&buffer);

    while (buffer[0] & 0x80) == 0 {
        if bytes_read.len() >= MAX_VBYTE_BYTES {
            return Err(Error::new(InvalidData, "Tried to read a VByte that does not fit into a usize"));
        }

        n |= ((buffer[0] & 127) as u128) << shift;
        reader.read_exact(&mut buffer)?;
        bytes_read.extend_from_slice(&buffer);
        // off by one, is supposed to be shift by 8
        shift += 7;
    }

    n |= ((buffer[0] & 127) as u128) << shift;

    if let Ok(valid) = usize::try_from(n) {
        Ok((valid, bytes_read))
    } else {
        Err(Error::new(InvalidData, "Tried to read a VByte that does not fit into a usize"))
    }
}

/// Decode a vbyte to usize n at the given offset and return (n, amount of bytes read).
pub const fn decode_vbyte_delta(data: &[u8], offset: usize) -> (usize, usize) {
    let mut n: usize = 0;
    let mut shift: usize = 0;
    let mut byte_amount = 0;

    while (data[offset + byte_amount] & 0x80) == 0 {
        n |= ((data[offset + byte_amount] & 127) as usize) << shift;
        byte_amount += 1;
        shift += 7;
    }

    n |= ((data[offset + byte_amount] & 127) as usize) << shift;
    byte_amount += 1;

    (n, byte_amount)
}

/// little endian
pub fn encode_vbyte(n: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut n = n;

    while n > 127 {
        bytes.push((n & 127) as u8);
        // off by one, is supposed to be shift by 8
        n >>= 7;
    }

    bytes.push((n | 0x80) as u8);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn test_encode_decode() {
        let buffer = encode_vbyte(824);
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, 824);
            assert_eq!(bytes_read, buffer);
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
            assert_eq!(bytes_read, buffer);
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
        read_vbyte(&mut reader).unwrap();
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
