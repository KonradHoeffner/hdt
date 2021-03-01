use std::io;
use std::io::BufRead;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct DictSectPFC {}

impl DictSectPFC {
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        unimplemented!();
    }
}

const MAX_VBYTE_BYTES: usize = size_of::<usize>() * 8 / 7 + 1;

fn read_vbyte<R: BufRead>(reader: &mut R) -> io::Result<(usize, usize)> {
    use io::Error;
    use io::ErrorKind::InvalidData;

    let mut n = 0;
    let mut buffer = [0_u8; 1];
    reader.read_exact(&mut buffer);
    let mut bytes_read = 1;

    while buffer[0] & 0x80 == 0x00 {
        if bytes_read >= MAX_VBYTE_BYTES {
            return Err(Error::new(
                InvalidData,
                "Tried to read a VByte that does not fit into a usize",
            ));
        } else {
            n = n * 128 + buffer[0] as usize;
            reader.read_exact(&mut buffer);
            bytes_read += 1;
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
    use std::io::BufReader;

    #[test]
    fn test_decode() {
        // this represents 824
        let buffer = b"\x06\xB8";
        let mut reader = BufReader::new(&buffer[..]);
        if let Ok((number, bytes_read)) = read_vbyte(&mut reader) {
            assert_eq!(number, 824);
            assert_eq!(bytes_read, 2);
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
            assert_eq!(bytes_read, MAX_VBYTE_BYTES);
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
