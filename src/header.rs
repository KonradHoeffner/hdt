use crate::ControlInfo;
use std::io;
use std::io::BufRead;

#[derive(Debug, Clone)]
pub struct Header {
    format: String,
    length: usize,
}

impl Header {
    pub fn new() -> Self {
        Header {
            format: String::new(),
            length: 0,
        }
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        // TODO: this is missing a NTriples parser for the body.
        use io::Error;
        use io::ErrorKind::InvalidData;

        let header_ci = ControlInfo::read(reader)?;
        if header_ci.format != "ntriples" {
            return Err(Error::new(
                InvalidData,
                "Headers currently only support the NTriples format",
            ));
        }

        let length = header_ci
            .get("length")
            .and_then(|v| v.parse::<usize>().ok());

        if let Some(length) = length {
            Ok(Header {
                format: header_ci.format,
                length: length,
            })
        } else {
            Err(Error::new(InvalidData, "Header is missing header length."))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn read_header() {
        let header = b"$HDT\x02ntriples\x00length=1891;\x00\xd1\xab";
        let mut reader = BufReader::new(&header[..]);

        if let Ok(header) = Header::read(&mut reader) {
            assert_eq!(header.format, "ntriples");
            assert_eq!(header.length, 1891);
        } else {
            panic!("Failed to read control info");
        }
    }
}
