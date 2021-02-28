use crc::{Crc, CRC_16_ARC};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::str;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum ControlType {
    Unknown = 0,
    Global = 1,
    Header = 2,
    Dictionary = 3,
    Triples = 4,
    Index = 5,
}

impl TryFrom<u8> for ControlType {
    type Error = std::io::Error;

    fn try_from(original: u8) -> Result<Self, Self::Error> {
        match original {
            0 => Ok(ControlType::Unknown),
            1 => Ok(ControlType::Global),
            2 => Ok(ControlType::Header),
            3 => Ok(ControlType::Dictionary),
            4 => Ok(ControlType::Triples),
            5 => Ok(ControlType::Index),
            _ => Err(Self::Error::new(
                io::ErrorKind::InvalidData,
                "Unrecognized control type",
            )),
        }
    }
}

#[derive(Debug)]
pub struct ControlInfo {
    pub control_type: ControlType,
    pub format: String,
    pub properties: HashMap<String, String>,
}

impl ControlInfo {
    pub fn new() -> Self {
        ControlInfo {
            control_type: ControlType::Unknown,
            format: String::new(),
            properties: HashMap::new(),
        }
    }

    pub fn load<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        // Keep track of what we are reading for computing the CRC afterwards.
        let mut history: Vec<u8> = Vec::new();

        // 1. Read the HDT Cookie
        let mut hdt_cookie: [u8; 4] = [0; 4];
        reader.read_exact(&mut hdt_cookie)?;
        if let Ok(hdt_cookie) = str::from_utf8(&hdt_cookie) {
            if hdt_cookie != "$HDT" {
                return Err(Error::new(
                    InvalidData,
                    "Chunk is invalid HDT Control Information",
                ));
            }
        }
        history.extend_from_slice(&hdt_cookie);

        // 2. Read the Control Type
        let mut control_type: [u8; 1] = [0; 1];
        reader.read_exact(&mut control_type)?;
        history.extend_from_slice(&control_type);
        let control_type = ControlType::try_from(control_type[0])?;

        // 3. Read the Format
        let mut format = Vec::new();
        reader.read_until(0x00, &mut format)?;
        history.extend_from_slice(&format);
        if let None = format.pop() {
            // We failed to get rid of the trailing 0x00 byte,
            // in theory we should never reach this branch.
            unreachable!();
        }
        let format = String::from_utf8(format).map_err(|e| Error::new(InvalidData, e))?;

        // 4. Read the Properties
        let mut prop_str = Vec::new();
        reader.read_until(0x00, &mut prop_str)?;
        history.extend_from_slice(&prop_str);
        if let None = prop_str.pop() {
            // We failed to get rid of the trailing 0x00 byte,
            // in theory we should never reach this branch.
            unreachable!();
        }
        let prop_str = String::from_utf8(prop_str).map_err(|e| Error::new(InvalidData, e))?;
        let mut properties = HashMap::new();
        for item in prop_str.split(';') {
            if let Some(index) = item.find('=') {
                let (key, val) = item.split_at(index);
                properties.insert(String::from(key), String::from(&val[1..]));
            }
        }

        // 5. Read the CRC
        let mut crc_code: [u8; 2] = [0; 2];
        reader.read_exact(&mut crc_code)?;
        let crc_code: u16 = u16::from_le_bytes(crc_code);

        // 6. Check the CRC
        if Crc::<u16>::new(&CRC_16_ARC).checksum(&history[..]) != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC16-ANSI checksum"));
        }

        Ok(ControlInfo {
            control_type,
            format,
            properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn read_header() {
        let header = [
            0x24, 0x48, 0x44, 0x54, 0x01, 0x3c, 0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x70,
            0x75, 0x72, 0x6c, 0x2e, 0x6f, 0x72, 0x67, 0x2f, 0x48, 0x44, 0x54, 0x2f, 0x68, 0x64,
            0x74, 0x23, 0x48, 0x44, 0x54, 0x76, 0x31, 0x3e, 0x00, 0x00, 0x76, 0x35,
        ];

        let mut reader = BufReader::new(&header[..]);

        if let Ok(info) = ControlInfo::load(&mut reader) {
            assert_eq!(info.control_type, ControlType::Global);
            assert_eq!(info.format, String::from("<http://purl.org/HDT/hdt#HDTv1>"));
            assert!(info.properties.is_empty());
        } else {
            panic!("Failed to load control info");
        }
    }
}
