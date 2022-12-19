use crc_any::CRCu16;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;
use std::io::BufRead;
use std::str;

/// Type of Control Information.
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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
            _ => Err(Self::Error::new(io::ErrorKind::InvalidData, "Unrecognized control type")),
        }
    }
}

/// <https://www.rdfhdt.org/hdt-binary-format/>: "preamble that describes a chunk of information".
#[derive(Debug, Clone)]
pub struct ControlInfo {
    /// Type of control information.
    pub control_type: ControlType,
    /// "URI identifier of the implementation of the following section."
    pub format: String,
    /// Key-value entries, ASCII only.
    properties: HashMap<String, String>,
}

impl ControlInfo {
    /// Read and verify control information.
    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        // Keep track of what we are reading for computing the CRC afterwards.
        let mut history: Vec<u8> = Vec::new();

        // 1. Read the HDT Cookie
        let mut hdt_cookie: [u8; 4] = [0; 4];
        reader.read_exact(&mut hdt_cookie)?;
        if let Ok(hdt_cookie) = str::from_utf8(&hdt_cookie) {
            if hdt_cookie != "$HDT" {
                return Err(Error::new(InvalidData, "Chunk is invalid HDT Control Information"));
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
        if format.pop().is_none() {
            // We failed to get rid of the trailing 0x00 byte,
            // in theory we should never reach this branch.
            unreachable!();
        }
        let format = String::from_utf8(format).map_err(|e| Error::new(InvalidData, e))?;

        // 4. Read the Properties
        let mut prop_str = Vec::new();
        reader.read_until(0x00, &mut prop_str)?;
        history.extend_from_slice(&prop_str);
        if prop_str.pop().is_none() {
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
        let mut crc_code = [0_u8; 2];
        reader.read_exact(&mut crc_code)?;
        let crc_code: u16 = u16::from_le_bytes(crc_code);

        // 6. Check the CRC
        let mut crc = CRCu16::crc16();
        crc.digest(&history[..]);
        if crc.get_crc() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC16-ANSI checksum"));
        }

        Ok(ControlInfo { control_type, format, properties })
    }

    /// Get property value for the given key, if available.
    pub fn get(&self, key: &str) -> Option<String> {
        self.properties.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init;
    use std::io::BufReader;

    #[test]
    fn read_info() {
        init();
        let info = b"$HDT\x01<http://purl.org/HDT/hdt#HDTv1>\x00\x00\x76\x35";
        let mut reader = BufReader::new(&info[..]);

        if let Ok(info) = ControlInfo::read(&mut reader) {
            assert_eq!(info.control_type, ControlType::Global);
            assert_eq!(info.format, "<http://purl.org/HDT/hdt#HDTv1>");
            assert!(info.properties.is_empty());
        } else {
            panic!("Failed to read control info");
        }
    }
}
