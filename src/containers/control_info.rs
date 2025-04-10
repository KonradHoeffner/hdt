use io::ErrorKind::UnexpectedEof;
use std::collections::HashMap;
use std::io::BufRead;
use std::io::{self, Write};
use std::str;

pub const TERMINATOR: [u8; 1] = [0];
const HDT_HEADER: &[u8] = b"$HDT";

/// Type of Control Information.
#[allow(missing_docs)]
#[repr(u8)]
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum ControlType {
    #[default]
    Unknown = 0,
    Global = 1,
    Header = 2,
    Dictionary = 3,
    Triples = 4,
    Index = 5,
}

impl TryFrom<u8> for ControlType {
    type Error = ControlInfoReadErrorKind;

    fn try_from(original: u8) -> Result<Self, Self::Error> {
        match original {
            0 => Ok(ControlType::Unknown),
            1 => Ok(ControlType::Global),
            2 => Ok(ControlType::Header),
            3 => Ok(ControlType::Dictionary),
            4 => Ok(ControlType::Triples),
            5 => Ok(ControlType::Index),
            _ => Err(ControlInfoReadErrorKind::InvalidControlType(original)),
        }
    }
}

/// <https://www.rdfhdt.org/hdt-binary-format/>: "preamble that describes a chunk of information".
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ControlInfo {
    /// Type of control information.
    pub control_type: ControlType,
    /// "URI identifier of the implementation of the following section."
    pub format: String,
    /// Key-value entries, ASCII only.
    pub properties: HashMap<String, String>,
}

/// The error type for the `read` method.
#[derive(thiserror::Error, Debug)]
#[error("failed to read HDT control info")]
pub struct ControlInfoReadError(#[from] ControlInfoReadErrorKind);

/// The kind of the ControlInfoReadError error.
#[derive(thiserror::Error, Debug)]
pub enum ControlInfoReadErrorKind {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("chunk {0:?} does not equal the HDT cookie '$HDT'")]
    HdtCookie([u8; 4]),
    #[error("invalid separator while reading format")]
    InvalidSeparator,
    #[error("invalid CRC16-ANSI checksum")]
    InvalidChecksum,
    #[error("invalid UTF8")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("invalid control type '{0}'")]
    InvalidControlType(u8),
}

impl ControlInfo {
    /// Read and verify control information.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<Self, ControlInfoReadError> {
        Ok(Self::read_kind(reader)?)
    }

    // Helper function returning a ControlInfoReadErrorKind that is wrapped by Self::read.
    fn read_kind<R: BufRead>(reader: &mut R) -> Result<Self, ControlInfoReadErrorKind> {
        use ControlInfoReadErrorKind::*;
        //use std::io::Error;

        // Keep track of what we are reading for computing the CRC afterwards.
        let crc = crc::Crc::<u16>::new(&crc::CRC_16_ARC);
        let mut digest = crc.digest();

        // 1. Read the HDT Cookie
        let mut hdt_cookie: [u8; 4] = [0; 4];
        reader.read_exact(&mut hdt_cookie)?;
        if &hdt_cookie != b"$HDT" {
            return Err(HdtCookie(hdt_cookie));
        }
        digest.update(&hdt_cookie);

        // 2. Read the Control Type
        let mut control_type: [u8; 1] = [0; 1];
        reader.read_exact(&mut control_type)?;
        digest.update(&control_type);
        let control_type = ControlType::try_from(control_type[0])?;

        // 3. Read the Format
        let mut format = Vec::new();
        reader.read_until(0x00, &mut format)?;
        digest.update(&format);
        if format.pop() != Some(0x00) {
            return Err(InvalidSeparator);
        }
        let format = String::from_utf8(format)?;

        // 4. Read the Properties
        let mut prop_str = Vec::new();
        reader.read_until(0x00, &mut prop_str)?;
        digest.update(&prop_str);
        if prop_str.pop() != Some(0x00) {
            return Err(std::io::Error::new(UnexpectedEof, "reading the properties").into());
        }
        let prop_str = String::from_utf8(prop_str)?;
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
        if digest.finalize() != crc_code {
            return Err(InvalidChecksum);
        }

        Ok(ControlInfo { control_type, format, properties })
    }

    /// Save a ControlInfo object to file using crc
    pub fn save(&self, dest_writer: &mut impl Write) -> Result<(), Box<dyn std::error::Error>> {
        let crc = crc::Crc::<u16>::new(&crc::CRC_16_ARC);
        let mut hasher = crc.digest();
        dest_writer.write_all(HDT_HEADER)?;
        hasher.update(HDT_HEADER);

        // write type
        let type_: [u8; 1] = [self.control_type as u8];
        dest_writer.write_all(&type_)?;
        hasher.update(&type_);

        // write format
        let format = self.format.as_bytes();
        dest_writer.write_all(format)?;
        hasher.update(format);
        dest_writer.write_all(&TERMINATOR)?;
        hasher.update(&TERMINATOR);

        // write properties
        let mut properties_string = String::new();
        for (key, value) in &self.properties {
            properties_string.push_str(key);
            properties_string.push('=');
            properties_string.push_str(value);
            properties_string.push(';');
        }
        dest_writer.write_all(properties_string.as_bytes())?;
        hasher.update(properties_string.as_bytes());
        dest_writer.write_all(&TERMINATOR)?;
        hasher.update(&TERMINATOR);

        let checksum = hasher.finalize();
        dest_writer.write_all(&checksum.to_le_bytes())?;

        Ok(())
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
    fn read_info() -> color_eyre::Result<()> {
        init();
        let info = b"$HDT\x01<http://purl.org/HDT/hdt#HDTv1>\x00\x00\x76\x35";
        let mut reader = BufReader::new(&info[..]);

        let info = ControlInfo::read(&mut reader)?;
        assert_eq!(info.control_type, ControlType::Global);
        assert_eq!(info.format, "<http://purl.org/HDT/hdt#HDTv1>");
        assert!(info.properties.is_empty());
        Ok(())
    }

    #[test]
    fn write_info() -> color_eyre::Result<()> {
        init();
        let control_type = ControlType::Global;
        let format = "<http://purl.org/HDT/hdt#HDTv1>".to_owned();
        let mut properties = HashMap::<String, String>::new();
        properties.insert("Software".to_owned(), "hdt_rs".to_owned());
        let info = ControlInfo { control_type, format, properties };

        let mut buffer = Vec::new();
        assert!(info.save(&mut buffer).is_ok());

        let expected = b"$HDT\x01<http://purl.org/HDT/hdt#HDTv1>\x00Software=hdt_rs;\x00\x52\x22";
        assert_eq!(buffer, expected);

        let mut reader = BufReader::new(&expected[..]);
        let info2 = ControlInfo::read(&mut reader)?;
        assert_eq!(info, info2);
        Ok(())
    }
}
