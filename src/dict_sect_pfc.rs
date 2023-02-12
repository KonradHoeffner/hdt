/// Dictionary section with plain front coding.
/// See <https://www.rdfhdt.org/hdt-binary-format/#DictionarySectionPlainFrontCoding>.
use crate::containers::vbyte::{decode_vbyte_delta, read_vbyte};
use crate::containers::Sequence;
use crate::triples::Id;
use bytesize::ByteSize;
use log::error;
use std::cmp::{min, Ordering};
use std::fmt;
use std::io;
use std::io::BufRead;
use std::str;
use std::sync::Arc;
use std::thread;
use thiserror::Error;

/// Dictionary section with plain front coding.
//#[derive(Clone)]
pub struct DictSectPFC {
    num_strings: usize,
    block_size: usize,
    sequence: Sequence,
    packed_data: Arc<[u8]>,
    /// whether CRC check was successful
    pub crc_handle: Option<thread::JoinHandle<bool>>,
}

impl fmt::Debug for DictSectPFC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total size {}, {} strings, sequence {:?}, packed data {}",
            ByteSize(self.size_in_bytes() as u64),
            self.num_strings,
            self.sequence,
            ByteSize(self.packed_data.len() as u64)
        )
    }
}

///
#[derive(Error, Debug)]
pub enum ExtractError {
    #[error("index out of bounds: id {id} > dictionary section len {len}")]
    IdOutOfBounds { id: Id, len: usize },
    #[error("Read invalid UTF-8 sequence in {data:?}, recovered: '{recovered}'")]
    InvalidUtf8 { source: std::str::Utf8Error, data: Vec<u8>, recovered: String },
}

impl DictSectPFC {
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.packed_data.len()
    }

    /*
    // TODO: fix this
    fn decode(string: String) -> String {
        let mut split: Vec<String> = string.rsplit('"').map(String::from).collect();

        if split.len() > 2 {
            split = split.into_iter().skip(1).collect();
            split[0] = format!("\"{}\"", split[0]);
            split.into_iter().collect()
        } else {
            split[0].clone()
        }
    }
    */

    fn index_str(&self, index: usize) -> &str {
        let position: usize = self.sequence.get(index);
        let length = self.strlen(position);
        str::from_utf8(&self.packed_data[position..position + length]).unwrap()
    }

    // translated from Java
    // https://github.com/rdfhdt/hdt-java/blob/master/hdt-java-core/src/main/java/org/rdfhdt/hdt/dictionary/impl/section/PFCDictionarySection.java
    // 0 means not found
    pub fn string_to_id(&self, element: &str) -> Id {
        if self.num_strings == 0 {
            // shared dictionary may be empty
            return 0;
        }
        // binary search
        let mut low: usize = 0;
        let mut high = self.sequence.entries.saturating_sub(2); // should be -1 but only works with -2, investigate
        let max = high;
        let mut mid = high;
        while low <= high {
            mid = (low + high) / 2;

            let cmp: Ordering = if mid > max {
                mid = max;
                break;
            } else {
                let text = self.index_str(mid);
                element.cmp(text)
                //println!("mid: {} text: {} cmp: {:?}", mid, text, cmp);
            };
            match cmp {
                Ordering::Less => {
                    if mid == 0 {
                        return 0;
                    }
                    high = mid - 1;
                }
                Ordering::Greater => low = mid + 1,
                Ordering::Equal => return ((mid * self.block_size) + 1) as Id,
            }
        }
        if high < mid {
            mid = high;
        }
        let idblock = self.locate_in_block(mid, element);
        if idblock == 0 {
            return 0;
        }
        ((mid * self.block_size) + idblock + 1) as Id
    }

    fn longest_common_prefix(a: &[u8], b: &[u8]) -> usize {
        let len = min(a.len(), b.len());
        let mut delta = 0;
        while delta < len && a[delta] == b[delta] {
            delta += 1;
        }
        delta
    }

    fn pos_str(&self, pos: usize, slen: usize) -> &str {
        assert!(
            pos + slen < self.packed_data.len(),
            "Invalid arguments pos_str({pos},{slen}), packed data len {}).",
            self.packed_data.len()
        );
        if let Ok(s) = str::from_utf8(&self.packed_data[pos..pos + slen]) {
            s
        } else {
            error!(
                "invalid UTF8, skipping a byte {}",
                String::from_utf8_lossy(&self.packed_data[pos..pos + slen])
            );
            self.pos_str(pos + 1, slen)
        }
    }

    fn locate_in_block(&self, block: usize, element: &str) -> usize {
        if block >= self.sequence.entries {
            return 0;
        }

        let mut pos = self.sequence.get(block);
        let mut temp_string = String::new();

        //let mut delta: u64 = 0;
        let mut id_in_block = 0;
        let mut cshared = 0;

        // Read the first string in the block
        let slen = self.strlen(pos);
        temp_string.push_str(self.pos_str(pos, slen));
        pos += slen + 1;
        id_in_block += 1;

        while (id_in_block < self.block_size) && (pos < self.packed_data.len()) {
            // Decode prefix
            let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, pos);
            pos += vbyte_bytes;

            //Copy suffix
            let slen = self.strlen(pos);
            temp_string.truncate(temp_string.floor_char_boundary(delta));
            temp_string.push_str(self.pos_str(pos, slen));

            if delta >= cshared {
                // Current delta value means that this string has a larger long common prefix than the previous one
                let boundary = temp_string.floor_char_boundary(cshared);
                cshared += Self::longest_common_prefix(
                    temp_string[boundary..].as_bytes(),
                    element[boundary..].as_bytes(),
                );

                if (cshared == element.len()) && (temp_string.len() == element.len()) {
                    break;
                }
            } else {
                // We have less common characters than before, this string is bigger that what we are looking for.
                // i.e. Not found.
                id_in_block = 0;
                break;
            }
            pos += slen + 1;
            id_in_block += 1;
        }

        if pos >= self.packed_data.len() || id_in_block == self.block_size {
            id_in_block = 0;
        }
        id_in_block
    }

    /// extract the string with the given ID from the dictionary
    pub fn extract(&self, id: Id) -> Result<String, ExtractError> {
        if id as usize > self.num_strings {
            return Err(ExtractError::IdOutOfBounds { id, len: self.num_strings });
        }
        let block_index = id.saturating_sub(1) as usize / self.block_size;
        let string_index = id.saturating_sub(1) as usize % self.block_size;
        let mut position = self.sequence.get(block_index);
        let mut slen = self.strlen(position);
        let mut string: Vec<u8> = self.packed_data[position..position + slen].to_vec();
        //println!("block_index={} string_index={}, string={}", block_index, string_index, str::from_utf8(&string).unwrap());
        // loop takes around nearly half the time of the function
        for _ in 0..string_index {
            position += slen + 1;
            let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, position);
            position += vbyte_bytes;
            slen = self.strlen(position);
            string.truncate(delta);
            string.extend_from_slice(&self.packed_data[position..position + slen]);
        }
        // tried simdutf8::basic::from_utf8 but that didn't speed up extract that much
        match str::from_utf8(&string) {
            Ok(string) => Ok(String::from(string)),
            Err(e) => Err(ExtractError::InvalidUtf8 {
                source: e,
                data: string.clone(),
                recovered: String::from_utf8_lossy(&string).into_owned(),
            }),
        }
    }

    fn strlen(&self, offset: usize) -> usize {
        let length = self.packed_data.len();
        let mut position = offset;

        while position < length && self.packed_data[position] != 0 {
            position += 1;
        }

        position - offset
    }

    pub const fn num_strings(&self) -> usize {
        self.num_strings
    }

    pub fn read<R: BufRead>(reader: &mut R) -> io::Result<Self> {
        use io::Error;
        use io::ErrorKind::InvalidData;

        let mut preamble = [0_u8];
        reader.read_exact(&mut preamble)?;
        if preamble[0] != 2 {
            return Err(Error::new(
                InvalidData, "Implementation only supports plain front coded dictionary sections.",
            ));
        }

        // read section meta data
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest = crc.digest();
        // The CRC includes the type of the block, inaccuracy in the spec, careful.
        digest.update(&[0x02]);
        // This was determined based on https://git.io/JthMG because the spec on this
        // https://www.rdfhdt.org/hdt-binary-format was inaccurate, it's 3 vbytes, not 2.
        let (num_strings, bytes_read) = read_vbyte(reader)?;
        digest.update(&bytes_read);
        let (packed_length, bytes_read) = read_vbyte(reader)?;
        digest.update(&bytes_read);
        let (block_size, bytes_read) = read_vbyte(reader)?;
        digest.update(&bytes_read);

        // read section CRC8
        let mut crc_code = [0_u8];
        reader.read_exact(&mut crc_code)?;
        let crc_code = crc_code[0];

        // validate section CRC8
        if digest.finalize() != crc_code {
            return Err(Error::new(InvalidData, "Invalid CRC8-CCIT checksum"));
        }

        // read sequence log array
        let sequence = Sequence::read(reader)?;

        // read packed data
        let mut packed_data = vec![0u8; packed_length];
        reader.read_exact(&mut packed_data)?;
        let packed_data = Arc::<[u8]>::from(packed_data);

        // read packed data CRC32
        let mut crc_code = [0_u8; 4];
        reader.read_exact(&mut crc_code)?;
        let cloned_data = Arc::clone(&packed_data);
        let crc_handle = Some(thread::spawn(move || {
            let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
            let mut digest = crc.digest();
            digest.update(&cloned_data[..]);
            digest.finalize() == u32::from_le_bytes(crc_code)
        }));

        Ok(DictSectPFC { num_strings, block_size, sequence, packed_data, crc_handle })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::Header;
    use crate::tests::init;
    use crate::ControlInfo;
    use pretty_assertions::assert_eq;
    use std::fs::File;
    use std::io::BufReader;
    /* unused
    #[test]
    fn test_decode() {
        let s = String::from("^^<http://www.w3.org/2001/XMLSchema#integer>\"123\"");
        let d = DictSectPFC::decode(s);
        assert_eq!(d, "\"123\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    }
    */
    #[test]
    fn test_section_read() {
        init();
        let file = File::open("tests/resources/snikmeta.hdt").expect("error opening file");
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader).unwrap();
        Header::read(&mut reader).unwrap();

        // read dictionary control information
        let dict_ci = ControlInfo::read(&mut reader).unwrap();
        assert!(
            dict_ci.format == "<http://purl.org/HDT/hdt#dictionaryFour>",
            "invalid dictionary type: {:?}",
            dict_ci.format
        );

        let shared = DictSectPFC::read(&mut reader).unwrap();
        // the file contains IRIs that are used both as subject and object 23128
        assert_eq!(shared.num_strings, 43);
        assert_eq!(shared.packed_data.len(), 614);
        assert_eq!(shared.block_size, 16);
        for term in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta/Function", "_:b1"] {
            let id = shared.string_to_id(term);
            let back = shared.extract(id).unwrap();
            assert_eq!(term, back, "term does not translate back to itself {} -> {} -> {}", term, id, back);
        }
        let sequence = shared.sequence;
        let data_size = (sequence.bits_per_entry * sequence.entries + 63) / 64;
        assert_eq!(sequence.data.len(), data_size);

        let subjects = DictSectPFC::read(&mut reader).unwrap();
        assert_eq!(subjects.num_strings, 5);
        for term in [
            "http://www.snik.eu/ontology/meta", "http://www.snik.eu/ontology/meta/feature",
            "http://www.snik.eu/ontology/meta/homonym", "http://www.snik.eu/ontology/meta/master",
            "http://www.snik.eu/ontology/meta/typicalFeature",
        ] {
            let id = subjects.string_to_id(term);
            let back = subjects.extract(id).unwrap();
            assert_eq!(term, back, "term does not translate back to itself {} -> {} -> {}", term, id, back);
        }
        let sequence = subjects.sequence;
        let data_size = (sequence.bits_per_entry * sequence.entries + 63) / 64;
        assert_eq!(sequence.data.len(), data_size);
    }
}
