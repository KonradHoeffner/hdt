#![allow(missing_docs)] // temporariy while we figure out what should be public in the end
/// Dictionary section with plain front coding.
/// See <https://www.rdfhdt.org/hdt-binary-format/#DictionarySectionPlainFrontCoding>.
use crate::containers::vbyte::{decode_vbyte_delta, encode_vbyte, read_vbyte};
use crate::containers::{Sequence, sequence};
use crate::triples::Id;
use bytesize::ByteSize;
use log::error;
use std::cmp::{Ordering, min};
use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::sync::Arc;
use std::thread::{JoinHandle, spawn};
use std::{fmt, str};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

/// Dictionary section with plain front coding.
//#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub struct DictSectPFC {
    /// total number of strings stored
    pub num_strings: usize,
    /// the last block may have less than "block_size" strings
    pub block_size: usize,
    /// stores the starting position of each block
    pub sequence: Sequence,
    /// the substrings
    pub packed_data: Arc<[u8]>,
}

/// The error type for the DictSectPFC read function.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Invalid CRC8-CCIT checksum {0}, expected {1}")]
    InvalidCrc8Checksum(u8, u8),
    #[error("Invalid CRC32-C checksum {0}, expected {1}")]
    InvalidCrc32Checksum(u32, u32),
    #[error("implementation only supports plain front coded dictionary section type 2, found type {0}")]
    DictSectNotPfc(u8),
    #[error("sequence read error")]
    Sequence(#[from] sequence::Error),
}

impl fmt::Debug for DictSectPFC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "total size {}, {} strings, sequence {:?}, packed data {:?}",
            ByteSize(self.size_in_bytes() as u64),
            self.num_strings,
            self.sequence,
            self.packed_data //ByteSize(self.packed_data.len() as u64)
        )
    }
}

#[derive(Error, Debug)]
pub enum ExtractError {
    #[error("index out of bounds: id {id} > dictionary section len {len}")]
    IdOutOfBounds { id: Id, len: usize },
    #[error("read invalid UTF-8 sequence in {data:?}, recovered: '{recovered}'")]
    InvalidUtf8 { source: std::str::Utf8Error, data: Vec<u8>, recovered: String },
}

impl DictSectPFC {
    /// size in bytes of the dictionary section
    pub fn size_in_bytes(&self) -> usize {
        self.sequence.size_in_bytes() + self.packed_data.len()
    }

    fn index_str(&self, index: usize) -> &str {
        let position: usize = self.sequence.get(index);
        let length = self.strlen(position);
        str::from_utf8(&self.packed_data[position..position + length]).unwrap()
    }

    /// translated from Java
    /// <https://github.com/rdfhdt/hdt-java/blob/master/hdt-java-core/src/main/java/org/rdfhdt/hdt/dictionary/impl/section/PFCDictionarySection.java>>
    /// 0 means not found
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
            mid = usize::midpoint(low, high);

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

    fn locate_in_block(&self, block: usize, element: &str) -> usize {
        if block >= self.sequence.entries {
            return 0;
        }
        let element = element.as_bytes();
        let mut pos = self.sequence.get(block);
        let mut id_in_block = 0;
        let mut cshared = 0;

        // Read the first string in the block
        let slen = self.strlen(pos);
        let mut temp_string: Vec<u8> = self.packed_data[pos..pos + slen].to_vec();
        pos += slen + 1;
        id_in_block += 1;

        while (id_in_block < self.block_size) && (pos < self.packed_data.len()) {
            // Decode prefix
            let (delta, vbyte_bytes) = decode_vbyte_delta(&self.packed_data, pos);
            pos += vbyte_bytes;

            //Copy suffix
            let slen = self.strlen(pos);
            temp_string.truncate(delta);
            temp_string.extend_from_slice(&self.packed_data[pos..pos + slen]);
            if delta >= cshared {
                // Current delta value means that this string has a larger long common prefix than the previous one
                cshared += Self::longest_common_prefix(&temp_string[cshared..], &element[cshared..]);

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

    /// extract the string with the given ID between 1 and self.num_strings (inclusive) from the dictionary
    pub fn extract(&self, id: Id) -> core::result::Result<String, ExtractError> {
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

    /// deprecated: we should be able to remove this as it is public now
    pub const fn num_strings(&self) -> usize {
        self.num_strings
    }

    /// Returns an unverified dictionary section together with a handle to verify the checksum.
    pub fn read<R: BufRead>(reader: &mut R) -> Result<JoinHandle<Result<Self>>> {
        let mut preamble = [0_u8];
        reader.read_exact(&mut preamble)?;
        if preamble[0] != 2 {
            return Err(Error::DictSectNotPfc(preamble[0]));
        }

        // read section meta data
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest8 = crc8.digest();
        // The CRC includes the type of the block, inaccuracy in the spec, careful.
        digest8.update(&[0x02]);
        // This was determined based on https://git.io/JthMG because the spec on this
        // https://www.rdfhdt.org/hdt-binary-format was inaccurate, it's 3 vbytes, not 2.
        let (num_strings, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("num strings {num_strings}");
        let (packed_length, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("packed_length {packed_length}");
        let (block_size, bytes_read) = read_vbyte(reader)?;
        digest8.update(&bytes_read);
        //println!("block_size {block_size}");
        // read section CRC8
        let mut crc_code8 = [0_u8];
        reader.read_exact(&mut crc_code8)?;
        let crc_code8 = crc_code8[0];
        //println!("crc_code {crc_code8}");

        let crc_calculated8 = digest8.finalize();
        if crc_calculated8 != crc_code8 {
            return Err(Error::InvalidCrc8Checksum(crc_calculated8, crc_code8));
        }

        // read sequence log array
        let sequence = Sequence::read(reader)?;
        //println!("read sequence of length {} {:?}", sequence.data.len(), sequence.data);

        // read packed data
        let mut packed_data = vec![0u8; packed_length];
        reader.read_exact(&mut packed_data)?;
        let packed_data = Arc::<[u8]>::from(packed_data);
        //println!("read packed data of length {} {:?}", packed_data.len(), packed_data);

        // read packed data CRC32
        let mut crc_code = [0u8; 4];
        reader.read_exact(&mut crc_code)?;
        let cloned_data = Arc::clone(&packed_data);
        Ok(spawn(move || {
            let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
            let mut digest32 = crc32.digest();
            digest32.update(&cloned_data[..]);
            let crc_calculated32 = digest32.finalize();
            let crc_code32 = u32::from_le_bytes(crc_code);
            if crc_calculated32 != crc_code32 {
                return Err(Error::InvalidCrc32Checksum(crc_calculated32, crc_code32));
            }
            Ok(DictSectPFC { num_strings, block_size, sequence, packed_data })
        }))
    }

    /// counterpoint to the read method
    pub fn write(&self, dest_writer: &mut impl Write) -> Result<()> {
        let crc8 = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut digest8 = crc8.digest();
        // libhdt/src/libdcs/CSD_PFC.cpp::save()
        // save type
        let seq_type: [u8; 1] = [2];
        dest_writer.write_all(&seq_type)?;
        digest8.update(&seq_type);

        // // Save sizes
        let mut buf: Vec<u8> = vec![];
        buf.extend_from_slice(&encode_vbyte(self.num_strings));
        buf.extend_from_slice(&encode_vbyte(self.packed_data.len()));
        buf.extend_from_slice(&encode_vbyte(self.block_size));
        dest_writer.write_all(&buf)?;
        digest8.update(&buf);
        let checksum8: u8 = digest8.finalize();
        dest_writer.write_all(&[checksum8])?;

        self.sequence.write(dest_writer)?;

        // Write packed data
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut digest32 = crc32.digest();
        dest_writer.write_all(&self.packed_data)?;
        digest32.update(&self.packed_data);
        // println!("{}", String::from_utf8_lossy(&self.compressed_terms));
        let checksum32 = digest32.finalize();
        let checksum_bytes: [u8; 4] = checksum32.to_le_bytes();
        //println!("write crc32 {checksum_bytes:?}");
        dest_writer.write_all(&checksum_bytes)?;
        dest_writer.flush()?;
        Ok(())
    }

    /// sorted and unique terms
    pub fn compress(terms: &BTreeSet<&str>, block_size: usize) -> Self {
        let mut compressed_terms = Vec::new();
        let mut offsets = Vec::new();
        let mut last_term: &[u8] = &[];

        let num_terms = terms.len();
        for (i, term) in terms.iter().enumerate() {
            let term = term.as_bytes();
            if i % block_size == 0 {
                offsets.push(compressed_terms.len());
                compressed_terms.extend_from_slice(term);
            } else {
                let common_prefix_len = last_term.iter().zip(term).take_while(|(a, b)| a == b).count();
                compressed_terms.extend_from_slice(&encode_vbyte(common_prefix_len));
                compressed_terms.extend_from_slice(&term[common_prefix_len..]);
            }

            compressed_terms.push(0); // Null separator
            last_term = term;
        }
        if num_terms > 0 {
            offsets.push(compressed_terms.len());
        }

        // offsets are an increasing list of array indices, therefore the last one will be the largest
        // TODO: potential off by 1 in comparison with hdt-cpp implementation?
        let bits_per_entry = if num_terms == 0 { 0 } else { (offsets.last().unwrap().ilog2() + 1) as usize };
        DictSectPFC {
            num_strings: num_terms,
            block_size,
            sequence: Sequence::new(&offsets, bits_per_entry),
            packed_data: Arc::from(compressed_terms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ControlInfo;
    use crate::hdt::tests::snikmeta;
    use crate::header::Header;
    use crate::tests::init;
    use fs_err::File;
    use pretty_assertions::assert_eq;
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
    fn read_section_read() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;

        // read dictionary control information
        let dict_ci = ControlInfo::read(&mut reader)?;
        assert!(
            dict_ci.format == "<http://purl.org/HDT/hdt#dictionaryFour>",
            "invalid dictionary type: {:?}",
            dict_ci.format
        );

        let shared = DictSectPFC::read(&mut reader)?.join().unwrap()?;
        // the file contains IRIs that are used both as subject and object 23128
        assert_eq!(shared.num_strings, 43);
        assert_eq!(shared.packed_data.len(), 614);
        assert_eq!(shared.block_size, 16);
        for term in ["http://www.snik.eu/ontology/meta/Top", "http://www.snik.eu/ontology/meta/Function", "_:b1"] {
            let id = shared.string_to_id(term);
            let back = shared.extract(id)?;
            assert_eq!(term, back, "term does not translate back to itself {} -> {} -> {}", term, id, back);
        }
        let sequence = shared.sequence;
        let data_size = (sequence.bits_per_entry * sequence.entries).div_ceil(64);
        assert_eq!(sequence.data.len(), data_size);

        let subjects = DictSectPFC::read(&mut reader)?.join().unwrap()?;
        assert_eq!(subjects.num_strings, 6);
        for term in [
            "http://www.snik.eu/ontology/meta", "http://www.snik.eu/ontology/meta/feature",
            "http://www.snik.eu/ontology/meta/homonym", "http://www.snik.eu/ontology/meta/master",
            "http://www.snik.eu/ontology/meta/typicalFeature",
        ] {
            let id = subjects.string_to_id(term);
            let back = subjects.extract(id)?;
            assert_eq!(term, back, "term does not translate back to itself {} -> {} -> {}", term, id, back);
        }
        let sequence = subjects.sequence;
        let data_size = (sequence.bits_per_entry * sequence.entries).div_ceil(64);
        assert_eq!(sequence.data.len(), data_size);
        Ok(())
    }

    #[test]
    fn write() -> color_eyre::Result<()> {
        init();
        let file = File::open("tests/resources/snikmeta.hdt")?;
        let mut reader = BufReader::new(file);
        ControlInfo::read(&mut reader)?;
        Header::read(&mut reader)?;
        let _ = ControlInfo::read(&mut reader)?;
        let shared = DictSectPFC::read(&mut reader)?.join().unwrap()?;
        assert_eq!(shared.num_strings, 43);
        assert_eq!(shared.packed_data.len(), 614);
        assert_eq!(shared.block_size, 16);

        let subjects = DictSectPFC::read(&mut reader)?.join().unwrap()?;
        let predicates = DictSectPFC::read(&mut reader)?.join().unwrap()?;
        let objects = DictSectPFC::read(&mut reader)?.join().unwrap()?;

        for sect in [shared, subjects, predicates, objects] {
            let mut buf = Vec::<u8>::new();
            sect.write(&mut buf)?;
            let mut cursor = std::io::Cursor::new(buf);
            let sect2 = DictSectPFC::read(&mut cursor)?.join().unwrap()?;
            assert_eq!(sect.num_strings, sect2.num_strings);
            assert_eq!(sect.sequence, sect2.sequence);
            assert_eq!(sect.packed_data.len(), sect2.packed_data.len());
            assert_eq!(sect.block_size, sect2.block_size);
            assert_eq!(sect.packed_data, sect2.packed_data);
        }
        Ok(())
    }

    #[test]
    fn compress() -> color_eyre::Result<()> {
        const BLOCK_SIZE: usize = 16;
        init();
        // stand-alone small test
        let strings = [
            "http://www.snik.eu/ontology/meta", "http://www.snik.eu/ontology/meta/feature",
            "http://www.snik.eu/ontology/meta/homonym", "http://www.snik.eu/ontology/meta/master",
            "http://www.snik.eu/ontology/meta/typicalFeature", "http://www.snik.eu/ontology/meta/хобби-N-0",
        ];
        let string_vec = Vec::from(strings);
        let set: BTreeSet<&str> = BTreeSet::from(strings);
        let dict = DictSectPFC::compress(&set, BLOCK_SIZE);
        // could add this as DictSectPFC::items if required elsewhere
        let sect_items =
            |ds: &DictSectPFC| -> Vec<String> { (1..=ds.num_strings()).map(|i| ds.extract(i).unwrap()).collect() };
        //let items: Vec<String> = (1..dict.num_strings() + 1).map(|i| dict.extract(i).unwrap()).collect();
        let items = sect_items(&dict);
        assert_eq!(string_vec, items);

        // large test that relies on HDT reading and involved components working correctly
        let hdt = snikmeta()?;
        let dict = hdt.dict;
        let names = ["shared", "subject", "predicate", "object"];
        let sects = [dict.shared, dict.subjects, dict.predicates, dict.objects];
        for (sect, name) in sects.iter().zip(names) {
            let items1 = sect_items(sect);
            let set1: BTreeSet<&str> = items1.iter().map(std::ops::Deref::deref).collect();
            let sect2 = DictSectPFC::compress(&set1, BLOCK_SIZE);
            let items2 = sect_items(&sect2);
            assert_eq!(items1, items2, "error compressing {name} section");
        }
        assert_eq!(0, DictSectPFC::compress(&BTreeSet::new(), BLOCK_SIZE).num_strings);
        Ok(())
    }
}
