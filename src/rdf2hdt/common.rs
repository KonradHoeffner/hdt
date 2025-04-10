// Copyright (c) 2024-2025, Decisym, LLC

use std::{
    error::Error,
    fs::File,
    io::{BufWriter, Write},
};

use crate::containers::vbyte::encode_vbyte;

pub fn save_u32_vec(ints: &[u32], dest_writer: &mut BufWriter<File>, num_bits: u8) -> Result<(), Box<dyn Error>> {
    let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
    let mut hasher = crc.digest();
    // libhdt/src/sequence/LogSequence2.cpp::save()
    // Write offsets using variable-length encoding
    let seq_type: [u8; 1] = [1];
    let _ = dest_writer.write(&seq_type)?;
    hasher.update(&seq_type);
    // Write numbits
    let bits_per_entry: [u8; 1] = [num_bits];
    let _ = dest_writer.write(&bits_per_entry)?;
    hasher.update(&bits_per_entry);
    // Write numentries
    let buf = &encode_vbyte(ints.len());
    let _ = dest_writer.write(buf)?;
    hasher.update(buf);
    let checksum = hasher.finalize();
    let _ = dest_writer.write(&checksum.to_le_bytes())?;

    // Write data
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
    let mut hasher = crc.digest();
    let offset_data = pack_bits(ints, num_bits);
    let _ = dest_writer.write(&offset_data)?;
    hasher.update(&offset_data);
    let checksum = hasher.finalize();
    let _ = dest_writer.write(&checksum.to_le_bytes())?;

    Ok(())
}

// TODO duplicate of containers/sequence.rs::save()
fn pack_bits(data: &[u32], bits_per_entry: u8) -> Vec<u8> {
    assert!(bits_per_entry > 0 && bits_per_entry as usize <= std::mem::size_of::<usize>() * 8);

    let mut output = Vec::new();
    let mut current_byte = 0u8;
    let mut bit_offset = 0;

    for &value in data {
        let mut val = value & ((1 << bits_per_entry) - 1); // mask to get only relevant bits
        let mut bits_left = bits_per_entry;

        while bits_left > 0 {
            let available = 8 - bit_offset;
            let to_write = bits_left.min(available);

            // Shift bits to align with current byte offset
            current_byte |= ((val & ((1 << to_write) - 1)) as u8) << bit_offset;

            bit_offset += to_write;
            val >>= to_write;
            bits_left -= to_write;

            if bit_offset == 8 {
                output.push(current_byte);
                current_byte = 0;
                bit_offset = 0;
            }
        }
    }

    // Push final byte if there's remaining bits
    if bit_offset > 0 {
        output.push(current_byte);
    }

    output
}

pub fn byte_align_bitmap(bits: &[bool]) -> Vec<u8> {
    let mut byte = 0u8;
    let mut bit_index = 0;
    let mut byte_vec = Vec::new();

    for &bit in bits {
        if bit {
            byte |= 1 << bit_index;
        }
        bit_index += 1;

        if bit_index == 8 {
            byte_vec.push(byte);
            byte = 0;
            bit_index = 0;
        }
    }

    // If remaining bits exist, pad the last byte
    if bit_index > 0 {
        byte_vec.push(byte);
    }
    byte_vec
}
