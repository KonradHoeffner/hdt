// Copyright (c) 2024-2025, Decisym, LLC

use crate::{
    containers::{self, ControlType, vbyte::encode_vbyte},
    rdf2hdt::{
        common::{byte_align_bitmap, save_u32_vec},
        dictionary::EncodedTripleId,
        vocab::HDT_TYPE_BITMAP,
    },
    triples::Order,
};
use log::debug;
use std::{
    cmp::Ordering,
    error::Error,
    fs::File,
    io::{BufWriter, Write},
};

#[derive(Default, Debug)]
pub struct BitmapTriplesBuilder {
    y_vec: Vec<u32>,
    z_vec: Vec<u32>,
    bitmap_y: Vec<bool>,
    bitmap_z: Vec<bool>,
    pub order: Order,
    num_triples: usize,
}

impl BitmapTriplesBuilder {
    /// Creates a new BitmapTriples from a list of sorted RDF triples
    pub fn load(mut triples: Vec<EncodedTripleId>) -> Result<Self, Box<dyn Error>> {
        // libhdt/src/triples/BitmapTriples.cpp:load()
        let timer = std::time::Instant::now();

        sort_triples_spo(&mut triples);

        let mut y_bitmap = Vec::new();
        let mut z_bitmap = Vec::new();
        let mut array_y = Vec::new();
        let mut array_z = Vec::new();

        let mut last_x: u32 = 0;
        let mut last_y: u32 = 0;
        let mut last_z: u32 = 0;
        for (i, triple) in triples.iter().enumerate() {
            let x = triple.subject;
            let y = triple.predicate;
            let z = triple.object;

            if x == 0 || y == 0 || z == 0 {
                panic!("triple IDs should never be zero")
            }

            if i == 0 {
                array_y.push(y);
                array_z.push(z);
            } else if x != last_x {
                if x != last_x + 1 {
                    panic!("the subjects must be correlative.")
                }

                //x unchanged
                y_bitmap.push(true);
                array_y.push(y);

                z_bitmap.push(true);
                array_z.push(z);
            } else if y != last_y {
                if y < last_y {
                    panic!("the predicates must be in increasing order.")
                }

                // y unchanged
                y_bitmap.push(false);
                array_y.push(y);

                z_bitmap.push(true);
                array_z.push(z);
            } else {
                if z < last_z {
                    panic!("the objects must be in increasing order")
                }

                // z changed
                z_bitmap.push(false);
                array_z.push(z);
            }

            last_x = x;
            last_y = y;
            last_z = z;
        }

        y_bitmap.push(true);
        z_bitmap.push(true);
        debug!("BitmapTriples build time: {:?}", timer.elapsed());

        Ok(BitmapTriplesBuilder {
            bitmap_y: y_bitmap,
            bitmap_z: z_bitmap,
            y_vec: array_y,
            z_vec: array_z,
            order: Order::SPO,
            num_triples: triples.len(),
        })
    }

    pub fn save(&self, dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        let mut ci = containers::ControlInfo {
            control_type: ControlType::Triples,
            format: HDT_TYPE_BITMAP.to_string(),
            ..Default::default()
        };
        ci.properties.insert("order".to_string(), (self.order.clone() as u8).to_string());
        ci.save(dest_writer)?;
        self.save_bitmap(&self.bitmap_y, dest_writer)?;

        // bitmapZ->save(output);
        self.save_bitmap(&self.bitmap_z, dest_writer)?;

        let num_bits = if self.num_triples == 0 { 0 } else { self.num_triples.ilog2() + 1 };
        if num_bits > u8::MAX as u32 {
            panic!("bits_per_entry too large")
        }
        // arrayY->save(output);
        save_u32_vec(&self.y_vec, dest_writer, num_bits.try_into().unwrap())?;
        // // libhdt/src/sequence/LogSequence2.cpp::save()
        save_u32_vec(&self.z_vec, dest_writer, num_bits.try_into().unwrap())?;

        Ok(())
    }

    fn save_bitmap(&self, v: &[bool], dest_writer: &mut BufWriter<File>) -> Result<(), Box<dyn Error>> {
        // libhdt/src/bitsequence/BitSequence375.cpp::save()
        let crc = crc::Crc::<u8>::new(&crc::CRC_8_SMBUS);
        let mut hasher = crc.digest();
        // type
        let bitmap_type: [u8; 1] = [1];
        let _ = dest_writer.write(&bitmap_type)?;
        hasher.update(&bitmap_type);
        // number of bits
        let t = encode_vbyte(v.len());
        let _ = dest_writer.write(&t)?;
        hasher.update(&t);
        // crc8 checksum
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;

        // write data
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let mut hasher = crc.digest();
        let buf = byte_align_bitmap(v);
        let _ = dest_writer.write(&buf)?;
        hasher.update(&buf);
        let checksum = hasher.finalize();
        let _ = dest_writer.write(&checksum.to_le_bytes())?;
        Ok(())
    }
}

/// Function to sort a vector of Triples in SPO order
fn sort_triples_spo(triples: &mut [EncodedTripleId]) {
    triples.sort_by(spo_comparator);
}

fn spo_comparator(a: &EncodedTripleId, b: &EncodedTripleId) -> Ordering {
    let subject_order = a.subject.cmp(&b.subject);
    if subject_order != Ordering::Equal {
        return subject_order;
    }

    let predicate_order = a.predicate.cmp(&b.predicate);
    if predicate_order != Ordering::Equal {
        return predicate_order;
    }

    a.object.cmp(&b.object)
}
