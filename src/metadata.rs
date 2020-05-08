use std::mem;

use byteorder::{LittleEndian, ByteOrder, WriteBytesExt};

use crate::compression::CompressionType;
use crate::error::Error;
use crate::FileVersion;
use crate::writer::DEFAULT_BLOCK_SIZE;
use crate::writer::DEFAULT_COMPRESSION_TYPE;

const MTBL_MAGIC_V1: u32 = 0x77846676;
const MTBL_MAGIC: u32 = 0x4D54424C;
pub const MTBL_METADATA_SIZE: usize = 512;

#[derive(Debug)]
#[repr(C)]
pub struct Metadata {
    pub file_version: FileVersion,
    pub index_block_offset: u64,
    pub data_block_size: u64,
    pub compression_algorithm: CompressionType,
    pub count_entries: u64,
    pub count_data_blocks: u64,
    pub bytes_data_blocks: u64,
    pub bytes_index_block: u64,
    pub bytes_keys: u64,
    pub bytes_values: u64,
}

impl Metadata {
    pub(crate) fn read_from_bytes(bytes: &[u8]) -> Result<Metadata, Error> {
        let magic = LittleEndian::read_u32(&bytes[MTBL_METADATA_SIZE - mem::size_of::<u32>()..]);
        let file_version = match magic {
            MTBL_MAGIC_V1 => FileVersion::FormatV1,
            MTBL_MAGIC => FileVersion::FormatV2,
            _ => return Err(Error::InvalidFormatVersion),
        };

        let mut b = bytes;
        let index_block_offset = LittleEndian::read_u64(b); b = &b[8..];
        let data_block_size = LittleEndian::read_u64(b); b = &b[8..];
        let compression_algorithm = LittleEndian::read_u64(b); b = &b[8..];
        let compression_algorithm = CompressionType::from_u64(compression_algorithm).ok_or(Error::InvalidCompressionAlgorithm)?;
        let count_entries = LittleEndian::read_u64(b); b = &b[8..];
        let count_data_blocks = LittleEndian::read_u64(b); b = &b[8..];
        let bytes_data_blocks = LittleEndian::read_u64(b); b = &b[8..];
        let bytes_index_block = LittleEndian::read_u64(b); b = &b[8..];
        let bytes_keys = LittleEndian::read_u64(b); b = &b[8..];
        let bytes_values = LittleEndian::read_u64(b);

        Ok(Metadata {
            file_version,
            index_block_offset,
            data_block_size,
            compression_algorithm,
            count_entries,
            count_data_blocks,
            bytes_data_blocks,
            bytes_index_block,
            bytes_keys,
            bytes_values,
        })
    }

    pub(crate) fn write_to_bytes(&self, bytes: &mut [u8]) {
        bytes.iter_mut().for_each(|x| *x = 0);

        // split, left part for data, right part for magic number
        let (mut data, magic) = bytes.split_at_mut(MTBL_METADATA_SIZE - mem::size_of::<u32>());

        data.write_u64::<LittleEndian>(self.index_block_offset).unwrap();
        data.write_u64::<LittleEndian>(self.data_block_size).unwrap();
        data.write_u64::<LittleEndian>(self.compression_algorithm as u64).unwrap();
        data.write_u64::<LittleEndian>(self.count_entries).unwrap();
        data.write_u64::<LittleEndian>(self.count_data_blocks).unwrap();
        data.write_u64::<LittleEndian>(self.bytes_data_blocks).unwrap();
        data.write_u64::<LittleEndian>(self.bytes_index_block).unwrap();
        data.write_u64::<LittleEndian>(self.bytes_keys).unwrap();
        data.write_u64::<LittleEndian>(self.bytes_values).unwrap();

        // Write the magic number at the end of the buffer
        LittleEndian::write_u32(magic, MTBL_MAGIC)
    }
}

impl Default for Metadata {
    fn default() -> Metadata {
        Metadata {
            file_version: FileVersion::FormatV2,
            index_block_offset: 0,
            data_block_size: DEFAULT_BLOCK_SIZE,
            compression_algorithm: DEFAULT_COMPRESSION_TYPE,
            count_entries: 0,
            count_data_blocks: 0,
            bytes_data_blocks: 0,
            bytes_index_block: 0,
            bytes_keys: 0,
            bytes_values: 0,
        }
    }
}
