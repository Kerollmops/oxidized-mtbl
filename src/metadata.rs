use std::mem;

use byteorder::{LittleEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

use crate::compression::CompressionType;
use crate::error::Error;
use crate::FileVersion;
use crate::{METADATA_SIZE, DEFAULT_BLOCK_SIZE, DEFAULT_COMPRESSION_TYPE};
use crate::{MAGIC, MAGIC_V1};

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
        let magic = LittleEndian::read_u32(&bytes[METADATA_SIZE - mem::size_of::<u32>()..]);
        let file_version = match magic {
            MAGIC_V1 => FileVersion::FormatV1,
            MAGIC => FileVersion::FormatV2,
            _ => return Err(Error::InvalidFormatVersion),
        };

        let mut b = bytes;
        let index_block_offset = b.read_u64::<LittleEndian>().unwrap();
        let data_block_size = b.read_u64::<LittleEndian>().unwrap();
        let compression_algorithm = b.read_u64::<LittleEndian>().unwrap();
        let compression_algorithm = CompressionType::from_u64(compression_algorithm).ok_or(Error::InvalidCompressionAlgorithm)?;
        let count_entries = b.read_u64::<LittleEndian>().unwrap();
        let count_data_blocks = b.read_u64::<LittleEndian>().unwrap();
        let bytes_data_blocks = b.read_u64::<LittleEndian>().unwrap();
        let bytes_index_block = b.read_u64::<LittleEndian>().unwrap();
        let bytes_keys = b.read_u64::<LittleEndian>().unwrap();
        let bytes_values = b.read_u64::<LittleEndian>().unwrap();

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
        let (mut data, magic) = bytes.split_at_mut(METADATA_SIZE - mem::size_of::<u32>());

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
        LittleEndian::write_u32(magic, MAGIC)
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
