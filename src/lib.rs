use std::mem;

use byteorder::{ByteOrder, LittleEndian};

use compression::CompressionType;
use crate::writer::DEFAULT_BLOCK_SIZE;
use crate::writer::DEFAULT_COMPRESSION_TYPE;
use error::Error;

pub use self::writer::{Writer, WriterOptions};
pub use self::reader::{Reader, ReaderOptions, ReaderGet, ReaderIter};

mod block;
mod block_builder;
mod compression;
mod error;
mod reader;
mod varint;
mod writer;

const MTBL_MAGIC_V1: u32 = 0x77846676;
const MTBL_MAGIC: u32 = 0x4D54424C;
const MTBL_METADATA_SIZE: usize = 512;

fn bytes_compare(a: &[u8], b: &[u8]) -> i32 {
    use std::cmp::Ordering;
    match a.cmp(&b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u32)]
pub enum FileVersion {
    FormatV1 = 0,
    FormatV2 = 1,
}

impl CompressionType {
    fn from_u64(value: u64) -> Option<CompressionType> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Snappy),
            2 => Some(CompressionType::Zlib),
            3 => Some(CompressionType::Lz4),
            4 => Some(CompressionType::Lz4hc),
            5 => Some(CompressionType::Zstd),
            _ => None,
        }
    }
}

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
    fn read_from_bytes(bytes: &[u8]) -> Result<Metadata, Error> {
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

    pub fn as_bytes<'a>(&'a self) -> &'a [u8] {
        let ptr = self as *const _ as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of::<Self>()) }
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
