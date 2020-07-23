#[cfg(test)]
#[macro_use] extern crate quickcheck;

const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
const DEFAULT_BLOCK_SIZE: u64 = 8192;
const DEFAULT_COMPRESSION_LEVEL: u32 = 0;
const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::None;
const METADATA_SIZE: usize = 512;
const MIN_BLOCK_SIZE: u64 = 1024;
const MAGIC: u32 = 0x4D54424C;
const MAGIC_V1: u32 = 0x77846676;

pub use error::Error;
pub use compression::CompressionType;
pub use self::metadata::Metadata;
pub use self::reader::{Reader, ReaderOptions, ReaderGet, ReaderIter};
pub use self::writer::{Writer, WriterOptions};
pub use self::merger::{Merger, MergerOptions, MergerIter};

mod block;
mod block_builder;
mod compression;
mod error;
mod merger;
mod metadata;
mod reader;
mod varint;
mod writer;

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
