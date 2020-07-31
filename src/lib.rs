#[cfg(test)]
#[macro_use] extern crate quickcheck;

const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
const DEFAULT_BLOCK_SIZE: u64 = 8192;
const MIN_BLOCK_SIZE: u64 = 1024;

const DEFAULT_COMPRESSION_LEVEL: u32 = 0;
const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::None;

const DEFAULT_SORTER_MEMORY: usize = 1_073_741_824; // 1GB
const MIN_SORTER_MEMORY: usize = 10_485_760; // 10MB
const INITIAL_SORTER_VEC_SIZE: usize = 131_072; // 128KB

const METADATA_SIZE: usize = 512;

const MAGIC: u32 = 0x4D54424C;
const MAGIC_V1: u32 = 0x77846676;

use std::sync::Arc;

pub use error::Error;
pub use compression::CompressionType;
pub use self::metadata::Metadata;
pub use self::reader::{Reader, ReaderBuilder, ReaderIntoGet, ReaderIntoIter};
pub use self::writer::{Writer, WriterBuilder};
pub use self::merger::{Merger, MergerOptions, MergerIter};
pub use self::sorter::{Sorter, SorterBuilder};

mod block;
mod block_builder;
mod compression;
mod error;
mod merger;
mod metadata;
mod reader;
mod sorter;
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

#[derive(Clone)]
pub struct BytesView<A: ?Sized> {
    inner: InnerBytesView<A>,
    offset: usize,
    length: usize,
}

enum InnerBytesView<A: ?Sized> {
    Bytes(Arc<[u8]>),
    Data(Arc<A>),
}

impl<A: AsRef<[u8]>> AsRef<[u8]> for InnerBytesView<A> {
    fn as_ref(&self) -> &[u8] {
        match self {
            InnerBytesView::Bytes(bytes) => bytes.as_ref(),
            InnerBytesView::Data(data) => (**data).as_ref(),
        }
    }
}

impl<A> BytesView<A> {
    fn new(data: A, offset: usize, length: usize) -> Self {
        let inner = InnerBytesView::Data(Arc::new(data));
        BytesView { inner, offset, length }
    }

    fn from_bytes(bytes: Vec<u8>) -> Self {
        let length = bytes.len();
        let inner = InnerBytesView::Bytes(Arc::from(bytes));
        BytesView { inner, offset: 0, length }
    }

    fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(offset + length <= self.length);
        BytesView {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            length,
        }
    }

    fn len(&self) -> usize {
        self.length
    }
}

impl<A> Clone for InnerBytesView<A> {
    fn clone(&self) -> InnerBytesView<A> {
        match self {
            InnerBytesView::Bytes(bytes) => InnerBytesView::Bytes(bytes.clone()),
            InnerBytesView::Data(data) => InnerBytesView::Data(data.clone()),
        }
    }
}

impl<A: AsRef<[u8]>> From<A> for BytesView<A> {
    fn from(data: A) -> BytesView<A> {
        let length = data.as_ref().len();
        let inner = InnerBytesView::Data(Arc::new(data));
        BytesView { inner, offset: 0, length }
    }
}

impl<A: AsRef<[u8]>> AsRef<[u8]> for BytesView<A> {
    fn as_ref(&self) -> &[u8] {
        let slice = self.inner.as_ref();
        &slice[self.offset..self.offset + self.length]
    }
}
