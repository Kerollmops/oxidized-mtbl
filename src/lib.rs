use std::borrow::Cow;
use std::mem;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use error::Error;
use block::{Block, BlockIter};
use varint::varint_decode64;
use compression::{CompressionType, decompress};

mod error;
mod block;
mod compression;
mod varint;

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
}

enum ReaderIterType {
    Iter,
    Get,
    GetPrefix,
    GetRange,
}

#[derive(Default, Copy, Clone)]
pub struct ReaderOptions {
    pub verify_checksums: bool,
    pub madvise_random: bool,
}

pub struct Reader<'a> {
    metadata: Metadata,
    data: &'a [u8],
    _opt: ReaderOptions,
    index: Arc<Block<'a>>,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8], _opt: ReaderOptions) -> Result<Reader<'a>, Error> {
        if data.len() < MTBL_METADATA_SIZE {
            return Err(Error::InvalidMetadataSize)
        }

        let metadata_offset = data.len() - MTBL_METADATA_SIZE;
        let metadata_bytes = &data[metadata_offset..metadata_offset + MTBL_METADATA_SIZE];
        let metadata = Metadata::read_from_bytes(metadata_bytes)?;

        // Sanitize the index block offset.
        // We calculate the maximum possible index block offset for this file to
        // be the total size of the file (r->len_data) minus the length of the
        // metadata block (MTBL_METADATA_SIZE) minus the length of the minimum
        // sized block, which requires 4 fixed-length 32-bit integers (16 bytes).
        let max_index_block_offset = (data.len() - MTBL_METADATA_SIZE - 16) as u64;
        if metadata.index_block_offset > max_index_block_offset {
            return Err(Error::InvalidIndexBlockOffset);
        }

        // reader_init_madvise(r);

        let index_len_len: usize;
        let index_len: usize;

        if metadata.file_version == FileVersion::FormatV1 {
            index_len_len = mem::size_of::<u32>();
            index_len = LittleEndian::read_u32(&data[metadata.index_block_offset as usize..]) as usize;
        } else {
            let mut tmp = 0;
            index_len_len = varint_decode64(&data[metadata.index_block_offset as usize..], &mut tmp);
            index_len = tmp as usize;
            if index_len as u64 != tmp {
                return Err(Error::InvalidIndexLength);
            }
        }

        let index_data = &data[metadata.index_block_offset as usize + index_len_len + mem::size_of::<u32>()..];
        let index_data = &index_data[..index_len];

        #[cfg(feature = "checksum")] {
        if _opt.verify_checksums {
            let index_crc = LittleEndian::read_u32(&data[metadata.index_block_offset as usize + index_len_len..]);
            assert_eq!(index_crc, crc32c::crc32c(index_data));
        } }

        let index = Block::init(Cow::Borrowed(index_data));
        let index = Arc::new(index);

        Ok(Reader { metadata, data, _opt, index })
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn get<'r>(&'r self, key: &[u8]) -> Result<ReaderGet<'a>, ()> {
        let mut iter = ReaderIter::new_get(self, key)?;
        iter.next().ok_or(())?;
        Ok(ReaderGet::new(iter.bi))
    }

    pub fn iter<'r>(&'r self) -> Result<ReaderIter<'r, 'a>, ()> {
        ReaderIter::new(self)
    }

    pub fn iter_from<'r>(&'r self, start: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        ReaderIter::new_from(self, start)
    }

    pub fn iter_prefix<'r>(&'r self, prefix: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        ReaderIter::new_get_prefix(self, prefix)
    }

    pub fn iter_range<'r>(&'r self, start: &[u8], end: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        ReaderIter::new_get_range(self, start, end)
    }

    fn block(&self, offset: usize) -> Block<'a> {
        assert!(offset < self.data.len());

        let raw_contents_size_len: usize;
        let raw_contents_size: usize;

        if self.metadata.file_version == FileVersion::FormatV1 {
            raw_contents_size_len = mem::size_of::<u32>();
            raw_contents_size = LittleEndian::read_u32(&self.data[offset..]) as usize;
        } else {
            let mut tmp = 0;
            raw_contents_size_len = varint_decode64(&self.data[offset..], &mut tmp);
            raw_contents_size = tmp as usize;
            assert_eq!(raw_contents_size as u64, tmp);
        }
        let raw_contents = &self.data[offset + raw_contents_size_len + mem::size_of::<u32>()..];
        let raw_contents = &raw_contents[..raw_contents_size];

        #[cfg(feature = "checksum")] {
        if self._opt.verify_checksums {
            let block_crc = LittleEndian::read_u32(&self.data[offset + raw_contents_size_len..]);
            let calc_crc = crc32c::crc32c(raw_contents);
            assert_eq!(block_crc, calc_crc);
        } }

        let data = decompress(self.metadata.compression_algorithm, raw_contents).unwrap();
        Block::init(data)
    }

    fn block_at_index<'r>(&self, index_iter: &BlockIter<'a>) -> Result<Block<'a>, ()> {
        match index_iter.get() {
            Some((_key, val)) => {
                let mut offset = 0;
                varint_decode64(val, &mut offset);
                Ok(self.block(offset as usize))
            },
            None => Err(()),
        }
    }
}

pub struct ReaderGet<'a> {
    block: Arc<Block<'a>>,
    val_offset: usize,
    val_len: usize,
}

impl<'a> ReaderGet<'a> {
    fn new(block_iter: BlockIter<'a>) -> ReaderGet<'a> {
        let (offset, length) = block_iter.val.unwrap();
        ReaderGet {
            block: block_iter.block,
            val_offset: offset,
            val_len: length,
        }
    }
}

impl AsRef<[u8]> for ReaderGet<'_> {
    fn as_ref(&self) -> &[u8] {
        &(*self.block).as_ref()[self.val_offset..self.val_offset + self.val_len]
    }
}

pub struct ReaderIter<'r, 'a> {
    r: &'r Reader<'a>,
    block_offset: u64,
    bi: BlockIter<'a>,
    index_iter: BlockIter<'a>,
    k: Vec<u8>,
    first: bool,
    valid: bool,
    it_type: ReaderIterType,
}

impl<'r, 'a> ReaderIter<'r, 'a> {
    fn new(r: &'r Reader<'a>) -> Result<ReaderIter<'r, 'a>, ()> {
        let mut index_iter = BlockIter::init(r.index.clone());
        index_iter.seek_to_first();

        let b = r.block_at_index(&index_iter)?;
        let mut bi = BlockIter::init(Arc::new(b));
        bi.seek_to_first();

        Ok(ReaderIter {
            r,
            block_offset: 0,
            bi,
            index_iter,
            k: Vec::new(),
            first: true,
            valid: true,
            it_type: ReaderIterType::Iter,
        })
    }

    fn new_from(r: &'r Reader<'a>, key: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        let mut index_iter = BlockIter::init(r.index.clone());
        index_iter.seek(key);

        let b = r.block_at_index(&index_iter)?;
        let mut bi = BlockIter::init(Arc::new(b));

        bi.seek(key);

        Ok(ReaderIter {
            r,
            block_offset: 0,
            bi,
            index_iter,
            k: Vec::new(),
            first: true,
            valid: true,
            it_type: ReaderIterType::Iter,
        })
    }

    fn new_get(r: &'r Reader<'a>, key: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        let mut iter = ReaderIter::new_from(r, key)?;
        iter.k.extend_from_slice(key);
        iter.it_type = ReaderIterType::Get;
        Ok(iter)
    }

    fn new_get_prefix(r: &'r Reader<'a>, prefix: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        let mut iter = ReaderIter::new_from(r, prefix)?;
        iter.k.extend_from_slice(prefix);
        iter.it_type = ReaderIterType::GetPrefix;
        Ok(iter)
    }

    fn new_get_range(r: &'r Reader<'a>, start: &[u8], end: &[u8]) -> Result<ReaderIter<'r, 'a>, ()> {
        let mut iter = ReaderIter::new_from(r, start)?;
        iter.k.extend_from_slice(end);
        iter.it_type = ReaderIterType::GetRange;
        Ok(iter)
    }

    pub fn seek(&mut self, key: &[u8]) -> bool {
        self.index_iter.seek(key);

        let (key, val) = match self.index_iter.get() {
            Some((key, val)) => (key, val),
            None => {
                // This seek puts us after the last key, so we mark the
                // iterator as invalid and return success. The next
                // next() operation will return false.
                self.valid = false;
                return true;
            }
        };

        let mut new_offset = 0;
        varint_decode64(val, &mut new_offset);

        // We can skip decoding a new block if our new key is within the
        // currently-decoded block.
        if self.block_offset != new_offset {
            self.block_offset = new_offset;
            let b = self.r.block(new_offset as usize);
            self.bi = BlockIter::init(Arc::new(b));
        }

        self.bi.seek(key);

        self.first = true;
        self.valid = true;

        return true;
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if !self.valid {
            return None;
        }

        if !self.first {
            self.bi.next();
        }
        self.first = false;

        let (key, val) = match self.bi.get() {
            Some((key, val)) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                (key, val)
            },
            None => {
                self.valid = false;
                if !self.index_iter.next() {
                    return None;
                }
                let b = self.r.block_at_index(&self.index_iter).unwrap();
                self.bi = BlockIter::init(Arc::new(b));
                self.bi.seek_to_first();

                let entry = self.bi.get();
                self.valid = entry.is_some();
                entry?
            }
        };

        match self.it_type {
            ReaderIterType::Iter => (),
            ReaderIterType::Get => {
                if key != self.k.as_slice() {
                    self.valid = false;
                }
            }
            ReaderIterType::GetPrefix => {
                if !(self.k.len() <= key.len() && key.starts_with(&self.k)) {
                    self.valid = false;
                }
            }
            ReaderIterType::GetRange => {
                if bytes_compare(key, &self.k) > 0 {
                    self.valid = false;
                }
            }
        }

        if self.valid { Some((key, val)) } else { None }
    }
}
