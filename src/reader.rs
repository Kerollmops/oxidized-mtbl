use std::borrow::Cow;
use std::mem;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use crate::block::{Block, BlockIter};
use crate::compression::decompress;
use crate::error::{Error, MtblError};
use crate::METADATA_SIZE;
use crate::varint::varint_decode64;
use crate::{BytesView, FileVersion, Metadata};

#[derive(Debug, Clone, Copy)]
pub struct ReaderBuilder {
    verify_checksums: bool,
}

impl ReaderBuilder {
    pub fn new() -> ReaderBuilder {
        ReaderBuilder {
            verify_checksums: true,
        }
    }

    pub fn verify_checksums(&mut self, verify: bool) -> &mut Self {
        self.verify_checksums = verify;
        self
    }

    pub fn read<A: AsRef<[u8]>>(&mut self, data: A) -> Result<Reader<A>, Error> {
        if data.as_ref().len() < METADATA_SIZE {
            return Err(Error::from(MtblError::InvalidMetadataSize))
        }

        let metadata_offset = data.as_ref().len() - METADATA_SIZE;
        let metadata_bytes = &data.as_ref()[metadata_offset..metadata_offset + METADATA_SIZE];
        let metadata = Metadata::read_from_bytes(metadata_bytes)?;

        // Sanitize the index block offset.
        // We calculate the maximum possible index block offset for this file to
        // be the total size of the file (r->len_data) minus the length of the
        // metadata block (METADATA_SIZE) minus the length of the minimum
        // sized block, which requires 4 fixed-length 32-bit integers (16 bytes).
        // FIXME why do I get 13 bytes!
        let max_index_block_offset = (data.as_ref().len() - METADATA_SIZE - 13) as u64;
        if metadata.index_block_offset > max_index_block_offset {
            return Err(Error::from(MtblError::InvalidIndexBlockOffset));
        }

        let index_len_len: usize;
        let index_len: usize;

        if metadata.file_version == FileVersion::FormatV1 {
            index_len_len = mem::size_of::<u32>();
            index_len = LittleEndian::read_u32(&data.as_ref()[metadata.index_block_offset as usize..]) as usize;
        } else {
            let mut tmp = 0;
            index_len_len = varint_decode64(&data.as_ref()[metadata.index_block_offset as usize..], &mut tmp);
            index_len = tmp as usize;
            if index_len as u64 != tmp {
                return Err(Error::from(MtblError::InvalidIndexLength));
            }
        }

        let start = metadata.index_block_offset as usize + index_len_len + mem::size_of::<u32>();
        let data = BytesView::from(data);
        let index_data = data.slice(start, index_len);

        #[cfg(feature = "checksum")] {
        if self.verify_checksums {
            let index_crc = LittleEndian::read_u32(&data.as_ref()[metadata.index_block_offset as usize + index_len_len..]);
            assert_eq!(index_crc, crc32c::crc32c(index_data.as_ref()));
        } }

        let index = Block::init(index_data).ok_or(MtblError::InvalidBlock)?;
        let index = Arc::new(index);
        let verify_checksums = self.verify_checksums;

        Ok(Reader { metadata, data, verify_checksums, index })
    }
}

#[derive(Clone)]
pub struct Reader<A> {
    metadata: Metadata,
    data: BytesView<A>,
    verify_checksums: bool,
    index: Arc<Block<A>>,
}

impl<A> Reader<A> {
    pub fn builder() -> ReaderBuilder {
        ReaderBuilder::new()
    }
}

impl<A: AsRef<[u8]>> Reader<A> {
    pub fn new(data: A) -> Result<Reader<A>, Error> {
        ReaderBuilder::new().read(data)
    }

    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    pub fn get(self, key: &[u8]) -> Result<Option<ReaderIntoGet<A>>, Error> {
        let mut iter = ReaderIntoIter::new_get(self, key)?;
        match iter.next() {
            Some(_) => Ok(ReaderIntoGet::new(iter.bi)),
            None => Ok(None),
        }
    }

    pub fn into_iter(self) -> Result<ReaderIntoIter<A>, Error> {
        ReaderIntoIter::new(self)
    }

    pub fn iter_from(self, start: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        ReaderIntoIter::new_from(self, start)
    }

    pub fn iter_prefix(self, prefix: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        ReaderIntoIter::new_get_prefix(self, prefix)
    }

    pub fn iter_range(self, start: &[u8], end: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        ReaderIntoIter::new_get_range(self, start, end)
    }

    fn block(&self, offset: usize) -> Result<Block<A>, Error> {
        assert!(offset < self.data.len());

        let raw_contents_size_len: usize;
        let raw_contents_size: usize;

        if self.metadata.file_version == FileVersion::FormatV1 {
            raw_contents_size_len = mem::size_of::<u32>();
            raw_contents_size = LittleEndian::read_u32(&self.data.as_ref()[offset..]) as usize;
        } else {
            let mut tmp = 0;
            raw_contents_size_len = varint_decode64(&self.data.as_ref()[offset..], &mut tmp);
            raw_contents_size = tmp as usize;
            assert_eq!(raw_contents_size as u64, tmp);
        }

        let raw_start = offset + raw_contents_size_len + mem::size_of::<u32>();
        let raw_contents = &self.data.as_ref()[raw_start..raw_start + raw_contents_size];

        #[cfg(feature = "checksum")] {
        if self.verify_checksums {
            let block_crc = LittleEndian::read_u32(&self.data.as_ref()[offset + raw_contents_size_len..]);
            let calc_crc = crc32c::crc32c(raw_contents);
            assert_eq!(block_crc, calc_crc);
        } }

        let data = decompress(self.metadata.compression_algorithm, raw_contents)?;
        let data = match data {
            Cow::Borrowed(_) => self.data.slice(raw_start, raw_contents_size),
            Cow::Owned(bytes) => BytesView::from_bytes(bytes),
        };

        let block = Block::init(data).ok_or(MtblError::InvalidBlock)?;

        Ok(block)
    }

    fn block_at_index(&self, index_iter: &BlockIter<A>) -> Result<Block<A>, Error> {
        match index_iter.get() {
            Some((_key, val)) => {
                let mut offset = 0;
                varint_decode64(val, &mut offset);
                self.block(offset as usize)
            },
            None => Err(Error::from(MtblError::InvalidBlock)),
        }
    }
}

pub struct ReaderIntoGet<A> {
    block: Arc<Block<A>>,
    val_offset: usize,
    val_len: usize,
}

impl<A> ReaderIntoGet<A> {
    fn new(block_iter: BlockIter<A>) -> Option<ReaderIntoGet<A>> {
        let (offset, length) = block_iter.val?;
        Some(ReaderIntoGet {
            block: block_iter.block,
            val_offset: offset,
            val_len: length,
        })
    }
}

impl<A: AsRef<[u8]>> AsRef<[u8]> for ReaderIntoGet<A> {
    fn as_ref(&self) -> &[u8] {
        &(*self.block).as_ref()[self.val_offset..self.val_offset + self.val_len]
    }
}

enum ReaderIterType {
    Iter,
    Get,
    GetPrefix,
    GetRange,
}

pub struct ReaderIntoIter<A> {
    r: Reader<A>,
    block_offset: u64,
    bi: BlockIter<A>,
    index_iter: BlockIter<A>,
    k: Vec<u8>,
    first: bool,
    valid: bool,
    it_type: ReaderIterType,
}

impl<A: AsRef<[u8]>> ReaderIntoIter<A> {
    fn new(r: Reader<A>) -> Result<ReaderIntoIter<A>, Error> {
        let mut index_iter = BlockIter::init(r.index.clone());
        index_iter.seek_to_first();

        let b = r.block_at_index(&index_iter)?;
        let mut bi = BlockIter::init(Arc::new(b));
        bi.seek_to_first();

        Ok(ReaderIntoIter {
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

    fn new_from(r: Reader<A>, key: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        let mut index_iter = BlockIter::init(r.index.clone());
        index_iter.seek(key);

        let b = r.block_at_index(&index_iter)?;
        let mut bi = BlockIter::init(Arc::new(b));

        bi.seek(key);

        Ok(ReaderIntoIter {
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

    fn new_get(r: Reader<A>, key: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        let mut iter = ReaderIntoIter::new_from(r, key)?;
        iter.k.extend_from_slice(key);
        iter.it_type = ReaderIterType::Get;
        Ok(iter)
    }

    fn new_get_prefix(r: Reader<A>, prefix: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        let mut iter = ReaderIntoIter::new_from(r, prefix)?;
        iter.k.extend_from_slice(prefix);
        iter.it_type = ReaderIterType::GetPrefix;
        Ok(iter)
    }

    fn new_get_range(r: Reader<A>, start: &[u8], end: &[u8]) -> Result<ReaderIntoIter<A>, Error> {
        let mut iter = ReaderIntoIter::new_from(r, start)?;
        iter.k.extend_from_slice(end);
        iter.it_type = ReaderIterType::GetRange;
        Ok(iter)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool, Error> {
        self.index_iter.seek(key);

        let (key, val) = match self.index_iter.get() {
            Some((key, val)) => (key, val),
            None => {
                // This seek puts us after the last key, so we mark the
                // iterator as invalid and return success. The next
                // next() operation will return false.
                self.valid = false;
                return Ok(true);
            }
        };

        let mut new_offset = 0;
        varint_decode64(val, &mut new_offset);

        // We can skip decoding a new block if our new key is within the
        // currently-decoded block.
        if self.block_offset != new_offset {
            self.block_offset = new_offset;
            let b = self.r.block(new_offset as usize)?;
            self.bi = BlockIter::init(Arc::new(b));
        }

        self.bi.seek(key);

        self.first = true;
        self.valid = true;

        return Ok(true);
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
                if key > &self.k {
                    self.valid = false;
                }
            }
        }

        if self.valid { Some((key, val)) } else { None }
    }
}
