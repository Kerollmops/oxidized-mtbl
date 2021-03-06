use std::{cmp, mem, io};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use crate::block_builder::BlockBuilder;
use crate::compression::compress;
use crate::compression::CompressionType;
use crate::varint::varint_encode64;
use crate::{FileVersion, Metadata};

use crate::{DEFAULT_COMPRESSION_TYPE, DEFAULT_COMPRESSION_LEVEL};
use crate::{DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_RESTART_INTERVAL};
use crate::{MIN_BLOCK_SIZE, METADATA_SIZE};

#[derive(Debug, Clone, Copy)]
pub struct WriterBuilder {
    compression_type: CompressionType,
    compression_level: u32,
    block_size: u64,
    block_restart_interval: usize,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            compression_type: DEFAULT_COMPRESSION_TYPE,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
        }
    }

    pub fn compression_type(&mut self, compression: CompressionType) -> &mut Self {
        self.compression_type = compression;
        self
    }

    pub fn compression_level(&mut self, level: u32) -> &mut Self {
        self.compression_level = level;
        self
    }

    pub fn block_size(&mut self, block_size: u64) -> &mut Self {
        self.block_size = cmp::max(block_size, MIN_BLOCK_SIZE);
        self
    }

    pub fn block_restart_interval(&mut self, interval: usize) -> &mut Self {
        self.block_restart_interval = interval;
        self
    }

    pub fn build<W: io::Write>(&mut self, writer: W) -> Writer<W> {
        // derive default eventually
        let metadata = Metadata {
            data_block_size: self.block_size,
            compression_algorithm: self.compression_type,
            ..Metadata::default()
        };

        let last_offset = 0;

        Writer {
            writer,
            metadata,
            compression_type: self.compression_type,
            compression_level: self.compression_level,
            last_offset,
            pending_offset: last_offset,
            last_key: Vec::with_capacity(256),
            data: BlockBuilder::new(self.block_restart_interval),
            index: BlockBuilder::new(self.block_restart_interval),
            pending_index_entry: false,
        }
    }

    pub fn memory(&mut self) -> Writer<Vec<u8>> {
        self.build(Vec::new())
    }
}

pub struct Writer<W> {
    writer: W,
    metadata: Metadata,
    data: BlockBuilder,
    index: BlockBuilder,
    compression_type: CompressionType,
    compression_level: u32,
    last_key: Vec<u8>,
    last_offset: u64,
    pending_index_entry: bool,
    pending_offset: u64,
}

impl Writer<Vec<u8>> {
    pub fn memory() -> Writer<Vec<u8>> {
        WriterBuilder::new().memory()
    }
}

impl Writer<WriterBuilder> {
    pub fn builder() -> WriterBuilder {
        WriterBuilder::new()
    }
}

impl<W: io::Write> Writer<W> {
    pub fn new(writer: W) -> Writer<W> {
        WriterBuilder::new().build(writer)
    }

    pub fn insert<K, V>(&mut self, key: K, val: V) -> io::Result<()>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

        if self.metadata.count_entries > 0 {
            if key <= &*self.last_key {
                panic!("out-of-order key");
            }
        }

        let estimated_block_size = self.data.current_size_estimate();
        let estimated_block_size = estimated_block_size + 3 * 5 + key.len() + val.len();

        if estimated_block_size >= self.metadata.data_block_size as usize {
           self.flush()?;
        }

        if self.pending_index_entry {
            let mut enc = [0; 10];
            assert!(self.data.is_empty());
            bytes_shortest_separator(&mut self.last_key, key);
            self.index.add(&self.last_key, varint_encode64(&mut enc, self.last_offset));
            self.pending_index_entry = false;
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        self.metadata.count_entries += 1;
        self.metadata.bytes_keys += key.len() as u64;
        self.metadata.bytes_values += val.len() as u64;
        self.data.add(key, val);

        Ok(())
    }

    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    pub fn into_inner(mut self) -> io::Result<W> {
        self.flush()?;

        if self.pending_index_entry {
            let mut enc = [0; 10];
            self.index.add(&self.last_key, varint_encode64(&mut enc, self.last_offset));
            self.pending_index_entry = false;
        }

        self.metadata.index_block_offset = self.pending_offset as u64;
        self.metadata.bytes_index_block += write_block(
            &mut self.writer,
            CompressionType::None,
            0,
            self.metadata.file_version,
            &mut self.last_offset,
            &mut self.pending_offset,
            &mut self.index,
        )? as u64;

        // We must write exactly 512 bytes at the end to store the metadata
        let mut tbuf = [0u8; METADATA_SIZE];
        self.metadata.write_to_bytes(&mut tbuf)?;
        self.writer.write_all(&tbuf)?;

        Ok(self.writer)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.data.is_empty() { return Ok(()) }

        assert!(!self.pending_index_entry);
        self.metadata.bytes_data_blocks += write_block(
            &mut self.writer,
            self.compression_type,
            self.compression_level,
            self.metadata.file_version,
            &mut self.last_offset,
            &mut self.pending_offset,
            &mut self.data,
        )? as u64;
        self.metadata.count_data_blocks += 1;
        self.pending_index_entry = true;

        Ok(())
    }
}

fn write_block<W: io::Write>(
    writer: &mut W,
    compression_type: CompressionType,
    compression_level: u32,
    file_version: FileVersion,
    last_offset: &mut u64,
    pending_offset: &mut u64,
    block: &mut BlockBuilder,
) -> io::Result<usize>
{
    let raw_content = block.finish();
    let block_content = compress(compression_type, compression_level, &raw_content)?;
    assert!(file_version == FileVersion::FormatV2);

    #[cfg(feature = "checksum")]
    let crc = crc32c::crc32c(&block_content).to_le_bytes();
    #[cfg(not(feature = "checksum"))]
    let crc = 0u32.to_le_bytes();

    let mut len = [0; 10];
    let len = varint_encode64(&mut len, block_content.len() as u64);
    writer.write_all(len)?;
    // already performed conversion before...
    writer.write_all(&crc)?;
    writer.write_all(&block_content)?;

    let bytes_written = len.len() + crc.len() + block_content.len();

    *last_offset = *pending_offset;
    *pending_offset += bytes_written as u64;

    block.reset();

    Ok(bytes_written)
}

fn bytes_shortest_separator(start: &mut Vec<u8>, limit: &[u8]) {
    let min_length = if start.len() < limit.len() { start.len() } else { limit.len() };

    let mut diff_index = 0;
    for (s, l) in start.iter().zip(limit).take(min_length) {
        if s != l { break }
        diff_index += 1;
    }

    if diff_index >= min_length { return }

    let diff_byte = start[diff_index];
    if diff_byte < u8::max_value() && diff_byte + 1 < limit[diff_index] {
        start[diff_index] += 1;
        start.truncate(diff_index + 1);
    } else if diff_index < min_length.saturating_sub(mem::size_of::<u16>()) {
        // awww yeah, big endian arithmetic on strings
        let u_start = BigEndian::read_u16(&start[diff_index..]);
        let u_limit = BigEndian::read_u16(&limit[diff_index..]);
        let u_between = u_start + 1;
        if u_start <= u_between && u_between <= u_limit {
            let _ = start.write_u16::<BigEndian>(u_between);
        }
    }

    assert!(start.as_slice() < limit);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Reader;

    #[test]
    fn empty() {
        let writer = WriterBuilder::new().memory();
        let vec = writer.into_inner().unwrap();

        let reader = Reader::new(&vec).unwrap();
        let mut iter = reader.into_iter().unwrap();

        assert!(iter.next().is_none());
    }

    #[test]
    fn one_key() {
        let mut writer = WriterBuilder::new().memory();
        writer.insert("hello", "I'm the one").unwrap();

        let vec = writer.into_inner().unwrap();
        let reader = Reader::new(&vec).unwrap();

        let mut count = 0;
        let mut iter = reader.into_iter().unwrap();
        while let Some(_) = iter.next() {
            count += 1;
        }

        assert_eq!(count, 1);
    }

    #[test]
    fn bytes_shortest_separator_to_short() {
        let mut start = vec![49, 115, 116];
        let limit = &[50];
        bytes_shortest_separator(&mut start, limit);
    }
}
