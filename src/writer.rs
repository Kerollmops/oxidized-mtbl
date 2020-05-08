use std::{cmp, mem, io};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};

use crate::block_builder::BlockBuilder;
use crate::compression::compress;
use crate::compression::CompressionType;
use crate::varint::varint_encode64;
use crate::{bytes_compare, FileVersion, Metadata};

const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
const DEFAULT_COMPRESSION_LEVEL: i32 = -10_000;
const METADATA_SIZE: usize = 512;
const MIN_BLOCK_SIZE: u64 = 1024;
pub const DEFAULT_BLOCK_SIZE: u64 = 8192;
pub const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::None;

#[derive(Clone, Copy)]
pub struct WriterOptions {
    compression_type: CompressionType,
    compression_level: i32,
    block_size: u64,
    block_restart_interval: usize,
}

impl WriterOptions {
    pub fn new() -> Self {
        WriterOptions::default()
    }

    pub fn set_compression_type(&mut self, compression_type: CompressionType) {
        self.compression_type = compression_type;
    }

    pub fn set_compression_level(&mut self, level: i32) {
        self.compression_level = level;
    }

    pub fn set_block_size(&mut self, block_size: u64) {
        self.block_size = cmp::max(block_size, MIN_BLOCK_SIZE);
    }

    pub fn set_block_restart_interval(&mut self, interval: usize) {
        self.block_restart_interval = interval;
    }
}

impl Default for WriterOptions {
    fn default() -> WriterOptions {
        WriterOptions {
            compression_type: DEFAULT_COMPRESSION_TYPE,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
        }
    }
}

pub struct Writer<W> {
    writer: W,
    metadata: Metadata,
    data: BlockBuilder,
    index: BlockBuilder,
    opt: WriterOptions,
    last_key: Vec<u8>,
    last_offset: u64,
    pending_index_entry: bool,
    pending_offset: u64,
}

impl<W: io::Write> Writer<W> {
    pub fn new(writer: W, options: Option<WriterOptions>) -> io::Result<Self> {
        let opt = options.unwrap_or_default();

        // derive defaut eventually
        let metadata = Metadata {
            data_block_size: opt.block_size,
            compression_algorithm: opt.compression_type,
            ..Metadata::default()
        };

        let last_offset = 0;

        Ok(Writer {
            writer,
            metadata,
            opt,
            last_offset,
            pending_offset: last_offset,
            last_key: Vec::with_capacity(256),
            data: BlockBuilder::new(opt.block_restart_interval),
            index: BlockBuilder::new(opt.block_restart_interval),
            pending_index_entry: false,
        })
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        if self.metadata.count_entries > 0 {
            if key <= &*self.last_key {
                panic!("out-of-order key");
            }
        }

        let estimated_block_size = self.data.current_size_estimate();
        let estimated_block_size = estimated_block_size + 3 * 5 + key.len() + val.len();

        if estimated_block_size >= self.opt.block_size as usize {
           self.flush()?;
        }

        if self.pending_index_entry {
            let mut enc = [0; 10];
            assert!(self.data.is_emtpy());
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
        // TODO find a better fix for the double borrow error.
        self.metadata.bytes_index_block = self.write_block(&mut self.index.clone(), CompressionType::None)? as u64;

        // We must write exactly 512 bytes at the end to store the metadata
        let mut tbuf = [0u8; METADATA_SIZE];
        self.metadata.write_to_bytes(&mut tbuf);
        self.writer.write_all(&tbuf)?;

        Ok(self.writer)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.data.is_emtpy() { return Ok(()) }

        assert!(!self.pending_index_entry);
        // TODO find a better fix for the double borrow error.
        self.metadata.bytes_data_blocks += self.write_block(&mut self.data.clone(), self.opt.compression_type)? as u64;
        self.data.reset();
        self.metadata.count_data_blocks += 1;
        self.pending_index_entry = true;

        Ok(())
    }

    pub fn write_block(&mut self, block: &mut BlockBuilder, compression_type: CompressionType) -> io::Result<usize> {
        let raw_content = block.finish();
        let block_content = compress(compression_type, self.opt.compression_level, &raw_content).expect("error compressing block");
        assert!(self.metadata.file_version == FileVersion::FormatV2);

        let crc = crc32c::crc32c(&block_content).to_le_bytes();

        let mut len = [0; 10];
        let len = varint_encode64(&mut len, block_content.len() as u64);
        self.writer.write_all(len)?;
        // already performed conversion before...
        self.writer.write_all(&crc)?;
        self.writer.write_all(&block_content)?;

        let bytes_written = len.len() + crc.len() + block_content.len();

        self.last_offset = self.pending_offset;
        self.pending_offset += bytes_written as u64;

        Ok(bytes_written)
    }
}

fn bytes_shortest_separator(start: &mut Vec<u8>, limit: &[u8]) {
    let min_length = if start.len() < limit.len() { start.len() } else { limit.len() };

    let mut diff_index = 0;
    for (s, l) in start.iter().zip(limit) {
        if diff_index >= min_length || s != l { break }
        diff_index += 1;
    }

    if diff_index >= min_length { return }

    let diff_byte = start[diff_index];
    if diff_byte < u8::max_value() && diff_byte + 1 < limit[diff_index] {
        start[diff_index] += 1;
        start.truncate(diff_index + 1);
    } else if diff_index < min_length - mem::size_of::<u16>() {
        // awww yeah, big endian arithmetic on strings
        let u_start = BigEndian::read_u16(&start[diff_index..]);
        let u_limit = BigEndian::read_u16(&limit[diff_index..]);
        let u_between = u_start + 1;
        if u_start <= u_between && u_between <= u_limit {
            let _ = start.write_u16::<BigEndian>(u_between);
        }
    }

    assert!(bytes_compare(&start, limit) < 0);
}
