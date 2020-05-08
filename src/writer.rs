use std::io::Write;
use std::fs::File;
use std::io;

use crate::compression::CompressionType;
use crate::Metadata;
use crate::block_builder::BlockBuilder;
use crate::FileVersion;
use crate::compression::{compress_level, compress};
use crate::varint::varint_encode64;

const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::None;
const DEFAULT_COMPRESSION_LEVEL: i32 = -10_000;
const DEFAULT_BLOCK_SIZE: u64 = 8192;
const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
const METADATA_SIZE: usize = 512;

pub struct WriterOptions {
    compression_type: CompressionType,
    compression_level: i32,
    block_size: u64,
    block_restart_interval: usize,
}

pub struct Writer {
    file: File,
    m: Metadata,
    data: BlockBuilder,
    index: BlockBuilder,
    opt: WriterOptions,
    last_key: Vec<u8>,
    last_offset: u64,
    closed: bool,
    pending_index_entry: bool,
    pending_offset: u64,
}

impl WriterOptions {
    pub fn new() -> Self {
        Self {
            compression_type: DEFAULT_COMPRESSION_TYPE,
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            block_size: DEFAULT_BLOCK_SIZE,
            block_restart_interval: DEFAULT_BLOCK_RESTART_INTERVAL,
        }
    }

    pub fn set_compression_type(&mut self, compression_type: CompressionType) {
        self.compression_type = compression_type;
    }

    pub fn set_compression_level(&mut self, _level: i32) {
        unimplemented!()
    }

    pub fn set_block_size(&mut self, _bs: usize) {
        unimplemented!()
    }

    pub fn set_block_restart_interval(&mut self, _interval: usize) {
        unimplemented!()
    }
}

impl Writer {
    pub fn new(filename: &str, options: WriterOptions) -> Option<Self> {
        File::create(filename).map(|f| Self::init_fd(f, Some(options))).ok()
    }

    pub fn init_fd(file: File, options: Option<WriterOptions>) -> Self {
        let mut file = file.try_clone().expect("error cloning the file");

        let opt = match options {
            Some(opt) => opt,
            // may break (see writer.c:121)
            None => WriterOptions::new(),
        };

        // derive defaut eventually
        let m = Metadata {
            file_version: FileVersion::FormatV2,
            index_block_offset: 0,
            data_block_size: opt.block_size,
            compression_algorithm: opt.compression_type,
            count_entries: 0,
            count_data_blocks: 0,
            bytes_data_blocks: 0,
            bytes_index_block: 0,
            bytes_keys: 0,
            bytes_values: 0,
        };

        use std::io::{Seek, SeekFrom};
        let last_offset = file.seek(SeekFrom::Start(0)).expect("error seeking file");
        let block_restart_interval = opt.block_restart_interval;
        Self {
            file,
            m,
            opt,
            last_offset,
            pending_offset: last_offset,
            last_key: Vec::with_capacity(256),
            data: BlockBuilder::new(block_restart_interval),
            index: BlockBuilder::new(block_restart_interval),
            pending_index_entry: false,
            closed: false,
        }
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) -> Result<(), ()> {

        if self.closed { panic!("writer is closed!") }
        if self.m.count_entries > 0 {
            if key != &*self.last_key {
                return Err(())
            }
        }

        let mut estimated_block_size = self.data.current_size_estimate();
        estimated_block_size += 3*5 + key.len() + val.len();
        if estimated_block_size >= self.opt.block_size as usize {
            self.flush();
        }

        if self.pending_index_entry {
            let mut enc = [0; 10];
            assert!(!self.data.is_emtpy());
            bytes_shortest_separator(&self.last_key, key);
            varint_encode64(&mut enc, self.last_offset as i64);

            self.index.add(&self.last_key, &enc);
            self.pending_index_entry = false;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.m.count_entries += 1;
        self.m.bytes_keys += key.len() as u64;
        self.m.bytes_values += val.len() as u64;
        self.data.add(key, val);
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        self.flush();
        assert!(!self.closed);
        self.closed = true;
        if self.pending_index_entry {
            let mut enc = [0; 10];
            varint_encode64(&mut enc, self.last_offset as i64);
            self.index.add(&self.last_key, &enc);
            self.pending_index_entry = false;
        }
        self.m.index_block_offset = self.pending_offset as u64;
        // TODO find a better fix for the double borrow error.
        self.m.bytes_index_block = self.write_block(&self.index.clone(), CompressionType::None) as u64;
        self.index.reset();
        let meta_bytes = self.m.as_bytes();
        self.file.write_all(meta_bytes)
    }

    fn flush(&mut self) {
        assert!(!self.closed);
        if self.data.is_emtpy() {
            return
        }
        assert!(!self.pending_index_entry);
        // TODO find a better fix for the double borrow error.
        self.m.bytes_data_blocks += self.write_block(&self.data.clone(), self.opt.compression_type) as u64;
        self.data.reset();
        self.m.count_data_blocks += 1;
        self.pending_index_entry = true;
    }

    pub fn write_block(&mut self, block: &BlockBuilder, compression_type: CompressionType) -> usize {

        let raw_content = block.finish();

        let block_content = if compression_type == CompressionType::None {
           raw_content
        } else if self.opt.compression_level == DEFAULT_COMPRESSION_LEVEL {
            compress(compression_type, &raw_content).expect("error compressing block")
        } else {
            compress_level(compression_type, self.opt.compression_level, &raw_content).expect("error compressing block")
        };

        assert!(self.m.file_version == FileVersion::FormatV2);

        let crc = crc32c::crc32c(&block_content).to_le_bytes();

        let mut len = [0; 10];
        varint_encode64(&mut len, block_content.len() as i64);
        self.file.write_all(&len).expect("write failed");
        // already performed conversion before...
        self.file.write_all(&crc).expect("write failed");
        self.file.write_all(&block_content).expect("write failed");

        let bytes_written = len.len() + crc.len() + block_content.len();

        self.last_offset = self.pending_offset;
        self.pending_offset += bytes_written as u64;

        return bytes_written
    }
}


pub fn bytes_shortest_separator(_start: &[u8], _limit: &[u8]) {
    unimplemented!()
}