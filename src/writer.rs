use crate::compression::CompressionType;
use crate::Metadata;
use crate::block_builder::BlockBuilder;
use crate::FileVersion;

use std::os::unix::io::{ AsRawFd, RawFd };
use std::os::unix::fs::PermissionsExt;
use std::fs::OpenOptions;
use nix::unistd::{dup, lseek, Whence};


const DEFAULT_COMPRESSION_TYPE: CompressionType = CompressionType::None;
const DEFAULT_COMPRESSION_LEVEL: i32 = -10_000;
const DEFAULT_BLOCK_SIZE: u64 = 8192;
const DEFAULT_BLOCK_RESTART_INTERVAL: usize = 16;
            
pub struct WriterOptions {
    compression_type: CompressionType,
    compression_level: i32,
    block_size: u64,
    block_restart_interval: usize,
}


pub struct Writer {
    fd: RawFd,
    m: Metadata,
    data: BlockBuilder, 
    index: BlockBuilder, 
    opt: WriterOptions,
    last_key: Vec<u8>,
    last_offset: i64,
    closed: bool,
    pending_index_entry: bool,
    pending_offset: i64,
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

    pub fn set_compression_level(&mut self, level: i32) {
        unimplemented!()
    }

    pub fn set_block_size(&mut self, bs: usize) {
        unimplemented!()
    }

    pub fn set_block_restart_interval(&mut self, interval: usize) {
        unimplemented!()
    }
}

impl Writer {

    pub fn new(filename: &str, options: WriterOptions) -> Option<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)
            .map(|mut f| {
                f.metadata()
                    .unwrap()
                    .permissions()
                    .set_mode(0o644); f
            })
            .map(|f| Self::init_fd(f.as_raw_fd(), Some(options)))
            .ok()
    }

    pub fn init_fd(orig_fd: RawFd, options: Option<WriterOptions>) -> Self {
        let fd =  dup(orig_fd).unwrap();

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

        let last_offset = lseek(fd, 0, Whence::SeekCur).unwrap();
        let block_restart_interval = opt.block_restart_interval;
        Self {
            fd,
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

    pub fn add(&self, key: &[u8], val: &[u8]) -> Result<(), ()> {

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
            let enc = [0; 10];
            assert!(!self.data.is_emtpy());
            bytes_shortest_separator(&self.last_key, key);

        }
        unimplemented!()

    }

    pub fn finish(&self) {
        unimplemented!()
    }

    pub fn flush(&self) {
        unimplemented!()
    }

    pub fn write_block(&self, block: &BlockBuilder, compression_type: CompressionType) -> Result<usize, ()> {
        unimplemented!()
    }
}


pub fn bytes_shortest_separator(start: &[u8], limit: &[u8]) {
    unimplemented!()
}
