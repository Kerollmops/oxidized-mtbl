use std::mem;
use byteorder::{LittleEndian, WriteBytesExt};
use crate::varint::varint_encode32;

#[derive(Clone)]
pub struct BlockBuilder {
    block_restart_interval: usize,
    buf: Vec<u8>,
    last_key: Vec<u8>,
    restarts: Vec<u64>,
    finished: bool,
    counter: usize,
}

impl BlockBuilder {
    pub fn new(block_restart_interval: usize) -> Self {
        BlockBuilder {
            block_restart_interval,
            buf: Vec::with_capacity(65536),
            last_key: Vec::with_capacity(256),
            restarts: vec![0],
            finished: false,
            counter: 0,
        }
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.last_key.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.finished = false;
        self.counter = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        let factor = if self.buf.len() > u32::max_value() as usize {
            mem::size_of::<u64>()
        } else {
            mem::size_of::<u64>() / 2
        };
        self.buf.len() + (self.restarts.len() * factor) + mem::size_of::<u32>()
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        assert!(self.counter <= self.block_restart_interval);
        assert!(!self.finished);

        let mut shared = 0;

        // see how much sharing to do with previous key
        if self.counter < self.block_restart_interval {
            shared = self.last_key.iter().zip(key).take_while(|(l, k)| l == k).count();
        } else {
            // restart compression
            self.restarts.push(self.buf.len() as u64);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // ensure enough buffer space is available
        self.buf.reserve(5 * 3 + key.len() + val.len());

        // add "[shared][non-shared][value length]" to buffer
        let mut buf = [0; 10];
        self.buf.extend_from_slice(varint_encode32(&mut buf, shared as u32));
        self.buf.extend_from_slice(varint_encode32(&mut buf, non_shared as u32));
        self.buf.extend_from_slice(varint_encode32(&mut buf, val.len() as u32));

        // add key suffix to buffer followed by value
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(val);

        // update state
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn finish(&mut self) -> Vec<u8> {
        let restart64 = self.buf.len() > u32::max_value() as usize;

        let estimate = self.current_size_estimate();
        self.buf.reserve(estimate);

        for restart in &self.restarts {
            let _ = if restart64 {
                self.buf.write_u64::<LittleEndian>(*restart)
            } else {
                self.buf.write_u32::<LittleEndian>(*restart as u32)
            };
        }

        let restarts_size = self.restarts.len();
        let _ = self.buf.write_u32::<LittleEndian>(restarts_size as u32);

        self.finished = true;
        mem::replace(&mut self.buf, Vec::with_capacity(65536))
    }
}
