use std::mem;
use byteorder::{LittleEndian, ByteOrder};

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

    pub fn is_emtpy(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        if self.buf.len() > u32::max_value() as usize {
            self.buf.len() + (self.restarts.len() * mem::size_of::<u64>()) + mem::size_of::<u32>()
        } else {
            self.buf.len() + (self.restarts.len() * mem::size_of::<u64>() / 2) + mem::size_of::<u32>()
        }
    }

    pub fn add(&mut self, key: &[u8], val: &[u8]) {
        assert!(self.counter <= self.block_restart_interval);
        assert!(self.finished == false);

        let mut shared = 0;

        // see how much sharing to do with previous key
        if self.counter < self.block_restart_interval {
            let min_length = if self.last_key.len() > key.len() { key.len() } else { self.last_key.len() };
            for (l, k) in self.last_key.iter().zip(key) {
                if shared >= min_length || l != k { break }
                shared += 1;
            }
        } else {
            // restart compression
            self.restarts.push(self.buf.len() as u64);
            self.counter = 0;
        }

        let non_shared = key.len() - shared;

        // ensure enough buffer space is available
        self.buf.reserve(5 * 3 + key.len() + val.len());

        // add "[shared][non-shared][value length]" to buffer
        let _ = LittleEndian::write_u32(&mut self.buf, shared as u32);
        let _ = LittleEndian::write_u32(&mut self.buf, non_shared as u32);
        let _ = LittleEndian::write_u32(&mut self.buf, val.len() as u32);

        // add key suffix to buffer followed by value
        self.buf.extend_from_slice(&key[shared..shared + non_shared]);
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
            if restart64 {
                let _ = LittleEndian::write_u64(&mut self.buf, *restart);
            } else {
                let _ = LittleEndian::write_u32(&mut self.buf, *restart as u32);
            }
        }

        let restarts_size = self.restarts.len();
        let _ = LittleEndian::write_u32(&mut self.buf, restarts_size as u32);

        self.finished = true;
        mem::replace(&mut self.buf, Vec::with_capacity(65536))
    }
}
