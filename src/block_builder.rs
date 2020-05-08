use std::mem;

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

    pub fn add(&mut self, _key: &[u8], _val: &[u8]) {
        unimplemented!()
    }

    pub fn finish(&self) -> Vec<u8> {
        unimplemented!()
    }

    //pub fn block_builder_finish(&self, uint8_t **buf, bufsz: &[usize]){
    //}
}
