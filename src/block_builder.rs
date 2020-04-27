
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
        let mut bb = Self {
            block_restart_interval,
            buf: Vec::with_capacity(65536),
            last_key: Vec::with_capacity(256),
            restarts: Vec::with_capacity(64),
            counter: 0,
            finished: false,
        };
        bb.restarts.push(0);
        bb
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.last_key.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.finished = false;
    }

    pub fn is_emtpy(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        unimplemented!()
    }

    //pub fn block_builder_finish(&self, uint8_t **buf, bufsz: &[usize]){
    //}
}


