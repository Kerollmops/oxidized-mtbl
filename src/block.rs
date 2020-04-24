use std::borrow::Cow;
use std::mem;
use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};

use crate::varint::varint_decode32;
use crate::bytes_compare;

#[derive(Clone)]
pub struct Block<'a> {
    data: Cow<'a, [u8]>,
    restart_offset: u64,
}

impl<'a> Block<'a> {
    pub fn init(mut data: Cow<'a, [u8]>) -> Block<'a> {
        let mut restart_offset = 0;

        if data.len() < mem::size_of::<u32>() {
            data = Cow::Borrowed(&[]);
        } else {
            restart_offset = data.len() - (1 + num_restarts(&data) as usize) * mem::size_of::<u32>();
        }

        // Check if a 32-bit restart array would leave room for restart offsets
        // too large for an unsigned 32 bit integer. The writer performs this
        // same check, and will switch to 64 bit restart offsets if necessary.
        // We detect this situation here, and do the same.
        if restart_offset > u32::max_value() as usize {
            restart_offset = data.len() - (
                mem::size_of::<u32>() + num_restarts(&data) as usize * mem::size_of::<u64>()
            );
            // b->restart_offset is the offset of the first byte after
            // the entries stored in the block. If that offset fits
            // in a 32 bit unsigned integer field, the block should have
            // used 32 bit restart offsets. We consider a block where
            // a 32 bit restart offset array would begin after UINT32_MAX
            // and a 64 bit restart array would begin before to be malformed.
            if restart_offset <= u32::max_value() as usize {
                data = Cow::Borrowed(&[]);
            }
        }

        if restart_offset > data.len() - mem::size_of::<u32>() {
            data = Cow::Borrowed(&[]);
        }

        Block { data, restart_offset: restart_offset as u64 }
    }
}

impl AsRef<[u8]> for Block<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

fn num_restarts(data: &[u8]) -> u32 {
    assert!(data.len() >= 2 * mem::size_of::<u32>());
    LittleEndian::read_u32(&data[data.len() - mem::size_of::<u32>()..])
}

pub struct BlockIter<'a> {
    pub(crate) block: Arc<Block<'a>>,
    restarts: u64,
    num_restarts: u32,
    current: u64,
    restart_index: u32,
    next: Option<u64>,
    pub(crate) key: Vec<u8>,
    pub(crate) val: Option<(usize, usize)>,
}

impl<'a> BlockIter<'a> {
    pub fn init(b: Arc<Block<'a>>) -> BlockIter<'a> {
        assert!(b.data.len() >= 2 * mem::size_of::<u32>());

        let num_restarts = num_restarts(&b.data);
        assert!(num_restarts > 0);

        let restart_offset = b.restart_offset;

        BlockIter {
            block: b,
            restarts: restart_offset,
            num_restarts,
            current: restart_offset,
            restart_index: num_restarts,
            next: None,
            key: Vec::new(),
            val: None,
        }
    }

    fn restart_point(&self, idx: u32) -> u64 {
        assert!(idx < self.num_restarts);

        let offset = self.restarts as usize + idx as usize * mem::size_of::<u32>();
        if self.restarts > u32::max_value() as u64 {
            LittleEndian::read_u64(&self.block.data[offset..])
        } else {
            LittleEndian::read_u32(&self.block.data[offset..]) as u64
        }
    }

    fn seek_to_restart_point(&mut self, idx: u32) {
        self.key.clear();

        self.restart_index = idx;
        let offset = self.restart_point(idx);
        self.next = Some(offset);
    }

    fn next_entry_offset(&self) -> u64 {
        // return the offset in ->data just past the end of the current entry
        self.next.unwrap_or(0)
    }

    fn parse_next_key(&mut self) -> bool {
        self.current = self.next_entry_offset();

        if self.current >= self.restarts {
            // no more entries to return, mark as invalid
            self.current = self.restarts;
            self.restart_index = self.num_restarts;
            return false;
        }

        // decode next entry
        let (shared, non_shared, value_length, p) =
            decode_entry(&self.block.data, self.current as usize, self.restarts as usize).unwrap();
        assert!(self.key.capacity() >= shared as usize);

        self.key.truncate(shared as usize);
        self.key.extend_from_slice(&self.block.data[p..p + non_shared as usize]);

        self.next = Some(p as u64 + non_shared as u64 + value_length as u64);
        self.val = Some((p + non_shared as usize, value_length as usize));
        while self.restart_index + 1 < self.num_restarts && self.restart_point(self.restart_index + 1) < self.current {
            self.restart_index += 1;
        }
        return true;
    }

    fn valid(&self) -> bool {
        self.current < self.restarts
    }

    pub fn seek_to_first(&mut self) {
        self.seek_to_restart_point(0);
        self.parse_next_key();
    }

    pub fn seek(&mut self, target: &[u8]) {
        // binary search in restart array to find the first restart point
        // with a key >= target
        let mut left: u32 = 0;
        let mut right: u32 = self.num_restarts - 1;

        while left < right {
            let mid = (left + right + 1) / 2;
            let region_offset = self.restart_point(mid);

            let (shared, non_shared, _value_length, key_offset) =
                decode_entry(&self.block.data, region_offset as usize, self.restarts as usize).unwrap();

            if shared != 0 {
                // corruption
                return;
            }

            let key = &self.block.data[key_offset..key_offset + non_shared as usize];
            if bytes_compare(key, target) < 0 {
                // key at "mid" is smaller than "target", therefore all
                // keys before "mid" are uninteresting
                left = mid;
            } else {
                // key at "mid" is larger than "target", therefore all
                // keys at or before "mid" are uninteresting
                right = mid - 1;
            }
        }

        // linear search within restart block for first key >= target
        self.seek_to_restart_point(left);
        loop {
            if !self.parse_next_key() {
                return;
            }
            if bytes_compare(&self.key, target) >= 0 {
                return;
            }
        }
    }

    pub fn next(&mut self) -> bool {
        if !self.valid() {
            return false;
        }
        self.parse_next_key();
        self.valid()
    }

    pub fn get(&self) -> Option<(&[u8], &[u8])> {
        if !self.valid() {
            return None;
        }

        let key = self.key.as_slice();
        let (val_offset, val_len) = self.val.unwrap();

        return Some((key, &self.block.data[val_offset..val_offset + val_len]));
    }
}

fn decode_entry(data: &[u8], mut p: usize, limit: usize) -> Result<(u32, u32, u32, usize), ()> {
    if limit - p < 3 {
        return Err(());
    }

    let mut shared = data[p + 0] as u32;
    let mut non_shared = data[p + 1] as u32;
    let mut value_length = data[p + 2] as u32;

    if (shared | non_shared | value_length) < 128 {
        // fast path
        p += 3;
    } else {
        p += varint_decode32(&data[p..], &mut shared);
        p += varint_decode32(&data[p..], &mut non_shared);
        p += varint_decode32(&data[p..], &mut value_length);
        assert!(p <= limit);
    }

    assert!(!((limit - p) < (non_shared + value_length) as usize));

    Ok((shared, non_shared, value_length, p))
}
