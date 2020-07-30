use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::cmp::{Reverse, Ordering};
use std::mem;

use crate::{Reader, ReaderIntoIter};

pub struct Entry<A> {
    iter: ReaderIntoIter<A>,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl<A: AsRef<[u8]>> Entry<A> {
    // also fills the entry
    fn new(iter: ReaderIntoIter<A>) -> Option<Entry<A>> {
        let mut entry = Entry {
            iter,
            key: Vec::with_capacity(256),
            val: Vec::with_capacity(256),
        };

        if !entry.fill() {
            return None
        }

        Some(entry)
    }

    fn fill(&mut self) -> bool {
        self.key.clear();
        self.val.clear();

        match self.iter.next() {
            Some((key, val)) => {
                self.key.extend_from_slice(key);
                self.val.extend_from_slice(val);
                true
            },
            None => false,
        }
    }
}

impl<A: AsRef<[u8]>> Ord for Entry<A> {
    fn cmp(&self, other: &Entry<A>) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<A: AsRef<[u8]>> Eq for Entry<A> {}

impl<A: AsRef<[u8]>> PartialEq for Entry<A> {
    fn eq(&self, other: &Entry<A>) -> bool {
        self.key == other.key
    }
}

impl<A: AsRef<[u8]>> PartialOrd for Entry<A> {
    fn partial_cmp(&self, other: &Entry<A>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergerOptions<MF> {
    pub merge: MF,
    // pub dupsort: DF,
}

pub struct Merger<A, MF> {
    sources: Vec<Reader<A>>,
    opt: MergerOptions<MF>,
}

impl<A: AsRef<[u8]>, MF> Merger<A, MF> {
    pub fn new(sources: Vec<Reader<A>>, opt: MergerOptions<MF>) -> Self {
        Merger { sources, opt }
    }

    pub fn into_merge_iter(mut self) -> MergerIter<A, MF> {
        let mut heap = BinaryHeap::new();
        for source in self.sources.drain(..) {
            if let Ok(iter) = source.into_iter() {
                if let Some(entry) = Entry::new(iter) {
                    heap.push(Reverse(entry));
                }
            }
        }

        MergerIter {
            merger: self,
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            merged_val: Vec::new(),
            pending: false,
        }
    }

    pub fn into_iter(self) -> MultiIter<A> {
        let mut heap = BinaryHeap::new();
        for source in self.sources {
            if let Ok(iter) = source.into_iter() {
                if let Some(entry) = Entry::new(iter) {
                    heap.push(Reverse(entry));
                }
            }
        }

        MultiIter {
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            pending: false,
        }
    }
}

pub struct MergerIter<A, MF> {
    merger: Merger<A, MF>,
    heap: BinaryHeap<Reverse<Entry<A>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Vec<u8>>,
    merged_val: Vec<u8>,
    pending: bool,
}

impl<A, MF> MergerIter<A, MF>
where A: AsRef<[u8]>,
      MF: Fn(&[u8], &[Vec<u8>]) -> Option<Vec<u8>>,
{
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        self.cur_key.clear();
        self.cur_vals.clear();

        loop {
            let mut entry = match self.heap.peek_mut() {
                Some(e) => e,
                None => break,
            };

            if self.cur_key.is_empty() {
                self.cur_key.extend_from_slice(&entry.0.key);
                self.cur_vals.clear();
                self.pending = true;
            }

            if self.cur_key == entry.0.key {
                self.cur_vals.push(mem::take(&mut entry.0.val));
                if !entry.0.fill() {
                    PeekMut::pop(entry);
                }
            } else {
                break;
            }
        }

        if self.pending {
            self.merged_val = (self.merger.opt.merge)(&self.cur_key, &self.cur_vals).expect("merge abort");
            self.pending = false;
            Some((&self.cur_key, &self.merged_val))
        } else {
            None
        }
    }
}

pub struct MultiIter<A> {
    heap: BinaryHeap<Reverse<Entry<A>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Vec<u8>>,
    pending: bool,
}

impl<A: AsRef<[u8]>> Iterator for MultiIter<A> {
    type Item = (Vec<u8>, Vec<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        self.cur_key.clear();
        self.cur_vals.clear();

        loop {
            let mut entry = match self.heap.peek_mut() {
                Some(e) => e,
                None => break,
            };

            if self.cur_key.is_empty() {
                self.cur_key.extend_from_slice(&entry.0.key);
                self.cur_vals.clear();
                self.pending = true;
            }

            if self.cur_key == entry.0.key {
                self.cur_vals.push(mem::take(&mut entry.0.val));
                if !entry.0.fill() {
                    PeekMut::pop(entry);
                }
            } else {
                break;
            }
        }

        if self.pending {
            self.pending = false;
            Some((mem::take(&mut self.cur_key), mem::take(&mut self.cur_vals)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Writer, Reader, ReaderOptions};

    #[test]
    fn easy() {
        fn merge(_key: &[u8], values: &[Vec<u8>]) -> Option<Vec<u8>> {
            let len = values.iter().map(|v| v.len()).sum::<usize>();
            let mut out = Vec::with_capacity(len);
            values.iter().for_each(|v| out.extend_from_slice(v));
            Some(out)
        }

        let mut vecs = Vec::new();
        for i in 0..10 {
            let mut writer = Writer::memory(None);
            for i in (0 + i)..30 * (i + 1) {
                let key = format!("{:010}", i);
                let value = format!("{:010}", i).repeat(i / 10_000);
                writer.add(key, value).unwrap();
            }
            let vec = writer.into_inner().unwrap();
            vecs.push(vec);
        }

        let sources = vecs.into_iter()
            .map(|v| Reader::new(v, ReaderOptions::default()).unwrap())
            .collect();

        let opt = MergerOptions { merge };
        let merger = Merger::new(sources, opt);

        let mut iter = merger.into_merge_iter();
        let mut prev_key = vec![];
        while let Some((k, _v)) = iter.next() {
            assert!(&*prev_key < k, "order is not respected");
            prev_key = k.to_vec();
        }
    }
}
