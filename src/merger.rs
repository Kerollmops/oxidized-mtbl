use std::collections::BinaryHeap;
use std::cmp::{Reverse, Ordering};
use std::mem;

use crate::{Reader, ReaderIter};

pub struct Entry<'r, 'a> {
    finished: bool,
    iter: ReaderIter<'r, 'a>,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl<'r, 'a> Entry<'r, 'a> {
    // also fills the entry
    fn new(iter: ReaderIter<'r, 'a>) -> Entry<'r, 'a> {
        let mut entry = Entry {
            finished: false,
            iter,
            key: Vec::with_capacity(256),
            val: Vec::with_capacity(256),
        };

        if !entry.fill() {
            entry.finished = true;
        }

        entry
    }

    fn fill(&mut self) -> bool {
        self.key.clear();
        self.val.clear();

        match self.iter.next() {
            Some((key, val)) => {
                self.finished = false;
                self.key.extend_from_slice(key);
                self.val.extend_from_slice(val);
                true
            },
            None => {
                self.finished = true;
                false
            }
        }
    }
}

impl Ord for Entry<'_, '_> {
    fn cmp(&self, other: &Entry) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl Eq for Entry<'_, '_> {}

impl PartialEq for Entry<'_, '_> {
    fn eq(&self, other: &Entry) -> bool {
        self.key == other.key
    }
}

impl PartialOrd for Entry<'_, '_> {
    fn partial_cmp(&self, other: &Entry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergerOptions<MF> {
    pub merge: MF,
    // pub dupsort: DF,
}

pub struct Merger<'a, MF> {
    sources: Vec<Reader<'a>>,
    opt: MergerOptions<MF>,
}

impl<'r, 'a, MF> Merger<'a, MF> {
    pub fn new(sources: Vec<Reader<'a>>, opt: MergerOptions<MF>) -> Self {
        Merger { sources, opt }
    }

    pub fn merge_iter(&'r mut self) -> MergerIter<'r, 'a, MF> {
        let mut heap = BinaryHeap::new();
        for source in &self.sources {
            if let Ok(iter) = source.iter() {
                let entry = Entry::new(iter);
                if !entry.finished {
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
            finished: false,
            pending: false,
        }
    }

    pub fn iter(&mut self) -> MultiIter {
        let mut heap = BinaryHeap::new();
        for source in &self.sources {
            if let Ok(iter) = source.iter() {
                let entry = Entry::new(iter);
                if !entry.finished {
                    heap.push(Reverse(entry));
                }
            }
        }

        MultiIter {
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            finished: false,
            pending: false,
        }
    }
}

pub struct MergerIter<'r, 'a, MF> {
    merger: &'r Merger<'a, MF>,
    heap: BinaryHeap<Reverse<Entry<'r, 'a>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Vec<u8>>,
    merged_val: Vec<u8>,
    finished: bool,
    pending: bool,
}

impl<MF> MergerIter<'_, '_, MF>
where MF: Fn(&[u8], &[Vec<u8>]) -> Option<Vec<u8>>
{
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        if self.finished {
            return None;
        }

        self.cur_key.clear();
        self.cur_vals.clear();

        'outer: loop {
            let mut entry = loop {
                match self.heap.peek() {
                    Some(e) => {
                        if e.0.finished {
                            self.heap.pop();
                        } else {
                            break self.heap.peek_mut().unwrap();
                        }
                    },
                    None => {
                        self.finished = true;
                        break 'outer;
                    }
                }
            };

            if self.cur_key.is_empty() {
                self.cur_key.extend_from_slice(&entry.0.key);
                self.cur_vals.clear();
                self.pending = true;
            }

            if self.cur_key == entry.0.key {
                self.cur_vals.push(mem::take(&mut entry.0.val));
                let _res = entry.0.fill();
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

pub struct MultiIter<'r, 'a> {
    heap: BinaryHeap<Reverse<Entry<'r, 'a>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Vec<u8>>,
    finished: bool,
    pending: bool,
}

impl Iterator for MultiIter<'_, '_> {
    type Item = (Vec<u8>, Vec<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        self.cur_key.clear();
        self.cur_vals.clear();

        'outer: loop {
            let mut entry = loop {
                match self.heap.peek() {
                    Some(e) => {
                        if e.0.finished {
                            self.heap.pop();
                        } else {
                            break self.heap.peek_mut().unwrap();
                        }
                    },
                    None => {
                        self.finished = true;
                        break 'outer;
                    }
                }
            };

            if self.cur_key.is_empty() {
                self.cur_key.extend_from_slice(&entry.0.key);
                self.cur_vals.clear();
                self.pending = true;
            }

            if self.cur_key == entry.0.key {
                self.cur_vals.push(mem::take(&mut entry.0.val));
                let _res = entry.0.fill();
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

        let sources = vecs.iter()
            .map(|v| Reader::new(v.as_ref(), ReaderOptions::default()).unwrap())
            .collect();

        let opt = MergerOptions { merge };
        let mut merger = Merger::new(sources, opt);

        let mut iter = merger.merge_iter();
        let mut prev_key = vec![];
        while let Some((k, _v)) = iter.next() {
            assert!(&*prev_key < k, "order is not respected");
            prev_key = k.to_vec();
        }
    }
}
