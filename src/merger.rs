use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::cmp::{Reverse, Ordering};
use std::{mem, io};

use crate::{Error, Writer, Reader, ReaderIntoIter};

pub struct Entry<A> {
    iter: ReaderIntoIter<A>,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl<A: AsRef<[u8]>> Entry<A> {
    // also fills the entry
    fn new(iter: ReaderIntoIter<A>) -> Result<Option<Entry<A>>, Error> {
        let mut entry = Entry {
            iter,
            key: Vec::with_capacity(256),
            val: Vec::with_capacity(256),
        };

        if !entry.fill()? {
            return Ok(None)
        }

        Ok(Some(entry))
    }

    fn fill(&mut self) -> Result<bool, Error> {
        self.key.clear();
        self.val.clear();

        match self.iter.next() {
            Some(result) => {
                let (key, val) = result?;
                self.key.extend_from_slice(key);
                self.val.extend_from_slice(val);
                Ok(true)
            },
            None => Ok(false),
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

#[derive(Clone)]
pub struct MergerBuilder<A, MF> {
    sources: Vec<Reader<A>>,
    merge: MF,
}

impl<A, MF> MergerBuilder<A, MF> {
    pub fn new(merge: MF) -> Self {
        MergerBuilder { merge, sources: Vec::new() }
    }

    pub fn add(&mut self, source: Reader<A>) -> &mut Self {
        self.push(source);
        self
    }

    pub fn push(&mut self, source: Reader<A>) {
        self.sources.push(source);
    }

    pub fn build(self) -> Merger<A, MF> {
        Merger { sources: self.sources, merge: self.merge }
    }
}

impl<A, MF> Extend<Reader<A>> for MergerBuilder<A, MF> {
    fn extend<T: IntoIterator<Item=Reader<A>>>(&mut self, iter: T) {
        self.sources.extend(iter);
    }
}

pub struct Merger<A, MF> {
    sources: Vec<Reader<A>>,
    merge: MF,
}

impl<A, MF> Merger<A, MF> {
    pub fn builder(merge: MF) -> MergerBuilder<A, MF> {
        MergerBuilder::new(merge)
    }
}

impl<A: AsRef<[u8]>, MF> Merger<A, MF> {
    pub fn into_merge_iter(self) -> Result<MergerIter<A, MF>, Error> {
        let mut heap = BinaryHeap::new();
        for source in self.sources {
            if let Ok(iter) = source.into_iter() {
                if let Some(entry) = Entry::new(iter)? {
                    heap.push(Reverse(entry));
                }
            }
        }

        Ok(MergerIter {
            merge: self.merge,
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            merged_val: Vec::new(),
            pending: false,
        })
    }

    pub fn into_iter(self) -> Result<MultiIter<A>, Error> {
        let mut heap = BinaryHeap::new();
        for source in self.sources {
            if let Ok(iter) = source.into_iter() {
                if let Some(entry) = Entry::new(iter)? {
                    heap.push(Reverse(entry));
                }
            }
        }

        Ok(MultiIter {
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            pending: false,
        })
    }
}

impl<A, MF, U> Merger<A, MF>
where A: AsRef<[u8]>,
      MF: Fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, U>,
{
    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_merge_iter().map_err(Error::convert_merge_error)?;
        while let Some(result) = iter.next() {
            let (key, val) = result?;
            writer.insert(key, val)?;
        }
        Ok(())
    }
}

pub struct MergerIter<A, MF> {
    merge: MF,
    heap: BinaryHeap<Reverse<Entry<A>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Vec<u8>>,
    merged_val: Vec<u8>,
    pending: bool,
}

impl<A, MF, U> MergerIter<A, MF>
where A: AsRef<[u8]>,
      MF: Fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, U>,
{
    pub fn next(&mut self) -> Option<Result<(&[u8], &[u8]), Error<U>>> {
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
                match entry.0.fill() {
                    Ok(filled) => if !filled { PeekMut::pop(entry); },
                    Err(e) => return Some(Err(e.convert_merge_error())),
                }
            } else {
                break;
            }
        }

        if self.pending {
            self.merged_val = if self.cur_vals.len() == 1 {
                self.cur_vals.pop().unwrap()
            } else {
                match (self.merge)(&self.cur_key, &self.cur_vals) {
                    Ok(val) => val,
                    Err(e) => return Some(Err(Error::Merge(e))),
                }
            };
            self.pending = false;
            Some(Ok((&self.cur_key, &self.merged_val)))
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
    type Item = Result<(Vec<u8>, Vec<Vec<u8>>), Error>;

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
                match entry.0.fill() {
                    Ok(filled) => if !filled { PeekMut::pop(entry); },
                    Err(e) => return Some(Err(e)),
                }
            } else {
                break;
            }
        }

        if self.pending {
            self.pending = false;
            Some(Ok((mem::take(&mut self.cur_key), mem::take(&mut self.cur_vals))))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WriterBuilder, Reader};

    #[test]
    fn easy() {
        fn merge(_key: &[u8], values: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
            assert_ne!(values.len(), 1);
            let len = values.iter().map(|v| v.len()).sum::<usize>();
            let mut out = Vec::with_capacity(len);
            values.iter().for_each(|v| out.extend_from_slice(v));
            Ok(out)
        }

        let mut vecs = Vec::new();
        for i in 0..10 {
            let mut writer = WriterBuilder::new().memory();
            for i in (0 + i)..30 * (i + 1) {
                let key = format!("{:010}", i);
                let value = format!("{:010}", i).repeat(i / 10_000);
                writer.insert(key, value).unwrap();
            }
            let vec = writer.into_inner().unwrap();
            vecs.push(vec);
        }

        let sources: Vec<_> = vecs.into_iter()
            .map(|v| Reader::new(v).unwrap())
            .collect();

        let mut builder = Merger::builder(merge);
        builder.extend(sources);
        let merger = builder.build();

        let mut iter = merger.into_merge_iter().unwrap();
        let mut prev_key = vec![];
        while let Some(result) = iter.next() {
            let (k, _v) = result.unwrap();
            assert!(&*prev_key < k, "order is not respected");
            prev_key = k.to_vec();
        }
    }
}
