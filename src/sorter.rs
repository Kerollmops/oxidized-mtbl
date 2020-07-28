use std::mem::size_of;
use std::fs::File;
use std::io;

use memmap::Mmap;

use crate::{Writer, WriterOptions, CompressionType};
use crate::{Merger, MergerOptions, MergerIter};
use crate::{Reader, ReaderOptions};
use crate::INITIAL_SORTER_VEC_SIZE;

pub struct SorterOptions<MF> {
    pub max_memory: usize,
    pub chunk_compression: CompressionType,
    pub merge: MF,
}

struct Entry {
    data: Vec<u8>,
    key_len: usize,
}

impl Entry {
    pub fn new(key: &[u8], val: &[u8]) -> Entry {
        let mut data = Vec::new();
        data.reserve_exact(key.len() + val.len());
        data.extend_from_slice(key);
        data.extend_from_slice(val);
        Entry { data, key_len: key.len() }
    }

    pub fn key(&self) -> &[u8] {
        &self.data[..self.key_len]
    }

    pub fn val(&self) -> &[u8] {
        &self.data[self.key_len..]
    }
}

pub struct Sorter<MF> {
    chunks: Vec<File>,
    entries: Vec<Entry>,
    /// The number of bytes allocated by the entries.
    entry_bytes: usize,
    options: SorterOptions<MF>,
}

impl<MF> Sorter<MF> {
    pub fn new(options: SorterOptions<MF>) -> Sorter<MF> {
        Sorter {
            chunks: Vec::new(),
            entries: Vec::with_capacity(INITIAL_SORTER_VEC_SIZE),
            entry_bytes: 0,
            options,
        }
    }
}

impl<MF> Sorter<MF>
where MF: Fn(&[u8], &[Vec<u8>]) -> Option<Vec<u8>>
{
    pub fn add<K, V>(&mut self, key: K, val: V) -> io::Result<()>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

        let ent = Entry::new(key, val);
        self.entry_bytes += ent.data.len();
        self.entries.push(ent);

        let entries_vec_size = self.entries.capacity() * size_of::<Entry>();
        if self.entry_bytes + entries_vec_size >= self.options.max_memory {
            self.write_chunk()?;
        }

        Ok(())
    }

    fn write_chunk(&mut self) -> io::Result<()> {
        let mut options = WriterOptions::default();
        options.set_compression_type(self.options.chunk_compression);

        let file = tempfile::tempfile()?;
        let mut writer = Writer::new(file, Some(options))?;

        self.entries.sort_unstable_by(|a, b| a.key().cmp(&b.key()));

        let mut current = None;
        for entry in self.entries.drain(..) {
            match current.as_mut() {
                None => {
                    let key = entry.key().to_vec();
                    let val = entry.val().to_vec();
                    current = Some((key, vec![val]));
                },
                Some((key, vals)) => {
                    if key == &entry.key() {
                        vals.push(entry.val().to_vec());
                    } else {
                        let merged_val = (self.options.merge)(&key, &vals).unwrap();
                        writer.add(&key, &merged_val)?;
                        key.clear();
                        vals.clear();
                        key.extend_from_slice(entry.key());
                        vals.push(entry.val().to_vec());
                    }
                }
            }
        }

        if let Some((key, vals)) = current.take() {
            let merged_val = (self.options.merge)(&key, &vals).unwrap();
            writer.add(&key, &merged_val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);
        self.entry_bytes = 0;

        Ok(())
    }

    pub fn write<W: io::Write>(self, writer: &mut Writer<W>) -> io::Result<()> {
        let mut iter = self.into_iter()?;
        while let Some((key, val)) = iter.next() {
            writer.add(key, val)?;
        }
        Ok(())
    }

    pub fn into_iter(mut self) -> io::Result<MergerIter<Mmap, MF>> {
        // Flush the pending unordered entries.
        self.write_chunk()?;

        let sources: io::Result<Vec<_>> = self.chunks.into_iter().map(|f| unsafe {
            let mmap = Mmap::map(&f)?;
            Ok(Reader::new(mmap, ReaderOptions::default()).unwrap())
        }).collect();
        let opt = MergerOptions { merge: self.options.merge };

        Ok(Merger::new(sources?, opt).into_merge_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        fn merge(_key: &[u8], vals: &[Vec<u8>]) -> Option<Vec<u8>> {
            Some(vals.iter().flatten().cloned().collect())
        }

        let opt = SorterOptions {
            max_memory: 1024*1024*1024,
            chunk_compression: CompressionType::Snappy,
            merge,
        };
        let mut sorter = Sorter::new(opt);

        sorter.add(b"hello", "kiki").unwrap();
        sorter.add(b"abstract", "lol").unwrap();
        sorter.add(b"allo", "lol").unwrap();
        sorter.add(b"abstract", "lol").unwrap();

        let mut bytes = Writer::memory(None);
        sorter.write(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let opt = ReaderOptions::default();
        let rdr = Reader::new(bytes.as_slice(), opt).unwrap();
        let mut iter = rdr.into_iter().unwrap();
        while let Some((key, val)) = iter.next() {
            match key {
                b"hello" => assert_eq!(val, b"kiki"),
                b"abstract" => assert_eq!(val, b"lollol"),
                b"allo" => assert_eq!(val, b"lol"),
                _ => panic!(),
            }
        }
    }
}
