use std::fs::File;
use std::mem::size_of;
use std::time::Instant;
use std::{cmp, io};

use log::debug;
use memmap::Mmap;

use crate::INITIAL_SORTER_VEC_SIZE;
use crate::{DEFAULT_COMPRESSION_LEVEL, DEFAULT_SORTER_MEMORY, MIN_SORTER_MEMORY};
use crate::{DEFAULT_NB_CHUNKS, MIN_NB_CHUNKS};
use crate::{Merger, MergerIter};
use crate::{Reader, Error};
use crate::{Writer, WriterBuilder, CompressionType};

#[derive(Debug, Clone, Copy)]
pub struct SorterBuilder<MF> {
    pub max_memory: usize,
    pub max_nb_chunks: usize,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: u32,
    pub merge: MF,
}

impl<MF> SorterBuilder<MF> {
    pub fn new(merge: MF) -> Self {
        SorterBuilder {
            max_memory: DEFAULT_SORTER_MEMORY,
            max_nb_chunks: DEFAULT_NB_CHUNKS,
            chunk_compression_type: CompressionType::Snappy,
            chunk_compression_level: DEFAULT_COMPRESSION_LEVEL,
            merge,
        }
    }

    pub fn max_memory(&mut self, memory: usize) -> &mut Self {
        self.max_memory = cmp::max(memory, MIN_SORTER_MEMORY);
        self
    }

    /// The maximum number of chunks on disk, if this number of chunks is reached
    /// they will be merged into a single chunk. Merging can reduce the disk usage.
    pub fn max_nb_chunks(&mut self, nb_chunks: usize) -> &mut Self {
        self.max_nb_chunks = cmp::max(nb_chunks, MIN_NB_CHUNKS);
        self
    }

    pub fn chunk_compression_type(&mut self, compression: CompressionType) -> &mut Self {
        self.chunk_compression_type = compression;
        self
    }

    pub fn chunk_compression_level(&mut self, level: u32) -> &mut Self {
        self.chunk_compression_level = level;
        self
    }

    pub fn build(self) -> Sorter<MF> {
        Sorter {
            chunks: Vec::new(),
            entries: Vec::with_capacity(INITIAL_SORTER_VEC_SIZE),
            entry_bytes: 0,
            max_memory: self.max_memory,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            merge: self.merge,
        }
    }
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
    max_memory: usize,
    max_nb_chunks: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: u32,
    merge: MF,
}

impl<MF> Sorter<MF> {
    pub fn builder(merge: MF) -> SorterBuilder<MF> {
        SorterBuilder::new(merge)
    }

    pub fn new(merge: MF) -> Sorter<MF> {
        SorterBuilder::new(merge).build()
    }
}

impl<MF, U> Sorter<MF>
where MF: Fn(&[u8], &[Vec<u8>]) -> Result<Vec<u8>, U>
{
    pub fn insert<K, V>(&mut self, key: K, val: V) -> Result<(), Error<U>>
    where K: AsRef<[u8]>,
          V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

        let ent = Entry::new(key, val);
        self.entry_bytes += ent.data.len();
        self.entries.push(ent);

        let entries_vec_size = self.entries.capacity() * size_of::<Entry>();
        if self.entry_bytes + entries_vec_size >= self.max_memory {
            self.write_chunk()?;
            if self.chunks.len() > self.max_nb_chunks {
                self.merge_chunks()?;
            }
        }

        Ok(())
    }

    fn write_chunk(&mut self) -> Result<(), Error<U>> {
        debug!("writing a chunk...");
        let before_write = Instant::now();

        let file = tempfile::tempfile()?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(file);

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
                        let merged_val = if vals.len() == 1 {
                            vals.pop().unwrap()
                        } else {
                            (self.merge)(&key, &vals).map_err(Error::Merge)?
                        };
                        writer.insert(&key, &merged_val)?;
                        key.clear();
                        vals.clear();
                        key.extend_from_slice(entry.key());
                        vals.push(entry.val().to_vec());
                    }
                }
            }
        }

        if let Some((key, mut vals)) = current.take() {
            let merged_val = if vals.len() == 1 {
                vals.pop().unwrap()
            } else {
                (self.merge)(&key, &vals).map_err(Error::Merge)?
            };
            writer.insert(&key, &merged_val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);
        self.entry_bytes = 0;

        debug!("writing a chunk took {:.02?}", before_write.elapsed());

        Ok(())
    }

    fn merge_chunks(&mut self) -> Result<(), Error<U>> {
        debug!("merging {} chunks...", self.chunks.len());
        let before_merge = Instant::now();
        let original_num_chunks = self.chunks.len();

        let file = tempfile::tempfile()?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(file);

        // Drain the chunks to mmap them and store them into a vector.
        let sources: Result<Vec<_>, Error<U>> = self.chunks.drain(..).map(|f| unsafe {
            let mmap = Mmap::map(&f)?;
            Reader::new(mmap).map_err(Error::convert_merge_error)
        }).collect();

        // Create a merger to merge all those chunks.
        let mut builder = Merger::builder(&self.merge);
        builder.extend(sources?);
        let merger = builder.build();

        let mut iter = merger.into_merge_iter().map_err(Error::convert_merge_error)?;
        while let Some(result) = iter.next() {
            let (key, val) = result?;
            writer.insert(key, val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);

        debug!("merging {} chunks took {:.02?}", original_num_chunks, before_merge.elapsed());

        Ok(())
    }

    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_iter()?;
        while let Some(result) = iter.next() {
            let (key, val) = result?;
            writer.insert(key, val)?;
        }
        Ok(())
    }

    pub fn into_iter(mut self) -> Result<MergerIter<Mmap, MF>, Error<U>> {
        // Flush the pending unordered entries.
        self.write_chunk()?;

        let sources: Result<Vec<_>, Error<U>> = self.chunks.into_iter().map(|f| unsafe {
            let mmap = Mmap::map(&f)?;
            Reader::new(mmap).map_err(Error::convert_merge_error)
        }).collect();

        let mut builder = Merger::builder(self.merge);
        builder.extend(sources?);

        builder.build().into_merge_iter().map_err(Error::convert_merge_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        fn merge(_key: &[u8], vals: &[Vec<u8>]) -> Result<Vec<u8>, ()> {
            assert_ne!(vals.len(), 1);
            Ok(vals.iter().flatten().cloned().collect())
        }

        let mut sorter = SorterBuilder::new(merge)
            .chunk_compression_type(CompressionType::Snappy)
            .build();

        sorter.insert(b"hello", "kiki").unwrap();
        sorter.insert(b"abstract", "lol").unwrap();
        sorter.insert(b"allo", "lol").unwrap();
        sorter.insert(b"abstract", "lol").unwrap();

        let mut bytes = WriterBuilder::new().memory();
        sorter.write_into(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let rdr = Reader::new(bytes.as_slice()).unwrap();
        let mut iter = rdr.into_iter().unwrap();
        while let Some(result) = iter.next() {
            let (key, val) = result.unwrap();
            match key {
                b"hello" => assert_eq!(val, b"kiki"),
                b"abstract" => assert_eq!(val, b"lollol"),
                b"allo" => assert_eq!(val, b"lol"),
                _ => panic!(),
            }
        }
    }
}
