use std::io::{self, Seek, SeekFrom};
use std::fs::File;

use oxidized_mtbl::*;
use memmap::Mmap;

// Here we concatenate all the values that we must merge.
fn concat_merge(_key: &[u8], vals: &[Vec<u8>]) -> Option<Vec<u8>> {
    Some(vals.iter().cloned().flatten().collect())
}

fn main() -> io::Result<()> {
    let file = File::create("target/first.mtbl")?;

    // We create a new writer to dump a first batch of entries to disk.
    let mut wtr = WriterBuilder::new()
        .compression_type(CompressionType::Snappy)
        .compression_level(5)
        .block_size(1024)
        .build(file);

    wtr.insert("abc", "hello1")?;
    wtr.insert("bcd", "hello2")?;
    wtr.insert("cde", "hello3")?;
    wtr.insert("def", "hello4")?;

    // When you can't or don't want to insert the entries in lexical order,
    // you can use the Sorter type, it will automatically sort them for you.
    let mut srt = SorterBuilder::new(concat_merge)
        .chunk_compression_type(CompressionType::Snappy)
        .chunk_compression_level(5)
        .build();

    srt.insert("def", "bonjour4")?;
    srt.insert("bcd", "bonjour2")?;
    srt.insert("cde", "bonjour3")?;
    srt.insert("abc", "bonjour1")?;

    // We flush the writer to disk and retrieve the underlying file.
    // We seek at the begining of the file and create a reader from it.
    let mut file = wtr.into_inner()?;
    file.seek(SeekFrom::Start(0))?;
    let mmap = unsafe { Mmap::map(&file)? };
    let _first_rdr = Reader::new(mmap).unwrap();

    // Here we use an helper method to directly read the batch
    // of entries we wrote into a Vec.
    let _second_rdr = srt.into_iter()?;

    // let mgr = MergerBuilder::new(concat_merge)
    //     .add(first_rdr)
    //     .add(second_rdr)
    //     .build();

    // // You can either iterate over the merged entries.
    // // let mut iter = mgr.into_iter();
    // // while let Some((_key, _val)) = iter.next() {
    // //     // ...
    // // }

    // // Or you can write them into a new Writer.
    // let file = File::create("merged.mtbl")?;
    // let mut writer = Writer::new(file)?;
    // mgr.write_into(&mut writer)?;

    Ok(())
}
