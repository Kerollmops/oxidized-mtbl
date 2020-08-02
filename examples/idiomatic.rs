use std::fs::OpenOptions;

use oxidized_mtbl::*;
use memmap::Mmap;

// Here we concatenate all the values that we must merge.
fn concat_merge(_key: &[u8], vals: &[Vec<u8>]) -> Option<Vec<u8>> {
    Some(vals.iter().cloned().flatten().collect())
}

fn main() -> Result<(), Error> {
    let mut file_options = OpenOptions::new();
    file_options.read(true).write(true).truncate(true).create(true);

    let file = file_options.open("target/first.mtbl")?;

    // We create a new writer to dump a first batch of entries to disk.
    let mut first_wtr = WriterBuilder::new()
        .compression_type(CompressionType::Snappy)
        .compression_level(5)
        .block_size(1024)
        .build(file);

    first_wtr.insert("abc", "hello1")?;
    first_wtr.insert("bcd", "hello2")?;
    first_wtr.insert("cde", "hello3")?;
    first_wtr.insert("def", "hello4")?;

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
    let file = first_wtr.into_inner()?;
    let mmap = unsafe { Mmap::map(&file)? };
    let first_rdr = Reader::new(mmap).unwrap();

    // Here we use an helper method to directly read the batch
    // of entries we wrote into a Vec.
    let file = file_options.open("target/second.mtbl")?;
    let mut second_wtr = Writer::new(file);
    srt.write_into(&mut second_wtr)?;

    let file = second_wtr.into_inner()?;
    let mmap = unsafe { Mmap::map(&file)? };
    let second_rdr = Reader::new(mmap).unwrap();

    let mut builder = MergerBuilder::new(concat_merge);
    builder.add(first_rdr).add(second_rdr);
    let mgr = builder.build();

    // You can either iterate over the merged entries.
    // let mut iter = mgr.into_iter();
    // while let Some((_key, _val)) = iter.next() {
    //     // ...
    // }

    // Or you can write them into a new Writer.
    let file = file_options.open("target/merged.mtbl")?;
    let mut writer = Writer::new(file);
    mgr.write_into(&mut writer)?;
    writer.finish()?;

    Ok(())
}
