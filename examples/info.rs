use std::env;
use std::fs::File;

use memmap::Mmap;
use oxidized_mtbl::{Reader, ReaderOptions};

fn main() {
    let path = env::args().nth(1).unwrap();
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let options = ReaderOptions::default();
    let reader = Reader::new(&mmap, options).unwrap();
    let metadata = reader.metadata();
    println!("{:#?}", metadata);
}
