use std::{env, str};
use std::fs::File;

use memmap::Mmap;
use oxidized_mtbl::{Reader, ReaderOptions};

fn main() {
    let path = env::args().nth(1).unwrap();
    let file = File::open(path).unwrap();
    let content = unsafe { Mmap::map(&file).unwrap() };

    let options = ReaderOptions::default();
    let reader = Reader::new(&content[..], options).unwrap();

    let mut iter = reader.into_iter().unwrap();

    while let Some((key, val)) = iter.next() {
        let key = str::from_utf8(key).unwrap();
        let val = str::from_utf8(val).unwrap();

        println!(r#""{}" "{}""#, key, val);
    }
}
