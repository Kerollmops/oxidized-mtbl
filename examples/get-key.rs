use std::{env, str};
use std::fs::File;

use memmap::Mmap;
use oxidized_mtbl::{Reader, Error};

fn main() -> Result<(), Error> {
    let path = env::args().nth(1).unwrap();
    let key = env::args().nth(2).unwrap();
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let reader = Reader::new(mmap).unwrap();
    if let Some(val) = reader.get(key.as_bytes())? {
        let val = str::from_utf8(val.as_ref()).unwrap();
        println!(r#""{}" "{}""#, key, val);
    } else {
        println!("entry not found");
    }

    Ok(())
}
