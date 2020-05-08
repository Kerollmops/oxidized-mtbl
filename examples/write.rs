use std::env;
use std::fs::File;

use oxidized_mtbl::{Writer, WriterOptions};

fn main() {
    let path = env::args().nth(1).unwrap();
    let mut file = File::create(path).unwrap();

    let options = WriterOptions::default();
    let mut writer = Writer::new(&mut file, Some(options)).unwrap();

    writer.add(b"comment", b"ca va?").unwrap();
    writer.add(b"hello", b"les potes").unwrap();

    writer.finish().unwrap();
}
