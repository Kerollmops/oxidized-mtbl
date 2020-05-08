use std::env;
use std::fs::File;

use oxidized_mtbl::{Writer, WriterOptions, CompressionType};

fn main() {
    let path = env::args().nth(1).unwrap();
    let file = File::create(path).unwrap();

    let mut options = WriterOptions::default();
    options.set_compression_type(CompressionType::Zlib);

    let mut writer = Writer::new(file, Some(options)).unwrap();

    for i in 0..300_000 {
        let string = format!("{:010}", i);
        let bytes = string.as_bytes();
        writer.add(bytes, bytes).unwrap();
    }

    writer.finish().unwrap();
}
