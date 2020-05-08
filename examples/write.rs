use std::env;
use std::fs::File;

use oxidized_mtbl::{Writer, WriterOptions, CompressionType};

fn main() {
    let path = env::args().nth(1).unwrap();
    let file = File::create(path).unwrap();

    let mut options = WriterOptions::default();
    options.set_compression_type(CompressionType::None);

    let mut writer = Writer::new(file, Some(options)).unwrap();

    for i in 0..300_000 {
        let key = format!("{:010}", i);
        let value = format!("{:010}", i).repeat(i / 10_000);
        writer.add(key, value).unwrap();
    }

    writer.finish().unwrap();
}
