use std::env;
use std::fs::File;

use oxidized_mtbl::{WriterBuilder, CompressionType};

fn main() {
    let path = env::args().nth(1).unwrap();
    let file = File::create(path).unwrap();

    let mut writer = WriterBuilder::new()
        .compression_type(CompressionType::Snappy)
        .build(file);

    for i in 0..300_000 {
        let key = format!("{:010}", i);
        let value = format!("{:010}", i).repeat(i / 10_000);
        writer.insert(key, value).unwrap();
    }

    writer.finish().unwrap();
}
