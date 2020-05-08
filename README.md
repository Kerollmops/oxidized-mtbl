# oxidized-mtbl

A Rust version of the mtbl immutable key-value store.
This is a port of the [farsightsec mtbl implementation](https://github.com/farsightsec/mtbl).

  - [x] Read and Iterate over the database
  - [x] Support blocks decompression
  - [x] Create an immutable key-value database
  - [x] Support most of the compression algorithms
    - [ ] lz4
    - [ ] lz4hc
    - [x] snappy
    - [x] zlib
    - [x] zstd
