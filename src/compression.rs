use std::borrow::Cow;
use std::io;

#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u64)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Zlib = 2,
    Lz4 = 3,
    Lz4hc = 4,
    Zstd = 5,
}

pub fn decompress(type_: CompressionType, data: &[u8]) -> io::Result<Cow<[u8]>> {
    match type_ {
        CompressionType::None => Ok(Cow::Borrowed(data)),
        CompressionType::Zlib => zlib_decompress(data),
        CompressionType::Snappy => snappy_decompress(data),
        CompressionType::Zstd => zstd_decompress(data),
        other => {
            let error = format!("unsupported {:?} decompression", other);
            Err(io::Error::new(io::ErrorKind::Other, error))
        },
    }
}

#[cfg(feature = "zlib")]
fn zlib_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    use std::io::Read;
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "zlib"))]
fn zlib_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zlib decompression"))
}

#[cfg(feature = "snappy")]
fn snappy_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder.decompress_vec(data).map_err(Into::into).map(Cow::Owned)
}

#[cfg(not(feature = "snappy"))]
fn snappy_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported snappy decompression"))
}

#[cfg(feature = "zstd")]
fn zstd_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    let mut buffer = Vec::new();
    zstd::stream::copy_decode(data, &mut buffer)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "zstd"))]
fn zstd_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zstd decompression"))
}
