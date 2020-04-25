use std::fmt;

#[derive(Debug)]
pub enum Error {
    InvalidMetadataSize,
    InvalidIndexBlockOffset,
    InvalidIndexLength,
    InvalidFormatVersion,
    InvalidCompressionAlgorithm,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidMetadataSize => f.write_str("invalid metadata size"),
            Error::InvalidIndexBlockOffset => f.write_str("Invalid index block offset"),
            Error::InvalidIndexLength => f.write_str("invalid index length"),
            Error::InvalidFormatVersion => f.write_str("invalid format version"),
            Error::InvalidCompressionAlgorithm => f.write_str("invalid compression algorithm"),
        }
    }
}
