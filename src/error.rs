use std::{fmt, io, error};

#[derive(Debug)]
pub enum Error {
    Mtbl(MtblError),
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Mtbl(mtbl) => write!(f, "{}", mtbl),
            Error::Io(io) => write!(f, "{}", io),
        }
    }
}

impl error::Error for Error { }

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<MtblError> for Error {
    fn from(err: MtblError) -> Error {
        Error::Mtbl(err)
    }
}

#[derive(Debug)]
pub enum MtblError {
    InvalidMetadataSize,
    InvalidIndexBlockOffset,
    InvalidIndexLength,
    InvalidFormatVersion,
    InvalidCompressionAlgorithm,
    InvalidBlock,
}

impl fmt::Display for MtblError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MtblError::InvalidMetadataSize => f.write_str("invalid metadata size"),
            MtblError::InvalidIndexBlockOffset => f.write_str("Invalid index block offset"),
            MtblError::InvalidIndexLength => f.write_str("invalid index length"),
            MtblError::InvalidFormatVersion => f.write_str("invalid format version"),
            MtblError::InvalidCompressionAlgorithm => f.write_str("invalid compression algorithm"),
            MtblError::InvalidBlock => f.write_str("invalid block"),
        }
    }
}

impl error::Error for MtblError { }
