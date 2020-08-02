use std::{fmt, io, error};

#[derive(Debug)]
pub enum Error<U=()> {
    Mtbl(MtblError),
    Io(io::Error),
    Merge(U),
}

impl<U> Error<U> {
    pub(crate) fn convert_merge_error<V>(self) -> Error<V> {
        match self {
            Error::Mtbl(mtbl) => Error::Mtbl(mtbl),
            Error::Io(io) => Error::Io(io),
            Error::Merge(_) => panic!("cannot convert a merge error"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Mtbl(mtbl) => write!(f, "{}", mtbl),
            Error::Io(io) => write!(f, "{}", io),
            Error::Merge(_) => f.write_str("<user merge error>"),
        }
    }
}

impl error::Error for Error { }

impl<U> From<io::Error> for Error<U> {
    fn from(err: io::Error) -> Error<U> {
        Error::Io(err)
    }
}

impl<U> From<MtblError> for Error<U> {
    fn from(err: MtblError) -> Error<U> {
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
