use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    EmptyInput,
    UnknownTag(u8),
    InvalidUtf8,
    InvalidJson(String),
    InvalidTemporal(String),
    InvalidSpatial(String),
    InvalidList(String),
    InvalidVector(String),
    InvalidStorageClass(&'static str),
    UnexpectedTag { expected: &'static str, actual: u8 },
    Truncated,
    NonCanonical(&'static str),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "empty input"),
            Self::UnknownTag(tag) => write!(f, "unknown tag 0x{tag:02x}"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8"),
            Self::InvalidJson(msg) => write!(f, "invalid JSON: {msg}"),
            Self::InvalidTemporal(msg) => write!(f, "invalid temporal value: {msg}"),
            Self::InvalidSpatial(msg) => write!(f, "invalid spatial value: {msg}"),
            Self::InvalidList(msg) => write!(f, "invalid list value: {msg}"),
            Self::InvalidVector(msg) => write!(f, "invalid vector value: {msg}"),
            Self::InvalidStorageClass(msg) => write!(f, "invalid storage class: {msg}"),
            Self::UnexpectedTag { expected, actual } => {
                write!(f, "unexpected tag 0x{actual:02x}, expected {expected}")
            }
            Self::Truncated => write!(f, "truncated input"),
            Self::NonCanonical(msg) => write!(f, "non-canonical encoding: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    InvalidTemporal(&'static str),
    InvalidSpatial(&'static str),
    InvalidList(&'static str),
    InvalidVector(&'static str),
    NonCanonicalValue(&'static str),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTemporal(msg) => write!(f, "invalid temporal value: {msg}"),
            Self::InvalidSpatial(msg) => write!(f, "invalid spatial value: {msg}"),
            Self::InvalidList(msg) => write!(f, "invalid list value: {msg}"),
            Self::InvalidVector(msg) => write!(f, "invalid vector value: {msg}"),
            Self::NonCanonicalValue(msg) => write!(f, "non-canonical value: {msg}"),
        }
    }
}

impl std::error::Error for EncodeError {}
