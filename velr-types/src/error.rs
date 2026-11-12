//! Errors returned when constructing, encoding, or decoding property values.
//!
//! [`EncodeError`] reports an invalid value supplied to a value constructor or encoder.
//! [`DecodeError`] reports input that cannot be read as the requested value type.
//! Query validation and execution failures are returned by the database driver's error type.

#![warn(missing_docs)]

use std::fmt;

/// A property value could not be decoded from its input representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// No input bytes were supplied.
    EmptyInput,
    /// The input identifies an unknown value type.
    UnknownTag(u8),
    /// Text bytes are not valid UTF-8.
    InvalidUtf8,
    /// JSON text is invalid; the message describes the failure.
    InvalidJson(String),
    /// A temporal value is invalid; the message describes the failure.
    InvalidTemporal(String),
    /// A spatial value is invalid; the message describes the failure.
    InvalidSpatial(String),
    /// A list value is invalid; the message describes the failure.
    InvalidList(String),
    /// A vector value is invalid; the message describes the failure.
    InvalidVector(String),
    /// The input representation does not support the requested value type.
    InvalidStorageClass(&'static str),
    /// A value has a different type identifier from the one required by the decoder.
    UnexpectedTag {
        /// The expected value type or type family.
        expected: &'static str,
        /// The type identifier found in the input.
        actual: u8,
    },
    /// The input ends before a complete value can be read.
    Truncated,
    /// The input is not in an accepted canonical form.
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

/// A property value could not be constructed or encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// Temporal components are invalid or out of range.
    InvalidTemporal(&'static str),
    /// Spatial coordinates or shape constraints are invalid.
    InvalidSpatial(&'static str),
    /// List elements cannot be represented in the requested format.
    InvalidList(&'static str),
    /// Vector length, coordinate type, or storage is invalid.
    InvalidVector(&'static str),
    /// The value has no accepted canonical representation, for example a non-finite vector coordinate.
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
