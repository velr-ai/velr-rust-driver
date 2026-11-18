//! Owned and borrowed property values used by shared Rust APIs and vector embedding callbacks.
//!
//! These typed values are distinct from the `velr` driver's `QueryValue` parameters and
//! `CellRef` query results. Ordinary query rows expose structured values as JSON cells;
//! they do not automatically return `PropertyValue` variants.

#![warn(missing_docs)]

use crate::list::ListValue;
use crate::spatial::{GeographyValue, GeometryValue, PointValue};
use crate::temporal::{
    DateValue, DurationValue, LocalDateTimeValue, LocalTimeValue, ZonedDateTimeValue,
    ZonedTimeValue,
};
use crate::vector::VectorValue;

/// An owned property value, including scalar, temporal, spatial, list, vector, and byte values.
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    /// A null value.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed 64-bit integer.
    Integer(i64),
    /// A 64-bit floating-point value.
    Float(f64),
    /// UTF-8 text.
    String(String),

    /// A calendar date.
    Date(DateValue),
    /// A time without a timezone.
    LocalTime(LocalTimeValue),
    /// A time with a UTC offset.
    ZonedTime(ZonedTimeValue),
    /// A date and time without a timezone.
    LocalDateTime(LocalDateTimeValue),
    /// A date and time with a UTC offset and optional named zone.
    ZonedDateTime(ZonedDateTimeValue),
    /// A calendar and elapsed-time duration.
    Duration(DurationValue),

    /// A single point.
    Point(PointValue),
    /// A geometry value.
    Geometry(GeometryValue),
    /// A geography value.
    Geography(GeographyValue),

    /// A list of property values.
    List(ListValue),
    /// A numeric vector.
    Vector(VectorValue),
    /// An uninterpreted byte sequence.
    Bytes(Vec<u8>),
}

/// A borrowed view of a property value. References remain valid only while the owning value lives.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PropertyValueRef<'a> {
    /// A null value.
    Null,
    /// A Boolean value.
    Bool(bool),
    /// A signed 64-bit integer.
    Integer(i64),
    /// A 64-bit floating-point value.
    Float(f64),
    /// UTF-8 text.
    String(&'a str),

    /// A calendar date.
    Date(&'a DateValue),
    /// A time without a timezone.
    LocalTime(&'a LocalTimeValue),
    /// A time with a UTC offset.
    ZonedTime(&'a ZonedTimeValue),
    /// A date and time without a timezone.
    LocalDateTime(&'a LocalDateTimeValue),
    /// A date and time with a UTC offset and optional named zone.
    ZonedDateTime(&'a ZonedDateTimeValue),
    /// A calendar and elapsed-time duration.
    Duration(&'a DurationValue),

    /// A single point.
    Point(&'a PointValue),
    /// A geometry value.
    Geometry(&'a GeometryValue),
    /// A geography value.
    Geography(&'a GeographyValue),

    /// A list of property values.
    List(&'a ListValue),
    /// A numeric vector.
    Vector(&'a VectorValue),
    /// An uninterpreted byte sequence.
    Bytes(&'a [u8]),
}

impl PropertyValue {
    /// Borrows this value without cloning its strings, collections, or structured values.
    pub fn as_ref(&self) -> PropertyValueRef<'_> {
        match self {
            Self::Null => PropertyValueRef::Null,
            Self::Bool(v) => PropertyValueRef::Bool(*v),
            Self::Integer(v) => PropertyValueRef::Integer(*v),
            Self::Float(v) => PropertyValueRef::Float(*v),
            Self::String(v) => PropertyValueRef::String(v),
            Self::Date(v) => PropertyValueRef::Date(v),
            Self::LocalTime(v) => PropertyValueRef::LocalTime(v),
            Self::ZonedTime(v) => PropertyValueRef::ZonedTime(v),
            Self::LocalDateTime(v) => PropertyValueRef::LocalDateTime(v),
            Self::ZonedDateTime(v) => PropertyValueRef::ZonedDateTime(v),
            Self::Duration(v) => PropertyValueRef::Duration(v),
            Self::Point(v) => PropertyValueRef::Point(v),
            Self::Geometry(v) => PropertyValueRef::Geometry(v),
            Self::Geography(v) => PropertyValueRef::Geography(v),
            Self::List(v) => PropertyValueRef::List(v),
            Self::Vector(v) => PropertyValueRef::Vector(v),
            Self::Bytes(v) => PropertyValueRef::Bytes(v),
        }
    }
}
