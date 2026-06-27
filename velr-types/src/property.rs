use crate::list::ListValue;
use crate::spatial::{GeographyValue, GeometryValue, PointValue};
use crate::temporal::{
    DateValue, DurationValue, LocalDateTimeValue, LocalTimeValue, ZonedDateTimeValue,
    ZonedTimeValue,
};
use crate::vector::VectorValue;

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),

    Date(DateValue),
    LocalTime(LocalTimeValue),
    ZonedTime(ZonedTimeValue),
    LocalDateTime(LocalDateTimeValue),
    ZonedDateTime(ZonedDateTimeValue),
    Duration(DurationValue),

    Point(PointValue),
    Geometry(GeometryValue),
    Geography(GeographyValue),

    List(ListValue),
    Vector(VectorValue),
    Bytes(Vec<u8>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum PropertyValueRef<'a> {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(&'a str),

    Date(&'a DateValue),
    LocalTime(&'a LocalTimeValue),
    ZonedTime(&'a ZonedTimeValue),
    LocalDateTime(&'a LocalDateTimeValue),
    ZonedDateTime(&'a ZonedDateTimeValue),
    Duration(&'a DurationValue),

    Point(&'a PointValue),
    Geometry(&'a GeometryValue),
    Geography(&'a GeographyValue),

    List(&'a ListValue),
    Vector(&'a VectorValue),
    Bytes(&'a [u8]),
}

impl PropertyValue {
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
