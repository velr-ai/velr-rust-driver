//! Owned and borrowed property values shared by Velr's Rust APIs.
//!
//! [`PropertyValue`] represents scalars, lists, vectors, temporal values, spatial
//! values, and bytes. [`PropertyValueRef`] borrows an existing value without
//! cloning its contents. The `velr` driver reexports these types for applications
//! such as vector embedding callbacks. Query parameters in that driver use its
//! separate `QueryValue` type. Ordinary query rows use `CellRef`, with structured
//! values represented as JSON bytes rather than automatically converted into these types.
//!
//! Typed lists retain null elements and iterate in their original order:
//!
//! ```
//! use velr_types::{ListValue, PropertyValueRef};
//!
//! let values = ListValue::Int64(vec![Some(2), None, Some(5)]);
//! let total: i64 = values.iter().filter_map(|value| match value {
//!     PropertyValueRef::Integer(number) => Some(number),
//!     _ => None,
//! }).sum();
//! assert_eq!(total, 7);
//! ```
//!
//! Use checked temporal constructors or parse ISO text with `str::parse`.
//! Spatial values provide GeoJSON, WKT, and EWKT conversions. Numeric vectors
//! pair a coordinate type with matching storage; validate values constructed
//! directly before passing them to APIs that encode them. Fallible constructors
//! and conversions return [`EncodeError`] or [`DecodeError`].

pub mod codec;
pub mod cypher_order;
pub mod error;
pub mod list;
pub mod property;
pub mod render;
pub mod spatial;
pub mod storage;
pub mod tag;
pub mod temporal;
pub mod temporal_arithmetic;
pub mod vector;

pub use codec::{decode_property_value, encode_property_value};
pub use cypher_order::{storage_value_to_cypher_order_key, OrderKeyError};
pub use error::{DecodeError, EncodeError};
pub use list::{ListIter, ListValue};
pub use property::{PropertyValue, PropertyValueRef};
pub use render::{
    blob_sql_literal, bool_blob_bytes, property_value_to_display_text, property_value_to_json_text,
    storage_value_sql_literal, storage_value_to_display_text, storage_value_to_json_text,
    RenderError, BOOL_FALSE_SQLITE_BLOB_LITERAL, BOOL_TRUE_SQLITE_BLOB_LITERAL,
};
pub use spatial::{
    GeographyValue, GeometryShape, GeometryValue, LineStringValue, LinearRingValue, PointValue,
    PolygonValue, Position,
};
pub use storage::{StorageValue, StorageValueRef};
pub use temporal::{
    current_temporal_value, temporal_component, CurrentTemporalKind, DateValue, DurationMapParts,
    DurationValue, LocalDateTimeValue, LocalTimeValue, UtcOffsetValue, ZonedDateTimeValue,
    ZonedTimeValue,
};
pub use temporal_arithmetic::{
    temporal_add_property_values, temporal_div_property_values, temporal_mul_property_values,
    temporal_sub_property_values,
};
pub use vector::{VectorElem, VectorIter, VectorStorage, VectorType, VectorValue};
