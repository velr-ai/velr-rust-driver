use std::fmt::Write;

use crate::error::{DecodeError, EncodeError};
use crate::list::ListValue;
use crate::property::{PropertyValue, PropertyValueRef};
use crate::storage::StorageValueRef;
use crate::tag;
use crate::vector::{VectorElem, VectorValue};

pub const BOOL_FALSE_SQLITE_BLOB_LITERAL: &str = "X'00'";
pub const BOOL_TRUE_SQLITE_BLOB_LITERAL: &str = "X'01'";

pub fn bool_blob_bytes(value: bool) -> &'static [u8] {
    if value {
        &[tag::BOOL_TRUE]
    } else {
        &[tag::BOOL_FALSE]
    }
}

pub fn blob_sql_literal(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(3 + bytes.len() * 2);
    out.push_str("X'");
    for byte in bytes {
        let _ = write!(out, "{byte:02X}");
    }
    out.push('\'');
    out
}

pub fn storage_value_sql_literal(value: StorageValueRef<'_>) -> String {
    match value {
        StorageValueRef::Null => "NULL".to_string(),
        StorageValueRef::Integer(v) => v.to_string(),
        StorageValueRef::Real(v) => {
            if v.is_finite() && v.fract() == 0.0 {
                format!("{v:.1}")
            } else {
                v.to_string()
            }
        }
        StorageValueRef::Text(text) => sql_quote(text),
        StorageValueRef::Blob(bytes) => blob_sql_literal(bytes),
    }
}

#[derive(Debug)]
pub enum RenderError {
    Decode(DecodeError),
    Encode(EncodeError),
    NonCanonical(&'static str),
}

impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(err) => err.fmt(f),
            Self::Encode(err) => err.fmt(f),
            Self::NonCanonical(msg) => write!(f, "non-canonical value: {msg}"),
        }
    }
}

impl std::error::Error for RenderError {}

impl From<DecodeError> for RenderError {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl From<EncodeError> for RenderError {
    fn from(value: EncodeError) -> Self {
        Self::Encode(value)
    }
}

pub fn storage_value_to_json_text(value: StorageValueRef<'_>) -> Result<String, RenderError> {
    match value {
        StorageValueRef::Null => Ok("null".to_string()),
        StorageValueRef::Integer(v) => Ok(v.to_string()),
        StorageValueRef::Real(v) => {
            if !v.is_finite() {
                return Err(RenderError::NonCanonical(
                    "non-finite floats are not canonical",
                ));
            }
            let mut out = String::new();
            push_json_real(&mut out, v);
            Ok(out)
        }
        StorageValueRef::Text(text) => Ok(json_quote(text)),
        StorageValueRef::Blob(blob) => blob_to_json_text(blob),
    }
}

pub fn storage_value_to_display_text(value: StorageValueRef<'_>) -> Result<String, RenderError> {
    match value {
        StorageValueRef::Null => Ok("null".to_string()),
        StorageValueRef::Integer(v) => Ok(v.to_string()),
        StorageValueRef::Real(v) => {
            if !v.is_finite() {
                return Err(RenderError::NonCanonical(
                    "non-finite floats are not canonical",
                ));
            }
            let mut out = String::new();
            push_json_f64(&mut out, v);
            Ok(out)
        }
        StorageValueRef::Text(text) => Ok(text.to_string()),
        StorageValueRef::Blob(blob) => blob_to_display_text(blob),
    }
}

pub fn property_value_to_json_text(value: &PropertyValue) -> Result<String, RenderError> {
    let mut out = String::new();
    append_property_json(&mut out, value.as_ref())?;
    Ok(out)
}

pub fn property_value_to_display_text(value: &PropertyValue) -> Result<String, RenderError> {
    let mut out = String::new();
    append_property_display(&mut out, value.as_ref())?;
    Ok(out)
}

fn blob_to_json_text(blob: &[u8]) -> Result<String, RenderError> {
    if let Some(text) = tagged_json_payload(blob)? {
        return Ok(text);
    }
    if let Some(text) = tagged_temporal_payload(blob)? {
        return Ok(json_quote(&text));
    }

    let value = crate::codec::decode_property_value(StorageValueRef::Blob(blob))?;
    property_value_to_json_text(&value)
}

fn blob_to_display_text(blob: &[u8]) -> Result<String, RenderError> {
    if let Some(text) = tagged_json_payload(blob)? {
        return Ok(text);
    }
    if let Some(text) = tagged_temporal_payload(blob)? {
        return Ok(text);
    }

    let value = crate::codec::decode_property_value(StorageValueRef::Blob(blob))?;
    property_value_to_display_text(&value)
}

fn tagged_json_payload(blob: &[u8]) -> Result<Option<String>, RenderError> {
    let Some(tag) = blob.first().copied() else {
        return Ok(None);
    };

    let raw_json_payload = matches!(
        tag,
        tag::JSON | tag::MAP_JSON | tag::NODE_JSON | tag::RELATIONSHIP_JSON | tag::PATH_JSON
    );
    if !raw_json_payload {
        return Ok(None);
    }

    let payload = std::str::from_utf8(&blob[1..]).map_err(|_| DecodeError::InvalidUtf8)?;
    Ok(Some(payload.to_string()))
}

fn tagged_temporal_payload(blob: &[u8]) -> Result<Option<String>, RenderError> {
    let Some(tag) = blob.first().copied() else {
        return Ok(None);
    };

    let temporal_payload = matches!(
        tag,
        tag::DATE
            | tag::LOCAL_TIME
            | tag::ZONED_TIME
            | tag::LOCAL_DATETIME
            | tag::ZONED_DATETIME
            | tag::DURATION
    );
    if !temporal_payload {
        return Ok(None);
    }

    let payload = std::str::from_utf8(&blob[1..]).map_err(|_| DecodeError::InvalidUtf8)?;
    Ok(Some(payload.to_string()))
}

fn append_property_json(out: &mut String, value: PropertyValueRef<'_>) -> Result<(), RenderError> {
    if let Some(text) = crate::codec::temporal_value_text(value) {
        push_json_string(out, &text);
        return Ok(());
    }

    match value {
        PropertyValueRef::Null => out.push_str("null"),
        PropertyValueRef::Bool(v) => out.push_str(if v { "true" } else { "false" }),
        PropertyValueRef::Integer(v) => {
            let _ = write!(out, "{v}");
        }
        PropertyValueRef::Float(v) => {
            if !v.is_finite() {
                return Err(RenderError::NonCanonical(
                    "non-finite floats are not canonical",
                ));
            }
            push_json_real(out, v);
        }
        PropertyValueRef::String(v) => push_json_string(out, v),
        PropertyValueRef::Date(_)
        | PropertyValueRef::LocalTime(_)
        | PropertyValueRef::ZonedTime(_)
        | PropertyValueRef::LocalDateTime(_)
        | PropertyValueRef::ZonedDateTime(_)
        | PropertyValueRef::Duration(_) => unreachable!("temporal values are handled above"),
        PropertyValueRef::Point(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::Geometry(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::Geography(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::List(v) => append_list_json(out, v)?,
        PropertyValueRef::Vector(v) => append_vector_json(out, v),
        PropertyValueRef::Bytes(v) => push_json_string(out, &hex_string(v)),
    }
    Ok(())
}

fn append_property_display(
    out: &mut String,
    value: PropertyValueRef<'_>,
) -> Result<(), RenderError> {
    if let Some(text) = crate::codec::temporal_value_text(value) {
        out.push_str(&text);
        return Ok(());
    }

    match value {
        PropertyValueRef::Null => out.push_str("null"),
        PropertyValueRef::Bool(v) => out.push_str(if v { "true" } else { "false" }),
        PropertyValueRef::Integer(v) => {
            let _ = write!(out, "{v}");
        }
        PropertyValueRef::Float(v) => {
            if !v.is_finite() {
                return Err(RenderError::NonCanonical(
                    "non-finite floats are not canonical",
                ));
            }
            push_json_f64(out, v);
        }
        PropertyValueRef::String(v) => out.push_str(v),
        PropertyValueRef::Date(_)
        | PropertyValueRef::LocalTime(_)
        | PropertyValueRef::ZonedTime(_)
        | PropertyValueRef::LocalDateTime(_)
        | PropertyValueRef::ZonedDateTime(_)
        | PropertyValueRef::Duration(_) => unreachable!("temporal values are handled above"),
        PropertyValueRef::Point(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::Geometry(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::Geography(v) => out.push_str(&v.to_geojson_string()?),
        PropertyValueRef::List(v) => append_list_json(out, v)?,
        PropertyValueRef::Vector(v) => append_vector_json(out, v),
        PropertyValueRef::Bytes(v) => out.push_str(&hex_string(v)),
    }
    Ok(())
}

fn append_list_json(out: &mut String, list: &ListValue) -> Result<(), RenderError> {
    out.push('[');
    for (idx, item) in list.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        append_property_json(out, item)?;
    }
    out.push(']');
    Ok(())
}

fn append_vector_json(out: &mut String, vector: &VectorValue) {
    out.push('[');
    for (idx, item) in vector.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        match item {
            VectorElem::I8(v) => {
                let _ = write!(out, "{v}");
            }
            VectorElem::I16(v) => {
                let _ = write!(out, "{v}");
            }
            VectorElem::I32(v) => {
                let _ = write!(out, "{v}");
            }
            VectorElem::I64(v) => {
                let _ = write!(out, "{v}");
            }
            VectorElem::F32(v) => push_json_real(out, v as f64),
            VectorElem::F64(v) => push_json_real(out, v),
        }
    }
    out.push(']');
}

fn push_json_real(out: &mut String, f: f64) {
    if f == 0.0 && f.is_sign_negative() {
        out.push_str("-0.0");
    } else if f.fract() == 0.0 {
        let _ = write!(out, "{f:.1}");
    } else {
        push_json_f64(out, f);
    }
}

fn push_json_f64(out: &mut String, f: f64) {
    let _ = write!(out, "{f}");
}

fn json_quote(text: &str) -> String {
    let mut out = String::new();
    push_json_string(&mut out, text);
    out
}

fn sql_quote(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

fn push_json_string(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1F}' => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        let _ = write!(out, "{byte:02X}");
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::{
        encode_property_value, DateValue, GeographyValue, GeometryValue, ListValue, LocalTimeValue,
        PointValue, PropertyValue, StorageValue, StorageValueRef, VectorStorage, VectorType,
        VectorValue, ZonedTimeValue,
    };

    use super::{
        blob_sql_literal, bool_blob_bytes, property_value_to_display_text,
        property_value_to_json_text, storage_value_sql_literal, storage_value_to_display_text,
        storage_value_to_json_text, BOOL_FALSE_SQLITE_BLOB_LITERAL, BOOL_TRUE_SQLITE_BLOB_LITERAL,
    };

    #[test]
    fn canonical_bool_helpers_match_contract() {
        assert_eq!(bool_blob_bytes(true), &[crate::tag::BOOL_TRUE]);
        assert_eq!(bool_blob_bytes(false), &[crate::tag::BOOL_FALSE]);
        assert_eq!(BOOL_TRUE_SQLITE_BLOB_LITERAL, "X'01'");
        assert_eq!(BOOL_FALSE_SQLITE_BLOB_LITERAL, "X'00'");
        assert_eq!(blob_sql_literal(bool_blob_bytes(true)), "X'01'");
    }

    #[test]
    fn storage_render_handles_native_scalars() {
        assert_eq!(
            storage_value_to_json_text(StorageValueRef::Integer(7)).unwrap(),
            "7"
        );
        assert_eq!(
            storage_value_to_json_text(StorageValueRef::Real(1.0)).unwrap(),
            "1.0"
        );
        assert_eq!(
            storage_value_to_json_text(StorageValueRef::Text("Ada")).unwrap(),
            "\"Ada\""
        );
        assert_eq!(
            storage_value_to_display_text(StorageValueRef::Text("Ada")).unwrap(),
            "Ada"
        );
    }

    #[test]
    fn storage_render_handles_canonical_blobs() {
        let list = PropertyValue::List(ListValue::Bool(vec![Some(true), None, Some(false)]));
        let vector = PropertyValue::Vector(VectorValue {
            coord_type: VectorType::Float64,
            values: VectorStorage::F64(vec![1.5, 2.5]),
        });
        let date = PropertyValue::Date("2026-04-06".parse::<DateValue>().unwrap());
        let point = PropertyValue::Point(
            PointValue::from_geojson_str(r#"{"type":"Point","coordinates":[12.5,55.7]}"#).unwrap(),
        );
        let geometry =
            PropertyValue::Geometry(GeometryValue::from_wkt_str("LINESTRING(0 0, 1 1)").unwrap());
        let geography = PropertyValue::Geography(
            GeographyValue::from_wkt_str("POLYGON((0 0, 1 0, 1 1, 0 0))").unwrap(),
        );

        for (value, expected_fragment) in [
            (list, "[true,null,false]"),
            (vector, "[1.5,2.5]"),
            (date, "\"2026-04-06\""),
            (point, "\"type\":\"Point\""),
            (geometry, "\"type\":\"LineString\""),
            (geography, "\"type\":\"Polygon\""),
        ] {
            let storage = encode_property_value(&value).unwrap();
            let json = storage_value_to_json_text(storage.as_ref()).unwrap();
            assert!(
                json.contains(expected_fragment),
                "json={json:?} expected fragment {expected_fragment:?}"
            );
        }
    }

    #[test]
    fn storage_render_rejects_untagged_blob_payload() {
        let err = storage_value_to_json_text(StorageValueRef::Blob(br#"[1,{"name":"Ada"}]"#))
            .expect_err("untagged blob should be rejected");
        assert!(
            err.to_string().contains("unknown tag")
                || err.to_string().contains("unexpected tag")
                || err.to_string().contains("unsupported")
                || err.to_string().contains("invalid"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn property_render_handles_values() {
        let list = PropertyValue::List(ListValue::Bool(vec![Some(true), None, Some(false)]));
        let point = PropertyValue::Point(
            PointValue::from_geojson_str(r#"{"type":"Point","coordinates":[12.5,55.7]}"#).unwrap(),
        );

        let rendered = property_value_to_json_text(&list).unwrap();
        assert_eq!(rendered, "[true,null,false]");

        let rendered = property_value_to_json_text(&PropertyValue::LocalTime(
            LocalTimeValue::new(10, 35, 0, 0).unwrap(),
        ))
        .unwrap();
        assert_eq!(rendered, "\"10:35\"");

        let rendered = property_value_to_display_text(&PropertyValue::ZonedTime(
            "10:35:00-08:00".parse::<ZonedTimeValue>().unwrap(),
        ))
        .unwrap();
        assert_eq!(rendered, "10:35-08:00");

        let rendered = property_value_to_display_text(&point).unwrap();
        assert!(rendered.contains(r#""type":"Point""#));

        let storage = StorageValue::Text("Ada".into());
        assert_eq!(
            storage_value_to_display_text(storage.as_ref()).unwrap(),
            "Ada"
        );
    }

    #[test]
    fn storage_sql_literal_renders_all_storage_classes() {
        assert_eq!(storage_value_sql_literal(StorageValueRef::Null), "NULL");
        assert_eq!(storage_value_sql_literal(StorageValueRef::Integer(7)), "7");
        assert_eq!(storage_value_sql_literal(StorageValueRef::Real(1.0)), "1.0");
        assert_eq!(
            storage_value_sql_literal(StorageValueRef::Text("Ada's")),
            "'Ada''s'"
        );
        assert_eq!(
            storage_value_sql_literal(StorageValueRef::Blob(&[0x01, 0xAB, 0x00])),
            "X'01AB00'"
        );
    }
}
