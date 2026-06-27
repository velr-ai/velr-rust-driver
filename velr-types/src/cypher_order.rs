use crate::codec::decode_property_value;
use crate::list::ListValue;
use crate::property::PropertyValueRef;
use crate::storage::StorageValueRef;
use crate::tag;
use serde_json::Value;

const RANK_EMPTY: u8 = 0x00;
const RANK_MAP: u8 = 0x01;
const RANK_NODE: u8 = 0x02;
const RANK_RELATIONSHIP: u8 = 0x03;
const RANK_LIST: u8 = 0x04;
const RANK_PATH: u8 = 0x05;
const RANK_OTHER: u8 = 0x06;
const RANK_STRING: u8 = 0x07;
const RANK_BOOLEAN: u8 = 0x08;
const RANK_NUMBER: u8 = 0x09;
const RANK_NAN: u8 = 0x0a;
const RANK_NULL: u8 = 0x0b;

const OTHER_TEMPORAL_DATE: u8 = 0x10;
const OTHER_TEMPORAL_LOCAL_TIME: u8 = 0x11;
const OTHER_TEMPORAL_ZONED_TIME: u8 = 0x12;
const OTHER_TEMPORAL_LOCAL_DATETIME: u8 = 0x13;
const OTHER_TEMPORAL_ZONED_DATETIME: u8 = 0x14;
const OTHER_TEMPORAL_DURATION: u8 = 0x15;
const OTHER_SPATIAL_POINT: u8 = 0x20;
const OTHER_SPATIAL_GEOMETRY: u8 = 0x21;
const OTHER_SPATIAL_GEOGRAPHY: u8 = 0x22;
const OTHER_VECTOR: u8 = 0x40;
const OTHER_BYTES: u8 = 0x41;

const NUM_EXP_BIAS: i32 = 1074;
const NUM_INF_EXPONENT: u16 = u16::MAX;

#[derive(Debug)]
pub enum OrderKeyError {
    Decode(crate::error::DecodeError),
    Encode(crate::error::EncodeError),
    Json(serde_json::Error),
    InvalidUtf8(std::str::Utf8Error),
}

impl std::fmt::Display for OrderKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(err) => err.fmt(f),
            Self::Encode(err) => err.fmt(f),
            Self::Json(err) => err.fmt(f),
            Self::InvalidUtf8(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for OrderKeyError {}

impl From<crate::error::DecodeError> for OrderKeyError {
    fn from(value: crate::error::DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl From<crate::error::EncodeError> for OrderKeyError {
    fn from(value: crate::error::EncodeError) -> Self {
        Self::Encode(value)
    }
}

impl From<serde_json::Error> for OrderKeyError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<std::str::Utf8Error> for OrderKeyError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(value)
    }
}

pub fn storage_value_to_cypher_order_key(
    value: StorageValueRef<'_>,
) -> Result<Vec<u8>, OrderKeyError> {
    let mut out = Vec::with_capacity(initial_key_capacity(value));
    append_storage_key(&mut out, value)?;
    Ok(out)
}

fn initial_key_capacity(value: StorageValueRef<'_>) -> usize {
    match value {
        StorageValueRef::Null | StorageValueRef::Integer(_) | StorageValueRef::Real(_) => 12,
        StorageValueRef::Text(v) => 2 + v.len().saturating_mul(2),
        StorageValueRef::Blob(v) => v.len().saturating_mul(2).saturating_add(2).min(128).max(16),
    }
}

fn append_storage_key(out: &mut Vec<u8>, value: StorageValueRef<'_>) -> Result<(), OrderKeyError> {
    match value {
        StorageValueRef::Null => out.push(RANK_NULL),
        StorageValueRef::Integer(v) => append_i64_key(out, v),
        StorageValueRef::Real(v) => append_f64_key(out, v),
        StorageValueRef::Text(v) => append_string_key(out, v),
        StorageValueRef::Blob(blob) => append_blob_key(out, blob)?,
    }
    Ok(())
}

fn append_blob_key(out: &mut Vec<u8>, blob: &[u8]) -> Result<(), OrderKeyError> {
    let Some(first) = blob.first().copied() else {
        out.push(RANK_OTHER);
        append_bytes_terminated(out, blob);
        return Ok(());
    };

    match first {
        tag::RUNTIME_NODE_REF => append_runtime_ref_key(out, RANK_NODE, &blob[1..]),
        tag::RUNTIME_REL_REF => append_runtime_ref_key(out, RANK_RELATIONSHIP, &blob[1..]),
        tag::RUNTIME_PATH_REF => append_runtime_ref_key(out, RANK_PATH, &blob[1..]),
        tag::RUNTIME_NAN => {
            out.push(RANK_NAN);
            Ok(())
        }
        tag::JSON | tag::MAP_JSON | tag::NODE_JSON | tag::RELATIONSHIP_JSON | tag::PATH_JSON => {
            let json_text = std::str::from_utf8(&blob[1..])?;
            let value: Value = serde_json::from_str(json_text)?;
            match first {
                tag::MAP_JSON => append_json_map_key(out, &value),
                tag::NODE_JSON => append_json_entity_key(out, RANK_NODE, &value),
                tag::RELATIONSHIP_JSON => append_json_entity_key(out, RANK_RELATIONSHIP, &value),
                tag::PATH_JSON => append_json_path_key(out, &value),
                _ => append_json_key(out, &value),
            }
        }
        _ => match decode_property_value(StorageValueRef::Blob(blob)) {
            Ok(value) => append_property_key(out, value.as_ref()),
            Err(err) => {
                if let Ok(text) = std::str::from_utf8(blob) {
                    if let Ok(value) = serde_json::from_str::<Value>(text) {
                        return append_json_key(out, &value);
                    }
                }
                Err(err.into())
            }
        },
    }
}

fn append_property_key(
    out: &mut Vec<u8>,
    value: PropertyValueRef<'_>,
) -> Result<(), OrderKeyError> {
    match value {
        PropertyValueRef::Null => out.push(RANK_NULL),
        PropertyValueRef::Bool(v) => append_bool_key(out, v),
        PropertyValueRef::Integer(v) => append_i64_key(out, v),
        PropertyValueRef::Float(v) => append_f64_key(out, v),
        PropertyValueRef::String(v) => append_string_key(out, v),
        PropertyValueRef::List(v) => append_list_key(out, v)?,

        PropertyValueRef::Date(v) => {
            append_other_subtype(out, OTHER_TEMPORAL_DATE);
            append_i64_payload(out, v.epoch_day());
        }
        PropertyValueRef::LocalTime(v) => {
            append_other_subtype(out, OTHER_TEMPORAL_LOCAL_TIME);
            append_i64_payload(out, v.nanos_since_midnight());
        }
        PropertyValueRef::ZonedTime(v) => {
            append_other_subtype(out, OTHER_TEMPORAL_ZONED_TIME);
            append_i64_payload(out, v.utc_nanos_since_midnight());
            append_i64_payload(out, v.time.nanos_since_midnight());
            append_i64_payload(out, v.offset.seconds_east() as i64);
        }
        PropertyValueRef::LocalDateTime(v) => {
            let (day, nanos) = v.local_epoch_day_and_nanos();
            append_other_subtype(out, OTHER_TEMPORAL_LOCAL_DATETIME);
            append_i64_payload(out, day);
            append_i64_payload(out, nanos);
        }
        PropertyValueRef::ZonedDateTime(v) => {
            let (day, nanos) = v.utc_epoch_day_and_nanos();
            let (local_day, local_nanos) = v.datetime.local_epoch_day_and_nanos();
            append_other_subtype(out, OTHER_TEMPORAL_ZONED_DATETIME);
            append_i64_payload(out, day);
            append_i64_payload(out, nanos);
            append_i64_payload(out, local_day);
            append_i64_payload(out, local_nanos);
            append_i64_payload(out, v.offset.seconds_east() as i64);
            append_string_payload(out, v.zone_id.as_deref().unwrap_or(""));
        }
        PropertyValueRef::Duration(v) => {
            append_other_subtype(out, OTHER_TEMPORAL_DURATION);
            append_string_payload(out, &v.to_string());
        }

        PropertyValueRef::Point(v) => {
            append_other_subtype(out, OTHER_SPATIAL_POINT);
            append_string_payload(out, &v.to_geojson_string()?);
        }
        PropertyValueRef::Geometry(v) => {
            append_other_subtype(out, OTHER_SPATIAL_GEOMETRY);
            append_string_payload(out, &v.to_geojson_string()?);
        }
        PropertyValueRef::Geography(v) => {
            append_other_subtype(out, OTHER_SPATIAL_GEOGRAPHY);
            append_string_payload(out, &v.to_geojson_string()?);
        }
        PropertyValueRef::Vector(v) => {
            append_other_subtype(out, OTHER_VECTOR);
            append_u64_be(out, v.len() as u64);
            for elem in v.iter() {
                match elem {
                    crate::vector::VectorElem::I8(x) => append_i64_key(out, x as i64),
                    crate::vector::VectorElem::I16(x) => append_i64_key(out, x as i64),
                    crate::vector::VectorElem::I32(x) => append_i64_key(out, x as i64),
                    crate::vector::VectorElem::I64(x) => append_i64_key(out, x),
                    crate::vector::VectorElem::F32(x) => append_f64_key(out, x as f64),
                    crate::vector::VectorElem::F64(x) => append_f64_key(out, x),
                }
            }
        }
        PropertyValueRef::Bytes(v) => {
            append_other_subtype(out, OTHER_BYTES);
            append_bytes_terminated(out, v);
        }
    }
    Ok(())
}

fn append_runtime_ref_key(
    out: &mut Vec<u8>,
    rank: u8,
    payload: &[u8],
) -> Result<(), OrderKeyError> {
    out.push(rank);
    if let Ok(text) = std::str::from_utf8(payload) {
        if let Some(id) = parse_runtime_ref_primary_i64(text) {
            append_i64_payload(out, id);
            return Ok(());
        }
    }
    append_bytes_terminated(out, payload);
    Ok(())
}

fn parse_runtime_ref_primary_i64(text: &str) -> Option<i64> {
    let primary = text.split_once(':').map(|(head, _)| head).unwrap_or(text);
    primary.parse::<i64>().ok()
}

fn append_json_key(out: &mut Vec<u8>, value: &Value) -> Result<(), OrderKeyError> {
    match value {
        Value::Null => out.push(RANK_NULL),
        Value::Bool(v) => append_bool_key(out, *v),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                append_i64_key(out, i);
            } else if let Some(u) = v.as_u64() {
                append_f64_key(out, u as f64);
            } else if let Some(f) = v.as_f64() {
                append_f64_key(out, f);
            } else {
                out.push(RANK_NAN);
            }
        }
        Value::String(v) => append_string_key(out, v),
        Value::Array(values) => append_json_list_key(out, values)?,
        Value::Object(_) => append_json_map_key(out, value)?,
    }
    Ok(())
}

fn append_json_list_key(out: &mut Vec<u8>, values: &[Value]) -> Result<(), OrderKeyError> {
    out.push(RANK_LIST);
    for value in values {
        append_json_key(out, value)?;
    }
    out.push(RANK_EMPTY);
    Ok(())
}

fn append_json_map_key(out: &mut Vec<u8>, value: &Value) -> Result<(), OrderKeyError> {
    let Value::Object(map) = value else {
        append_json_key(out, value)?;
        return Ok(());
    };

    out.push(RANK_MAP);
    append_u64_be(out, map.len() as u64);

    let mut entries = map.iter().collect::<Vec<_>>();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    for (key, _) in &entries {
        append_string_payload(out, key);
    }
    for (_, value) in entries {
        append_json_key(out, value)?;
    }

    Ok(())
}

fn append_json_entity_key(out: &mut Vec<u8>, rank: u8, value: &Value) -> Result<(), OrderKeyError> {
    let id = value
        .get("identity")
        .and_then(Value::as_i64)
        .or_else(|| value.get("id").and_then(Value::as_i64))
        .unwrap_or(0);
    out.push(rank);
    append_i64_payload(out, id);
    Ok(())
}

fn append_json_path_key(out: &mut Vec<u8>, value: &Value) -> Result<(), OrderKeyError> {
    let Value::Array(values) = value else {
        append_json_key(out, value)?;
        return Ok(());
    };

    out.push(RANK_PATH);
    for value in values {
        append_path_element_key(out, value)?;
    }
    out.push(RANK_EMPTY);
    Ok(())
}

fn append_path_element_key(out: &mut Vec<u8>, value: &Value) -> Result<(), OrderKeyError> {
    if value.get("labels").is_some() && value.get("identity").is_some() {
        append_json_entity_key(out, RANK_NODE, value)
    } else if value.get("type").is_some() && value.get("identity").is_some() {
        append_json_entity_key(out, RANK_RELATIONSHIP, value)
    } else {
        append_json_key(out, value)
    }
}

fn append_list_key(out: &mut Vec<u8>, list: &ListValue) -> Result<(), OrderKeyError> {
    out.push(RANK_LIST);
    for value in list.iter() {
        append_property_key(out, value)?;
    }
    out.push(RANK_EMPTY);
    Ok(())
}

fn append_bool_key(out: &mut Vec<u8>, value: bool) {
    out.push(RANK_BOOLEAN);
    out.push(u8::from(value));
}

fn append_string_key(out: &mut Vec<u8>, value: &str) {
    out.push(RANK_STRING);
    append_string_payload(out, value);
}

fn append_other_subtype(out: &mut Vec<u8>, subtype: u8) {
    out.push(RANK_OTHER);
    out.push(subtype);
}

fn append_string_payload(out: &mut Vec<u8>, value: &str) {
    append_bytes_terminated(out, value.as_bytes());
}

fn append_bytes_terminated(out: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        out.push(0x01);
        out.push(*byte);
    }
    out.push(RANK_EMPTY);
}

fn append_u64_be(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn append_i64_key(out: &mut Vec<u8>, value: i64) {
    out.push(RANK_NUMBER);
    append_i64_payload(out, value);
}

fn append_i64_payload(out: &mut Vec<u8>, value: i64) {
    let (negative, magnitude) = if value < 0 {
        (true, value.wrapping_neg() as u64)
    } else {
        (false, value as u64)
    };

    if magnitude == 0 {
        append_zero_number_payload(out);
        return;
    }

    let highest_bit = 63 - magnitude.leading_zeros();
    let significand = magnitude << (63 - highest_bit);
    append_finite_number_payload(out, negative, highest_bit as i32, significand);
}

fn append_f64_key(out: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        out.push(RANK_NAN);
        return;
    }

    out.push(RANK_NUMBER);

    if value == 0.0 {
        append_zero_number_payload(out);
        return;
    }

    if value.is_infinite() {
        append_infinite_number_payload(out, value.is_sign_negative());
        return;
    }

    let bits = value.to_bits();
    let negative = (bits >> 63) != 0;
    let raw_exp = ((bits >> 52) & 0x7ff) as u16;
    let fraction = bits & ((1u64 << 52) - 1);

    if raw_exp == 0 {
        let highest_bit = 63 - fraction.leading_zeros();
        let exponent = highest_bit as i32 - NUM_EXP_BIAS;
        let significand = fraction << (63 - highest_bit);
        append_finite_number_payload(out, negative, exponent, significand);
    } else {
        let exponent = raw_exp as i32 - 1023;
        let significand = ((1u64 << 52) | fraction) << 11;
        append_finite_number_payload(out, negative, exponent, significand);
    }
}

fn append_zero_number_payload(out: &mut Vec<u8>) {
    out.push(0x01);
}

fn append_infinite_number_payload(out: &mut Vec<u8>, negative: bool) {
    append_number_magnitude_payload(out, negative, NUM_INF_EXPONENT, u64::MAX);
}

fn append_finite_number_payload(
    out: &mut Vec<u8>,
    negative: bool,
    exponent: i32,
    significand: u64,
) {
    let biased_exponent = (exponent + NUM_EXP_BIAS) as u16;
    append_number_magnitude_payload(out, negative, biased_exponent, significand);
}

fn append_number_magnitude_payload(
    out: &mut Vec<u8>,
    negative: bool,
    exponent: u16,
    significand: u64,
) {
    out.push(if negative { 0x00 } else { 0x02 });

    let mut exponent_bytes = exponent.to_be_bytes();
    let mut significand_bytes = significand.to_be_bytes();
    if negative {
        for byte in &mut exponent_bytes {
            *byte = !*byte;
        }
        for byte in &mut significand_bytes {
            *byte = !*byte;
        }
    }

    out.extend_from_slice(&exponent_bytes);
    out.extend_from_slice(&significand_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_property_value;
    use crate::list::ListValue;
    use crate::property::PropertyValue;
    use crate::storage::{StorageValue, StorageValueRef};
    use std::hint::black_box;
    use std::time::Instant;

    fn key(value: StorageValueRef<'_>) -> Vec<u8> {
        storage_value_to_cypher_order_key(value).unwrap()
    }

    fn storage_key(value: &StorageValue) -> Vec<u8> {
        key(value.as_ref())
    }

    fn property_key(value: PropertyValue) -> Vec<u8> {
        storage_key(&encode_property_value(&value).unwrap())
    }

    #[test]
    fn numeric_order_keys_are_compact() {
        let values = [
            StorageValueRef::Real(f64::NEG_INFINITY),
            StorageValueRef::Integer(i64::MIN),
            StorageValueRef::Real(-1.5),
            StorageValueRef::Integer(-1),
            StorageValueRef::Integer(0),
            StorageValueRef::Real(-0.0),
            StorageValueRef::Real(0.5),
            StorageValueRef::Integer(1),
            StorageValueRef::Real(1.5),
            StorageValueRef::Integer(i64::MAX),
            StorageValueRef::Real(f64::INFINITY),
        ];

        for value in values {
            assert!(
                key(value).len() <= 12,
                "numeric order keys should stay small enough for hot ORDER BY paths"
            );
        }
    }

    #[test]
    fn numeric_sort_keys_preserve_mixed_integer_float_order() {
        let ordered = [
            StorageValueRef::Real(f64::NEG_INFINITY),
            StorageValueRef::Integer(i64::MIN),
            StorageValueRef::Real(-9_007_199_254_740_994.0),
            StorageValueRef::Integer(-9_007_199_254_740_993),
            StorageValueRef::Real(-9_007_199_254_740_992.0),
            StorageValueRef::Integer(-2),
            StorageValueRef::Real(-1.5),
            StorageValueRef::Integer(-1),
            StorageValueRef::Real(-0.5),
            StorageValueRef::Integer(0),
            StorageValueRef::Real(0.5),
            StorageValueRef::Integer(1),
            StorageValueRef::Real(1.5),
            StorageValueRef::Integer(2),
            StorageValueRef::Real(9_007_199_254_740_992.0),
            StorageValueRef::Integer(9_007_199_254_740_993),
            StorageValueRef::Real(9_007_199_254_740_994.0),
            StorageValueRef::Integer(i64::MAX),
            StorageValueRef::Real(f64::INFINITY),
            StorageValueRef::Real(f64::NAN),
            StorageValueRef::Null,
        ];

        for pair in ordered.windows(2) {
            assert!(
                key(pair[0]) < key(pair[1]),
                "{:?} should sort before {:?}",
                pair[0],
                pair[1]
            );
        }

        assert_eq!(
            key(StorageValueRef::Integer(1)),
            key(StorageValueRef::Real(1.0))
        );
        assert_eq!(
            key(StorageValueRef::Integer(0)),
            key(StorageValueRef::Real(-0.0))
        );
    }

    #[test]
    fn zoned_time_order_keys_use_utc_time_of_day() {
        let mut values = vec![
            "10:35-08:00",
            "12:31:14.645876123+01:00",
            "12:31:14.645876124+01:00",
            "12:35:15+05:00",
            "12:30:14.645876123+01:01",
        ];

        values.sort_by_key(|value| property_key(PropertyValue::ZonedTime(value.parse().unwrap())));

        assert_eq!(
            values,
            vec![
                "12:35:15+05:00",
                "12:30:14.645876123+01:01",
                "12:31:14.645876123+01:00",
                "12:31:14.645876124+01:00",
                "10:35-08:00",
            ]
        );
    }

    #[test]
    fn zoned_datetime_order_keys_use_utc_instant() {
        let mut values = vec![
            "1984-10-11T12:30:14.000000012+00:15",
            "1984-10-11T12:31:14.645876123+00:17",
            "0001-01-01T01:01:01.000000001-11:59",
            "9999-09-09T09:59:59.999999999+11:59",
            "1980-12-11T12:31:14-11:59",
        ];

        values.sort_by_key(|value| {
            property_key(PropertyValue::ZonedDateTime(value.parse().unwrap()))
        });

        assert_eq!(
            values,
            vec![
                "0001-01-01T01:01:01.000000001-11:59",
                "1980-12-11T12:31:14-11:59",
                "1984-10-11T12:31:14.645876123+00:17",
                "1984-10-11T12:30:14.000000012+00:15",
                "9999-09-09T09:59:59.999999999+11:59",
            ]
        );
    }

    #[test]
    #[ignore = "microbenchmark; run explicitly with --ignored --nocapture"]
    fn cypher_order_key_micro_benchmark() {
        let list = encode_property_value(&PropertyValue::List(ListValue::Generic(vec![
            PropertyValue::Integer(42),
            PropertyValue::String("alpha".to_string()),
            PropertyValue::Bool(true),
            PropertyValue::Null,
        ])))
        .unwrap();
        let bool_value = encode_property_value(&PropertyValue::Bool(true)).unwrap();
        let raw_json_list = StorageValue::Blob(br#"[1,"alpha",false,null]"#.to_vec());

        let samples = [
            StorageValue::Null,
            StorageValue::Integer(-9_007_199_254_740_993),
            StorageValue::Integer(9_007_199_254_740_993),
            StorageValue::Real(-12345.25),
            StorageValue::Real(12345.25),
            StorageValue::Text("ordering-hot-path".to_string()),
            bool_value,
            list,
            raw_json_list,
        ];

        let iterations: usize = std::env::var("VELR_ORDER_KEY_BENCH_ITERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(if cfg!(debug_assertions) {
                20_000
            } else {
                500_000
            });

        let start = Instant::now();
        let mut bytes = 0usize;
        for i in 0..iterations {
            let sample = black_box(&samples[i % samples.len()]);
            let sort_key = storage_key(sample);
            bytes = bytes.wrapping_add(sort_key.len());
            black_box(&sort_key);
        }
        let elapsed = start.elapsed();
        let ns_per_key = elapsed.as_nanos() as f64 / iterations as f64;

        eprintln!(
            "cypher_order_key_micro_benchmark: iterations={iterations} elapsed={elapsed:?} ns_per_key={ns_per_key:.1} bytes={bytes}"
        );

        if let Ok(max_ns_per_key) = std::env::var("VELR_ORDER_KEY_MAX_NS") {
            let max_ns_per_key: f64 = max_ns_per_key.parse().unwrap();
            assert!(
                ns_per_key <= max_ns_per_key,
                "sort key encoding took {ns_per_key:.1} ns/key, expected <= {max_ns_per_key:.1} ns/key"
            );
        }
    }
}
