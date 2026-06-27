use crate::error::{DecodeError, EncodeError};
use crate::property::{PropertyValue, PropertyValueRef};
use crate::tag;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum ListValue {
    Generic(Vec<PropertyValue>),
    String(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
}

impl ListValue {
    pub fn len(&self) -> usize {
        match self {
            Self::Generic(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Float64(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> ListIter<'_> {
        match self {
            Self::Generic(v) => ListIter::Generic(v.iter()),
            Self::String(v) => ListIter::String(v.iter()),
            Self::Bool(v) => ListIter::Bool(v.iter()),
            Self::Int64(v) => ListIter::Int64(v.iter()),
            Self::Float64(v) => ListIter::Float64(v.iter()),
        }
    }

    pub fn encode_blob(&self) -> Result<Vec<u8>, EncodeError> {
        match self {
            Self::Generic(values) => {
                let mut json_values = Vec::with_capacity(values.len());
                for value in values {
                    json_values.push(property_to_json_value(value)?);
                }
                let payload = serde_json::to_vec(&Value::Array(json_values))
                    .map_err(|_| EncodeError::InvalidList("failed to encode generic list"))?;
                let mut out = vec![tag::LIST_JSON];
                out.extend_from_slice(&payload);
                Ok(out)
            }
            Self::String(values) => {
                let payload = serde_json::to_vec(values)
                    .map_err(|_| EncodeError::InvalidList("failed to encode string list"))?;
                let mut out = vec![tag::STRING_LIST_JSON];
                out.extend_from_slice(&payload);
                Ok(out)
            }
            Self::Bool(values) => encode_bool_list(values),
            Self::Int64(values) => encode_int64_list(values),
            Self::Float64(values) => encode_float64_list(values),
        }
    }

    pub fn decode_blob(blob: &[u8]) -> Result<Self, DecodeError> {
        if blob.is_empty() {
            return Err(DecodeError::EmptyInput);
        }
        match blob[0] {
            tag::LIST_JSON => decode_generic_list(&blob[1..]),
            tag::STRING_LIST_JSON => decode_string_list(&blob[1..]),
            tag::BOOL_LIST => decode_bool_list(blob),
            tag::INT64_LIST => decode_int64_list(blob),
            tag::FLOAT64_LIST => decode_float64_list(blob),
            actual => Err(DecodeError::UnexpectedTag {
                expected: "list tag",
                actual,
            }),
        }
    }
}

pub enum ListIter<'a> {
    Generic(std::slice::Iter<'a, PropertyValue>),
    String(std::slice::Iter<'a, Option<String>>),
    Bool(std::slice::Iter<'a, Option<bool>>),
    Int64(std::slice::Iter<'a, Option<i64>>),
    Float64(std::slice::Iter<'a, Option<f64>>),
}

impl<'a> Iterator for ListIter<'a> {
    type Item = PropertyValueRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Generic(iter) => iter.next().map(PropertyValue::as_ref),
            Self::String(iter) => iter.next().map(|v| match v {
                None => PropertyValueRef::Null,
                Some(value) => PropertyValueRef::String(value.as_str()),
            }),
            Self::Bool(iter) => iter.next().map(|v| match v {
                None => PropertyValueRef::Null,
                Some(value) => PropertyValueRef::Bool(*value),
            }),
            Self::Int64(iter) => iter.next().map(|v| match v {
                None => PropertyValueRef::Null,
                Some(value) => PropertyValueRef::Integer(*value),
            }),
            Self::Float64(iter) => iter.next().map(|v| match v {
                None => PropertyValueRef::Null,
                Some(value) => PropertyValueRef::Float(*value),
            }),
        }
    }
}

fn property_to_json_value(value: &PropertyValue) -> Result<Value, EncodeError> {
    match value {
        PropertyValue::Null => Ok(Value::Null),
        PropertyValue::Bool(v) => Ok(Value::Bool(*v)),
        PropertyValue::Integer(v) => Ok(Value::Number((*v).into())),
        PropertyValue::Float(v) if v.is_finite() => {
            serde_json::Number::from_f64(*v).map(Value::Number).ok_or(
                EncodeError::NonCanonicalValue("non-finite floats are not canonical"),
            )
        }
        PropertyValue::Float(_) => Err(EncodeError::NonCanonicalValue(
            "non-finite floats are not canonical",
        )),
        PropertyValue::String(v) => Ok(Value::String(v.clone())),
        PropertyValue::List(ListValue::Generic(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(property_to_json_value(value)?);
            }
            Ok(Value::Array(out))
        }
        PropertyValue::List(ListValue::String(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(match value {
                    None => Value::Null,
                    Some(v) => Value::String(v.clone()),
                });
            }
            Ok(Value::Array(out))
        }
        PropertyValue::List(ListValue::Bool(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(match value {
                    None => Value::Null,
                    Some(v) => Value::Bool(*v),
                });
            }
            Ok(Value::Array(out))
        }
        PropertyValue::List(ListValue::Int64(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(match value {
                    None => Value::Null,
                    Some(v) => Value::Number((*v).into()),
                });
            }
            Ok(Value::Array(out))
        }
        PropertyValue::List(ListValue::Float64(values)) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(match value {
                    None => Value::Null,
                    Some(v) => serde_json::Number::from_f64(*v).map(Value::Number).ok_or(
                        EncodeError::NonCanonicalValue("non-finite floats are not canonical"),
                    )?,
                });
            }
            Ok(Value::Array(out))
        }
        _ => Err(EncodeError::InvalidList(
            "generic JSON-backed lists only support JSON-compatible property values",
        )),
    }
}

fn json_to_property_value(value: &Value) -> Result<PropertyValue, DecodeError> {
    match value {
        Value::Null => Ok(PropertyValue::Null),
        Value::Bool(v) => Ok(PropertyValue::Bool(*v)),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Ok(PropertyValue::Integer(i))
            } else if let Some(f) = v.as_f64() {
                Ok(PropertyValue::Float(f))
            } else {
                Err(DecodeError::InvalidJson("unsupported JSON number".into()))
            }
        }
        Value::String(v) => Ok(PropertyValue::String(v.clone())),
        Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                out.push(json_to_property_value(value)?);
            }
            Ok(PropertyValue::List(ListValue::Generic(out)))
        }
        Value::Object(_) => Err(DecodeError::InvalidList(
            "maps are not part of the persisted property-value contract".into(),
        )),
    }
}

fn decode_generic_list(payload: &[u8]) -> Result<ListValue, DecodeError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|e| DecodeError::InvalidJson(e.to_string()))?;
    let Value::Array(values) = value else {
        return Err(DecodeError::InvalidList(
            "generic list payload must be a JSON array".into(),
        ));
    };
    let mut out = Vec::with_capacity(values.len());
    for value in &values {
        out.push(json_to_property_value(value)?);
    }
    Ok(ListValue::Generic(out))
}

fn decode_string_list(payload: &[u8]) -> Result<ListValue, DecodeError> {
    let values: Vec<Option<String>> =
        serde_json::from_slice(payload).map_err(|e| DecodeError::InvalidJson(e.to_string()))?;
    Ok(ListValue::String(values))
}

fn encode_bool_list(values: &[Option<bool>]) -> Result<Vec<u8>, EncodeError> {
    let mut out = vec![tag::BOOL_LIST];
    encode_list_header(
        &mut out,
        values.len(),
        values.iter().filter(|v| v.is_none()).count(),
    )?;
    if values.iter().any(|v| v.is_none()) {
        out.extend_from_slice(&bitmap(values.len(), |idx| values[idx].is_some()));
    }
    out.extend_from_slice(&bitmap(values.len(), |idx| {
        matches!(values[idx], Some(true))
    }));
    Ok(out)
}

fn encode_int64_list(values: &[Option<i64>]) -> Result<Vec<u8>, EncodeError> {
    let mut out = vec![tag::INT64_LIST];
    encode_list_header(
        &mut out,
        values.len(),
        values.iter().filter(|v| v.is_none()).count(),
    )?;
    if values.iter().any(|v| v.is_none()) {
        out.extend_from_slice(&bitmap(values.len(), |idx| values[idx].is_some()));
    }
    for value in values {
        out.extend_from_slice(&value.unwrap_or_default().to_le_bytes());
    }
    Ok(out)
}

fn encode_float64_list(values: &[Option<f64>]) -> Result<Vec<u8>, EncodeError> {
    let mut out = vec![tag::FLOAT64_LIST];
    encode_list_header(
        &mut out,
        values.len(),
        values.iter().filter(|v| v.is_none()).count(),
    )?;
    if values.iter().any(|v| v.is_none()) {
        out.extend_from_slice(&bitmap(values.len(), |idx| values[idx].is_some()));
    }
    for value in values {
        let value = value.unwrap_or_default();
        if !value.is_finite() {
            return Err(EncodeError::NonCanonicalValue(
                "non-finite float in Float64 list",
            ));
        }
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn decode_bool_list(blob: &[u8]) -> Result<ListValue, DecodeError> {
    if blob.first().copied() != Some(tag::BOOL_LIST) {
        return Err(DecodeError::UnexpectedTag {
            expected: "BoolList",
            actual: blob.first().copied().unwrap_or(0),
        });
    }
    let (length, null_count, validity, values, remainder) = decode_list_buffers(blob, 1)?;
    if !remainder.is_empty() {
        return Err(DecodeError::NonCanonical("trailing bytes after BoolList"));
    }
    if values.len() != bitmap_len(length) {
        return Err(DecodeError::InvalidList(
            "invalid BoolList value bitmap length".into(),
        ));
    }
    let mut out = Vec::with_capacity(length);
    for idx in 0..length {
        if !is_valid(validity, idx, null_count) {
            out.push(None);
        } else {
            out.push(Some(bit_is_set(values, idx)));
        }
    }
    Ok(ListValue::Bool(out))
}

fn decode_int64_list(blob: &[u8]) -> Result<ListValue, DecodeError> {
    if blob.first().copied() != Some(tag::INT64_LIST) {
        return Err(DecodeError::UnexpectedTag {
            expected: "Int64List",
            actual: blob.first().copied().unwrap_or(0),
        });
    }
    let (length, null_count, validity, values, remainder) = decode_list_buffers(blob, 8)?;
    if !remainder.is_empty() {
        return Err(DecodeError::NonCanonical("trailing bytes after Int64List"));
    }
    let mut out = Vec::with_capacity(length);
    for idx in 0..length {
        if !is_valid(validity, idx, null_count) {
            out.push(None);
            continue;
        }
        let start = idx * 8;
        let value = i64::from_le_bytes(values[start..start + 8].try_into().unwrap());
        out.push(Some(value));
    }
    Ok(ListValue::Int64(out))
}

fn decode_float64_list(blob: &[u8]) -> Result<ListValue, DecodeError> {
    if blob.first().copied() != Some(tag::FLOAT64_LIST) {
        return Err(DecodeError::UnexpectedTag {
            expected: "Float64List",
            actual: blob.first().copied().unwrap_or(0),
        });
    }
    let (length, null_count, validity, values, remainder) = decode_list_buffers(blob, 8)?;
    if !remainder.is_empty() {
        return Err(DecodeError::NonCanonical(
            "trailing bytes after Float64List",
        ));
    }
    let mut out = Vec::with_capacity(length);
    for idx in 0..length {
        if !is_valid(validity, idx, null_count) {
            out.push(None);
            continue;
        }
        let start = idx * 8;
        let value = f64::from_le_bytes(values[start..start + 8].try_into().unwrap());
        out.push(Some(value));
    }
    Ok(ListValue::Float64(out))
}

fn encode_list_header(out: &mut Vec<u8>, len: usize, null_count: usize) -> Result<(), EncodeError> {
    if len > u32::MAX as usize || null_count > u32::MAX as usize {
        return Err(EncodeError::InvalidList("list length exceeds u32::MAX"));
    }
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&(null_count as u32).to_le_bytes());
    Ok(())
}

fn decode_list_buffers<'a>(
    blob: &'a [u8],
    element_width: usize,
) -> Result<(usize, usize, Option<&'a [u8]>, &'a [u8], &'a [u8]), DecodeError> {
    if blob.len() < 9 {
        return Err(DecodeError::Truncated);
    }
    let len = u32::from_le_bytes(blob[1..5].try_into().unwrap()) as usize;
    let null_count = u32::from_le_bytes(blob[5..9].try_into().unwrap()) as usize;
    let validity_len = if null_count == 0 { 0 } else { bitmap_len(len) };
    let value_len = if element_width == 1 {
        bitmap_len(len)
    } else {
        len.checked_mul(element_width)
            .ok_or_else(|| DecodeError::InvalidList("list payload length overflow".into()))?
    };
    if blob.len() < 9 + validity_len + value_len {
        return Err(DecodeError::Truncated);
    }
    let validity = if validity_len == 0 {
        None
    } else {
        Some(&blob[9..9 + validity_len])
    };
    let values_start = 9 + validity_len;
    let values = &blob[values_start..values_start + value_len];
    let remainder = &blob[values_start + value_len..];
    Ok((len, null_count, validity, values, remainder))
}

fn bitmap_len(len: usize) -> usize {
    (len + 7) / 8
}

fn bitmap(len: usize, is_set: impl Fn(usize) -> bool) -> Vec<u8> {
    let mut out = vec![0u8; bitmap_len(len)];
    for idx in 0..len {
        if is_set(idx) {
            out[idx / 8] |= 1 << (idx % 8);
        }
    }
    out
}

fn bit_is_set(bytes: &[u8], idx: usize) -> bool {
    (bytes[idx / 8] & (1 << (idx % 8))) != 0
}

fn is_valid(validity: Option<&[u8]>, idx: usize, null_count: usize) -> bool {
    if null_count == 0 {
        true
    } else {
        bit_is_set(validity.unwrap(), idx)
    }
}
