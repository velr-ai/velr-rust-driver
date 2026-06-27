use crate::error::EncodeError;
use crate::property::{PropertyValue, PropertyValueRef};

pub fn temporal_add_property_values(
    lhs: PropertyValueRef<'_>,
    rhs: PropertyValueRef<'_>,
) -> Result<Option<PropertyValue>, EncodeError> {
    use PropertyValueRef::*;

    let result = match (lhs, rhs) {
        (Date(value), Duration(duration)) | (Duration(duration), Date(value)) => {
            PropertyValue::Date(value.checked_add_duration(duration)?)
        }
        (LocalTime(value), Duration(duration)) | (Duration(duration), LocalTime(value)) => {
            PropertyValue::LocalTime(value.checked_add_duration(duration)?)
        }
        (ZonedTime(value), Duration(duration)) | (Duration(duration), ZonedTime(value)) => {
            PropertyValue::ZonedTime(value.checked_add_duration(duration)?)
        }
        (LocalDateTime(value), Duration(duration)) | (Duration(duration), LocalDateTime(value)) => {
            PropertyValue::LocalDateTime(value.checked_add_duration(duration)?)
        }
        (ZonedDateTime(value), Duration(duration)) | (Duration(duration), ZonedDateTime(value)) => {
            PropertyValue::ZonedDateTime(value.checked_add_duration(duration)?)
        }
        (Duration(lhs), Duration(rhs)) => PropertyValue::Duration(lhs.checked_add_duration(rhs)?),
        _ => return Ok(None),
    };

    Ok(Some(result))
}

pub fn temporal_sub_property_values(
    lhs: PropertyValueRef<'_>,
    rhs: PropertyValueRef<'_>,
) -> Result<Option<PropertyValue>, EncodeError> {
    use PropertyValueRef::*;

    let result = match (lhs, rhs) {
        (Date(value), Duration(duration)) => {
            PropertyValue::Date(value.checked_sub_duration(duration)?)
        }
        (LocalTime(value), Duration(duration)) => {
            PropertyValue::LocalTime(value.checked_sub_duration(duration)?)
        }
        (ZonedTime(value), Duration(duration)) => {
            PropertyValue::ZonedTime(value.checked_sub_duration(duration)?)
        }
        (LocalDateTime(value), Duration(duration)) => {
            PropertyValue::LocalDateTime(value.checked_sub_duration(duration)?)
        }
        (ZonedDateTime(value), Duration(duration)) => {
            PropertyValue::ZonedDateTime(value.checked_sub_duration(duration)?)
        }
        (Duration(lhs), Duration(rhs)) => PropertyValue::Duration(lhs.checked_sub_duration(rhs)?),
        _ => return Ok(None),
    };

    Ok(Some(result))
}

pub fn temporal_mul_property_values(
    lhs: PropertyValueRef<'_>,
    rhs: PropertyValueRef<'_>,
) -> Result<Option<PropertyValue>, EncodeError> {
    use PropertyValueRef::*;

    let result = match (lhs, rhs) {
        (Duration(duration), Integer(value)) | (Integer(value), Duration(duration)) => {
            PropertyValue::Duration(duration.checked_mul_number(value as f64)?)
        }
        (Duration(duration), Float(value)) | (Float(value), Duration(duration)) => {
            PropertyValue::Duration(duration.checked_mul_number(value)?)
        }
        _ => return Ok(None),
    };

    Ok(Some(result))
}

pub fn temporal_div_property_values(
    lhs: PropertyValueRef<'_>,
    rhs: PropertyValueRef<'_>,
) -> Result<Option<PropertyValue>, EncodeError> {
    use PropertyValueRef::*;

    let result = match (lhs, rhs) {
        (Duration(duration), Integer(value)) => {
            PropertyValue::Duration(duration.checked_div_number(value as f64)?)
        }
        (Duration(duration), Float(value)) => {
            PropertyValue::Duration(duration.checked_div_number(value)?)
        }
        _ => return Ok(None),
    };

    Ok(Some(result))
}
