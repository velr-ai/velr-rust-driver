use crate::error::{DecodeError, EncodeError};
use crate::list::ListValue;
use crate::property::{PropertyValue, PropertyValueRef};
use crate::spatial::{GeographyValue, GeometryValue, PointValue};
use crate::storage::{StorageValue, StorageValueRef};
use crate::tag;
use crate::temporal::{LocalDateTimeValue, LocalTimeValue, ZonedDateTimeValue, ZonedTimeValue};
use crate::vector::VectorValue;
use std::str::FromStr;

pub fn encode_property_value(value: &PropertyValue) -> Result<StorageValue, EncodeError> {
    match value {
        PropertyValue::Null => Ok(StorageValue::Null),
        PropertyValue::Bool(false) => Ok(StorageValue::Blob(vec![tag::BOOL_FALSE])),
        PropertyValue::Bool(true) => Ok(StorageValue::Blob(vec![tag::BOOL_TRUE])),
        PropertyValue::Integer(v) => Ok(StorageValue::Integer(*v)),
        PropertyValue::Float(v) if v.is_finite() => Ok(StorageValue::Real(*v)),
        PropertyValue::Float(_) => Err(EncodeError::NonCanonicalValue(
            "non-finite floats are not canonical",
        )),
        PropertyValue::String(v) => Ok(StorageValue::Text(v.clone())),
        PropertyValue::Date(v) => encode_text_blob(tag::DATE, &v.to_string()),
        PropertyValue::LocalTime(v) => encode_text_blob(tag::LOCAL_TIME, &local_time_text(v)),
        PropertyValue::ZonedTime(v) => encode_text_blob(tag::ZONED_TIME, &zoned_time_text(v)),
        PropertyValue::LocalDateTime(v) => {
            encode_text_blob(tag::LOCAL_DATETIME, &local_datetime_text(v))
        }
        PropertyValue::ZonedDateTime(v) => {
            encode_text_blob(tag::ZONED_DATETIME, &zoned_datetime_text(v))
        }
        PropertyValue::Duration(v) => encode_text_blob(tag::DURATION, &v.to_string()),
        PropertyValue::Point(v) => Ok(StorageValue::Blob(v.encode_blob()?)),
        PropertyValue::Geometry(v) => Ok(StorageValue::Blob(v.encode_blob()?)),
        PropertyValue::Geography(v) => Ok(StorageValue::Blob(v.encode_blob()?)),
        PropertyValue::List(v) => Ok(StorageValue::Blob(v.encode_blob()?)),
        PropertyValue::Vector(v) => Ok(StorageValue::Blob(v.encode_blob()?)),
        PropertyValue::Bytes(v) => {
            let mut out = Vec::with_capacity(v.len() + 1);
            out.push(tag::BINARY);
            out.extend_from_slice(v);
            Ok(StorageValue::Blob(out))
        }
    }
}

pub fn decode_property_value(value: StorageValueRef<'_>) -> Result<PropertyValue, DecodeError> {
    match value {
        StorageValueRef::Null => Ok(PropertyValue::Null),
        StorageValueRef::Integer(v) => Ok(PropertyValue::Integer(v)),
        StorageValueRef::Real(v) => Ok(PropertyValue::Float(v)),
        StorageValueRef::Text(v) => Ok(PropertyValue::String(v.to_string())),
        StorageValueRef::Blob(blob) => decode_property_blob(blob),
    }
}

fn encode_text_blob(tag: u8, value: &str) -> Result<StorageValue, EncodeError> {
    let mut out = Vec::with_capacity(1 + value.len());
    out.push(tag);
    out.extend_from_slice(value.as_bytes());
    Ok(StorageValue::Blob(out))
}

pub(crate) fn temporal_value_text(value: PropertyValueRef<'_>) -> Option<String> {
    match value {
        PropertyValueRef::Date(v) => Some(v.to_string()),
        PropertyValueRef::LocalTime(v) => Some(local_time_text(v)),
        PropertyValueRef::ZonedTime(v) => Some(zoned_time_text(v)),
        PropertyValueRef::LocalDateTime(v) => Some(local_datetime_text(v)),
        PropertyValueRef::ZonedDateTime(v) => Some(zoned_datetime_text(v)),
        PropertyValueRef::Duration(v) => Some(v.to_string()),
        _ => None,
    }
}

fn local_time_text(value: &LocalTimeValue) -> String {
    let mut out = format!("{:02}:{:02}", value.hour(), value.minute());
    if value.second() != 0 || value.nanos() != 0 {
        out.push_str(&format!(":{:02}", value.second()));
        push_fraction(&mut out, value.nanos());
    }
    out
}

fn zoned_time_text(value: &ZonedTimeValue) -> String {
    format!("{}{}", local_time_text(&value.time), value.offset)
}

fn local_datetime_text(value: &LocalDateTimeValue) -> String {
    format!("{}T{}", value.date(), local_time_text(&value.time()))
}

fn zoned_datetime_text(value: &ZonedDateTimeValue) -> String {
    let mut out = format!("{}{}", local_datetime_text(&value.datetime), value.offset);
    if let Some(zone_id) = &value.zone_id {
        out.push('[');
        out.push_str(zone_id);
        out.push(']');
    }
    out
}

fn push_fraction(out: &mut String, nanos: u32) {
    if nanos == 0 {
        return;
    }

    let mut frac = format!("{nanos:09}");
    while frac.ends_with('0') {
        frac.pop();
    }
    out.push('.');
    out.push_str(&frac);
}

fn decode_property_blob(blob: &[u8]) -> Result<PropertyValue, DecodeError> {
    let Some(tag) = blob.first().copied() else {
        return Err(DecodeError::EmptyInput);
    };
    match tag {
        tag::BOOL_FALSE => Ok(PropertyValue::Bool(false)),
        tag::BOOL_TRUE => Ok(PropertyValue::Bool(true)),
        tag::BINARY => Ok(PropertyValue::Bytes(blob[1..].to_vec())),
        tag::DATE => Ok(PropertyValue::Date(parse_temporal(&blob[1..])?)),
        tag::LOCAL_TIME => Ok(PropertyValue::LocalTime(parse_temporal(&blob[1..])?)),
        tag::ZONED_TIME => Ok(PropertyValue::ZonedTime(parse_temporal(&blob[1..])?)),
        tag::LOCAL_DATETIME => Ok(PropertyValue::LocalDateTime(parse_temporal(&blob[1..])?)),
        tag::ZONED_DATETIME => Ok(PropertyValue::ZonedDateTime(parse_temporal(&blob[1..])?)),
        tag::DURATION => Ok(PropertyValue::Duration(parse_temporal(&blob[1..])?)),
        tag::POINT => Ok(PropertyValue::Point(PointValue::decode_blob(blob)?)),
        tag::GEOMETRY => Ok(PropertyValue::Geometry(GeometryValue::decode_blob(blob)?)),
        tag::GEOGRAPHY => Ok(PropertyValue::Geography(GeographyValue::decode_blob(blob)?)),
        tag::LIST_JSON
        | tag::STRING_LIST_JSON
        | tag::BOOL_LIST
        | tag::INT64_LIST
        | tag::FLOAT64_LIST => Ok(PropertyValue::List(ListValue::decode_blob(blob)?)),
        tag::VECTOR => Ok(PropertyValue::Vector(VectorValue::decode_blob(blob)?)),
        actual => Err(DecodeError::UnexpectedTag {
            expected: "property tag",
            actual,
        }),
    }
}

fn parse_temporal<T>(payload: &[u8]) -> Result<T, DecodeError>
where
    T: FromStr<Err = DecodeError>,
{
    let text = std::str::from_utf8(payload).map_err(|_| DecodeError::InvalidUtf8)?;
    text.parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list::ListValue;
    use crate::spatial::{
        GeographyValue, GeometryShape, GeometryValue, LineStringValue, LinearRingValue, PointValue,
        PolygonValue, Position,
    };
    use crate::storage::StorageValue;
    use crate::temporal::{
        DateValue, DurationValue, LocalDateTimeValue, LocalTimeValue, ZonedDateTimeValue,
        ZonedTimeValue,
    };
    use crate::vector::{VectorStorage, VectorType, VectorValue};

    fn assert_roundtrip(value: PropertyValue) {
        let decoded =
            decode_property_value(encode_property_value(&value).unwrap().as_ref()).unwrap();
        assert_eq!(decoded, value);
    }

    fn sample_xyz_ring() -> LinearRingValue {
        LinearRingValue::new(vec![
            Position::Xyz {
                x: 0.0,
                y: 0.0,
                z: 1.0,
            },
            Position::Xyz {
                x: 4.0,
                y: 0.0,
                z: 1.0,
            },
            Position::Xyz {
                x: 4.0,
                y: 4.0,
                z: 1.0,
            },
            Position::Xyz {
                x: 0.0,
                y: 0.0,
                z: 1.0,
            },
        ])
        .unwrap()
    }

    fn sample_supported_shapes() -> Vec<GeometryShape> {
        vec![
            GeometryShape::Point(Position::Xyz {
                x: 12.5,
                y: 55.7,
                z: 9.0,
            }),
            GeometryShape::LineString(
                LineStringValue::new(vec![
                    Position::Xyz {
                        x: 12.5,
                        y: 55.7,
                        z: 9.0,
                    },
                    Position::Xyz {
                        x: 13.1,
                        y: 56.0,
                        z: 10.5,
                    },
                ])
                .unwrap(),
            ),
            GeometryShape::Polygon(PolygonValue::new(vec![sample_xyz_ring()]).unwrap()),
            GeometryShape::MultiPoint(vec![
                Position::Xyz {
                    x: 1.0,
                    y: 2.0,
                    z: 3.0,
                },
                Position::Xyz {
                    x: 4.0,
                    y: 5.0,
                    z: 6.0,
                },
            ]),
            GeometryShape::MultiLineString(vec![
                LineStringValue::new(vec![
                    Position::Xyz {
                        x: 0.0,
                        y: 0.0,
                        z: 1.0,
                    },
                    Position::Xyz {
                        x: 1.0,
                        y: 1.0,
                        z: 1.0,
                    },
                ])
                .unwrap(),
                LineStringValue::new(vec![
                    Position::Xyz {
                        x: 2.0,
                        y: 2.0,
                        z: 2.0,
                    },
                    Position::Xyz {
                        x: 3.0,
                        y: 3.0,
                        z: 2.0,
                    },
                ])
                .unwrap(),
            ]),
            GeometryShape::MultiPolygon(vec![
                PolygonValue::new(vec![sample_xyz_ring()]).unwrap(),
                PolygonValue::new(vec![sample_xyz_ring()]).unwrap(),
            ]),
            GeometryShape::GeometryCollection(vec![
                GeometryShape::Point(Position::Xyz {
                    x: 12.5,
                    y: 55.7,
                    z: 9.0,
                }),
                GeometryShape::LineString(
                    LineStringValue::new(vec![
                        Position::Xyz {
                            x: 12.5,
                            y: 55.7,
                            z: 9.0,
                        },
                        Position::Xyz {
                            x: 13.1,
                            y: 56.0,
                            z: 10.5,
                        },
                    ])
                    .unwrap(),
                ),
            ]),
        ]
    }

    #[test]
    fn roundtrip_null() {
        let storage = encode_property_value(&PropertyValue::Null).unwrap();
        assert_eq!(storage, StorageValue::Null);
        let decoded = decode_property_value(storage.as_ref()).unwrap();
        assert_eq!(decoded, PropertyValue::Null);
    }

    #[test]
    fn roundtrip_bool() {
        let storage = encode_property_value(&PropertyValue::Bool(true)).unwrap();
        assert_eq!(storage, StorageValue::Blob(vec![tag::BOOL_TRUE]));
        let decoded = decode_property_value(storage.as_ref()).unwrap();
        assert_eq!(decoded, PropertyValue::Bool(true));
    }

    #[test]
    fn roundtrip_integer() {
        let storage = encode_property_value(&PropertyValue::Integer(42)).unwrap();
        assert_eq!(storage, StorageValue::Integer(42));
        let decoded = decode_property_value(storage.as_ref()).unwrap();
        assert_eq!(decoded, PropertyValue::Integer(42));
    }

    #[test]
    fn roundtrip_float() {
        let storage = encode_property_value(&PropertyValue::Float(42.5)).unwrap();
        assert_eq!(storage, StorageValue::Real(42.5));
        let decoded = decode_property_value(storage.as_ref()).unwrap();
        assert_eq!(decoded, PropertyValue::Float(42.5));
    }

    #[test]
    fn roundtrip_string() {
        let storage = encode_property_value(&PropertyValue::String("hello".into())).unwrap();
        assert_eq!(storage, StorageValue::Text("hello".into()));
        let decoded = decode_property_value(storage.as_ref()).unwrap();
        assert_eq!(decoded, PropertyValue::String("hello".into()));
    }

    #[test]
    fn roundtrip_date() {
        assert_roundtrip(PropertyValue::Date(DateValue::new(2026, 4, 6).unwrap()));
    }

    #[test]
    fn roundtrip_local_time_with_fraction() {
        assert_roundtrip(PropertyValue::LocalTime(
            LocalTimeValue::new(21, 40, 32, 142_000_000).unwrap(),
        ));
    }

    #[test]
    fn temporal_storage_omits_zero_seconds() {
        let storage = encode_property_value(&PropertyValue::LocalTime(
            LocalTimeValue::new(10, 35, 0, 0).unwrap(),
        ))
        .unwrap();
        assert_eq!(storage, StorageValue::Blob(b"\x1110:35".to_vec()));

        let storage = encode_property_value(&PropertyValue::ZonedTime(
            "10:35:00-08:00".parse::<ZonedTimeValue>().unwrap(),
        ))
        .unwrap();
        assert_eq!(storage, StorageValue::Blob(b"\x1210:35-08:00".to_vec()));
    }

    #[test]
    fn roundtrip_zoned_time() {
        assert_roundtrip(PropertyValue::ZonedTime(
            "12:00:00+01:00".parse::<ZonedTimeValue>().unwrap(),
        ));
    }

    #[test]
    fn roundtrip_local_datetime() {
        assert_roundtrip(PropertyValue::LocalDateTime(
            LocalDateTimeValue::new(2026, 4, 6, 12, 0, 1, 120_000_000).unwrap(),
        ));
    }

    #[test]
    fn roundtrip_zoned_datetime_offset_only() {
        assert_roundtrip(PropertyValue::ZonedDateTime(
            "2026-04-06T12:00:00+01:00"
                .parse::<ZonedDateTimeValue>()
                .unwrap(),
        ));
    }

    #[cfg(any(feature = "tzdb-system", feature = "tzdb-bundled"))]
    #[test]
    fn roundtrip_zoned_datetime_named_zone() {
        assert_roundtrip(PropertyValue::ZonedDateTime(
            "2026-04-06T12:00:00+01:00[Europe/Lisbon]"
                .parse::<ZonedDateTimeValue>()
                .unwrap(),
        ));
    }

    #[test]
    fn roundtrip_duration() {
        assert_roundtrip(PropertyValue::Duration(
            "P2M10DT2H30M".parse::<DurationValue>().unwrap(),
        ));
    }

    #[test]
    fn roundtrip_string_list_json() {
        assert_roundtrip(PropertyValue::List(ListValue::String(vec![
            Some("Ada".into()),
            None,
            Some("Bob".into()),
        ])));
    }

    #[test]
    fn roundtrip_string_list_json_with_escapes_and_unicode() {
        assert_roundtrip(PropertyValue::List(ListValue::String(vec![
            Some("Ada \"Lovelace\"".into()),
            Some("line\\break".into()),
            Some("Lisboa €".into()),
            None,
        ])));
    }

    #[test]
    fn roundtrip_bool_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Bool(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ])));
    }

    #[test]
    fn roundtrip_bool_list_without_nulls() {
        assert_roundtrip(PropertyValue::List(ListValue::Bool(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
        ])));
    }

    #[test]
    fn roundtrip_bool_list_crosses_bitmap_byte_boundary() {
        assert_roundtrip(PropertyValue::List(ListValue::Bool(vec![
            None,
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            None,
            None,
        ])));
    }

    #[test]
    fn roundtrip_empty_bool_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Bool(vec![])));
    }

    #[test]
    fn roundtrip_int_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Int64(vec![
            Some(1),
            None,
            Some(2),
            Some(4),
        ])));
    }

    #[test]
    fn roundtrip_int_list_without_nulls() {
        assert_roundtrip(PropertyValue::List(ListValue::Int64(vec![
            Some(-9),
            Some(-1),
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(5),
            Some(8),
            Some(13),
        ])));
    }

    #[test]
    fn roundtrip_int_list_crosses_bitmap_byte_boundary() {
        assert_roundtrip(PropertyValue::List(ListValue::Int64(vec![
            Some(-9),
            None,
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(5),
            None,
            Some(13),
        ])));
    }

    #[test]
    fn roundtrip_empty_int_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Int64(vec![])));
    }

    #[test]
    fn roundtrip_float_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Float64(vec![
            Some(1.5),
            None,
            Some(2.25),
        ])));
    }

    #[test]
    fn roundtrip_float_list_without_nulls() {
        assert_roundtrip(PropertyValue::List(ListValue::Float64(vec![
            Some(-9.5),
            Some(-1.25),
            Some(0.0),
            Some(1.0),
            Some(2.0),
            Some(3.5),
            Some(5.75),
            Some(8.0),
            Some(13.125),
        ])));
    }

    #[test]
    fn roundtrip_float_list_crosses_bitmap_byte_boundary() {
        assert_roundtrip(PropertyValue::List(ListValue::Float64(vec![
            Some(-9.5),
            None,
            Some(0.0),
            Some(1.0),
            Some(2.25),
            Some(3.5),
            Some(5.75),
            None,
            Some(13.125),
        ])));
    }

    #[test]
    fn roundtrip_empty_float_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Float64(vec![])));
    }

    #[test]
    fn roundtrip_generic_list() {
        assert_roundtrip(PropertyValue::List(ListValue::Generic(vec![
            PropertyValue::Integer(1),
            PropertyValue::Bool(true),
            PropertyValue::String("x".into()),
            PropertyValue::List(ListValue::Generic(vec![PropertyValue::Integer(2)])),
        ])));
    }

    #[test]
    fn roundtrip_generic_list_with_null_and_escaped_strings() {
        assert_roundtrip(PropertyValue::List(ListValue::Generic(vec![
            PropertyValue::Null,
            PropertyValue::String("Ada \"Lovelace\"".into()),
            PropertyValue::String("slash\\\\path".into()),
            PropertyValue::String("Lisboa €".into()),
            PropertyValue::List(ListValue::Generic(vec![
                PropertyValue::Null,
                PropertyValue::String("nested".into()),
            ])),
        ])));
    }

    #[test]
    fn generic_list_rejects_temporal_value() {
        let value = PropertyValue::List(ListValue::Generic(vec![PropertyValue::Date(
            DateValue::new(2026, 4, 6).unwrap(),
        )]));
        assert!(encode_property_value(&value).is_err());
    }

    #[test]
    fn roundtrip_vector() {
        assert_roundtrip(PropertyValue::Vector(VectorValue {
            coord_type: VectorType::Float32,
            values: VectorStorage::F32(vec![1.05, 0.123, 5.0]),
        }));
    }

    #[test]
    fn roundtrip_all_vector_storage_types() {
        let cases = vec![
            PropertyValue::Vector(VectorValue {
                coord_type: VectorType::Integer8,
                values: VectorStorage::I8(vec![-3, 0, 12]),
            }),
            PropertyValue::Vector(VectorValue {
                coord_type: VectorType::Integer16,
                values: VectorStorage::I16(vec![-300, 0, 1200]),
            }),
            PropertyValue::Vector(VectorValue {
                coord_type: VectorType::Integer32,
                values: VectorStorage::I32(vec![-30_000, 0, 120_000]),
            }),
            PropertyValue::Vector(VectorValue {
                coord_type: VectorType::Integer64,
                values: VectorStorage::I64(vec![-3_000_000, 0, 12_000_000]),
            }),
            PropertyValue::Vector(VectorValue {
                coord_type: VectorType::Float64,
                values: VectorStorage::F64(vec![1.25, -5.5, 0.0]),
            }),
        ];
        for case in cases {
            assert_roundtrip(case);
        }
    }

    #[test]
    fn roundtrip_bytes() {
        assert_roundtrip(PropertyValue::Bytes(vec![0x00, 0x01, 0xfe, 0xff]));
    }

    #[test]
    fn roundtrip_point() {
        assert_roundtrip(PropertyValue::Point(PointValue {
            srid: 4326,
            position: Position::Xy { x: 12.5, y: 55.7 },
        }));
    }

    #[test]
    fn roundtrip_point_xyz() {
        assert_roundtrip(PropertyValue::Point(PointValue {
            srid: 4326,
            position: Position::Xyz {
                x: 12.5,
                y: 55.7,
                z: 9.0,
            },
        }));
    }

    #[test]
    fn roundtrip_geometry_family() {
        for shape in sample_supported_shapes() {
            assert_roundtrip(PropertyValue::Geometry(GeometryValue { srid: 4326, shape }));
        }
    }

    #[test]
    fn roundtrip_geography_family() {
        for shape in sample_supported_shapes() {
            assert_roundtrip(PropertyValue::Geography(GeographyValue {
                srid: 4326,
                shape,
            }));
        }
    }
}
