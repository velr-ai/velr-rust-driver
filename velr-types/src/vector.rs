use crate::error::{DecodeError, EncodeError};
use crate::tag;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum VectorType {
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    Float32,
    Float64,
}

impl VectorType {
    pub fn code(self) -> u8 {
        match self {
            Self::Integer8 => 0x01,
            Self::Integer16 => 0x02,
            Self::Integer32 => 0x03,
            Self::Integer64 => 0x04,
            Self::Float32 => 0x11,
            Self::Float64 => 0x12,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, DecodeError> {
        match code {
            0x01 => Ok(Self::Integer8),
            0x02 => Ok(Self::Integer16),
            0x03 => Ok(Self::Integer32),
            0x04 => Ok(Self::Integer64),
            0x11 => Ok(Self::Float32),
            0x12 => Ok(Self::Float64),
            _ => Err(DecodeError::InvalidVector(format!(
                "unknown vector coordinate type 0x{code:02x}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorStorage {
    I8(Vec<i8>),
    I16(Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum VectorElem {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorValue {
    pub coord_type: VectorType,
    pub values: VectorStorage,
}

impl VectorValue {
    pub fn len(&self) -> usize {
        match &self.values {
            VectorStorage::I8(v) => v.len(),
            VectorStorage::I16(v) => v.len(),
            VectorStorage::I32(v) => v.len(),
            VectorStorage::I64(v) => v.len(),
            VectorStorage::F32(v) => v.len(),
            VectorStorage::F64(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> VectorIter<'_> {
        match &self.values {
            VectorStorage::I8(v) => VectorIter::I8(v.iter()),
            VectorStorage::I16(v) => VectorIter::I16(v.iter()),
            VectorStorage::I32(v) => VectorIter::I32(v.iter()),
            VectorStorage::I64(v) => VectorIter::I64(v.iter()),
            VectorStorage::F32(v) => VectorIter::F32(v.iter()),
            VectorStorage::F64(v) => VectorIter::F64(v.iter()),
        }
    }

    pub fn validate(&self) -> Result<(), EncodeError> {
        let ok = matches!(
            (&self.coord_type, &self.values),
            (VectorType::Integer8, VectorStorage::I8(_))
                | (VectorType::Integer16, VectorStorage::I16(_))
                | (VectorType::Integer32, VectorStorage::I32(_))
                | (VectorType::Integer64, VectorStorage::I64(_))
                | (VectorType::Float32, VectorStorage::F32(_))
                | (VectorType::Float64, VectorStorage::F64(_))
        );
        if ok {
            Ok(())
        } else {
            Err(EncodeError::InvalidVector(
                "coord_type does not match vector storage",
            ))
        }
    }

    pub fn encode_blob(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate()?;
        if self.len() > u16::MAX as usize {
            return Err(EncodeError::InvalidVector("vector length exceeds u16::MAX"));
        }
        let mut out = Vec::with_capacity(1 + 1 + 2 + self.len() * 8);
        out.push(tag::VECTOR);
        out.push(self.coord_type.code());
        out.extend_from_slice(&(self.len() as u16).to_le_bytes());
        match &self.values {
            VectorStorage::I8(values) => {
                for value in values {
                    out.push(*value as u8);
                }
            }
            VectorStorage::I16(values) => {
                for value in values {
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            VectorStorage::I32(values) => {
                for value in values {
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            VectorStorage::I64(values) => {
                for value in values {
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            VectorStorage::F32(values) => {
                for value in values {
                    if !value.is_finite() {
                        return Err(EncodeError::NonCanonicalValue(
                            "non-finite float in Float32 vector",
                        ));
                    }
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
            VectorStorage::F64(values) => {
                for value in values {
                    if !value.is_finite() {
                        return Err(EncodeError::NonCanonicalValue(
                            "non-finite float in Float64 vector",
                        ));
                    }
                    out.extend_from_slice(&value.to_le_bytes());
                }
            }
        }
        Ok(out)
    }

    pub fn decode_blob(blob: &[u8]) -> Result<Self, DecodeError> {
        if blob.is_empty() {
            return Err(DecodeError::EmptyInput);
        }
        if blob[0] != tag::VECTOR {
            return Err(DecodeError::UnexpectedTag {
                expected: "vector tag",
                actual: blob[0],
            });
        }
        if blob.len() < 4 {
            return Err(DecodeError::Truncated);
        }
        let coord_type = VectorType::from_code(blob[1])?;
        let len = u16::from_le_bytes([blob[2], blob[3]]) as usize;
        let payload = &blob[4..];
        let values = match coord_type {
            VectorType::Integer8 => {
                if payload.len() != len {
                    return Err(DecodeError::InvalidVector(
                        "invalid i8 payload length".into(),
                    ));
                }
                VectorStorage::I8(payload.iter().map(|b| *b as i8).collect())
            }
            VectorType::Integer16 => {
                if payload.len() != len * 2 {
                    return Err(DecodeError::InvalidVector(
                        "invalid i16 payload length".into(),
                    ));
                }
                let mut out = Vec::with_capacity(len);
                for chunk in payload.chunks_exact(2) {
                    out.push(i16::from_le_bytes([chunk[0], chunk[1]]));
                }
                VectorStorage::I16(out)
            }
            VectorType::Integer32 => {
                if payload.len() != len * 4 {
                    return Err(DecodeError::InvalidVector(
                        "invalid i32 payload length".into(),
                    ));
                }
                let mut out = Vec::with_capacity(len);
                for chunk in payload.chunks_exact(4) {
                    out.push(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                }
                VectorStorage::I32(out)
            }
            VectorType::Integer64 => {
                if payload.len() != len * 8 {
                    return Err(DecodeError::InvalidVector(
                        "invalid i64 payload length".into(),
                    ));
                }
                let mut out = Vec::with_capacity(len);
                for chunk in payload.chunks_exact(8) {
                    out.push(i64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ]));
                }
                VectorStorage::I64(out)
            }
            VectorType::Float32 => {
                if payload.len() != len * 4 {
                    return Err(DecodeError::InvalidVector(
                        "invalid f32 payload length".into(),
                    ));
                }
                let mut out = Vec::with_capacity(len);
                for chunk in payload.chunks_exact(4) {
                    let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    if !value.is_finite() {
                        return Err(DecodeError::NonCanonical(
                            "non-finite float in Float32 vector",
                        ));
                    }
                    out.push(value);
                }
                VectorStorage::F32(out)
            }
            VectorType::Float64 => {
                if payload.len() != len * 8 {
                    return Err(DecodeError::InvalidVector(
                        "invalid f64 payload length".into(),
                    ));
                }
                let mut out = Vec::with_capacity(len);
                for chunk in payload.chunks_exact(8) {
                    let value = f64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ]);
                    if !value.is_finite() {
                        return Err(DecodeError::NonCanonical(
                            "non-finite float in Float64 vector",
                        ));
                    }
                    out.push(value);
                }
                VectorStorage::F64(out)
            }
        };
        Ok(Self { coord_type, values })
    }
}

pub enum VectorIter<'a> {
    I8(std::slice::Iter<'a, i8>),
    I16(std::slice::Iter<'a, i16>),
    I32(std::slice::Iter<'a, i32>),
    I64(std::slice::Iter<'a, i64>),
    F32(std::slice::Iter<'a, f32>),
    F64(std::slice::Iter<'a, f64>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_vector_roundtrip(value: VectorValue) {
        let decoded = VectorValue::decode_blob(&value.encode_blob().unwrap()).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn roundtrip_empty_vector() {
        assert_vector_roundtrip(VectorValue {
            coord_type: VectorType::Integer16,
            values: VectorStorage::I16(vec![]),
        });
    }

    #[test]
    fn roundtrip_single_value_vector() {
        assert_vector_roundtrip(VectorValue {
            coord_type: VectorType::Integer8,
            values: VectorStorage::I8(vec![-7]),
        });
    }

    #[test]
    fn roundtrip_longer_vector() {
        let values = (0..257).map(|v| v as i32 - 128).collect::<Vec<_>>();
        assert_vector_roundtrip(VectorValue {
            coord_type: VectorType::Integer32,
            values: VectorStorage::I32(values),
        });
    }

    #[test]
    fn rejects_oversized_vector() {
        let value = VectorValue {
            coord_type: VectorType::Integer8,
            values: VectorStorage::I8(vec![0; u16::MAX as usize + 1]),
        };
        assert!(matches!(
            value.encode_blob(),
            Err(EncodeError::InvalidVector("vector length exceeds u16::MAX"))
        ));
    }

    #[test]
    fn rejects_mismatched_coord_type_and_storage() {
        let value = VectorValue {
            coord_type: VectorType::Integer16,
            values: VectorStorage::I8(vec![1, 2, 3]),
        };
        assert!(matches!(
            value.encode_blob(),
            Err(EncodeError::InvalidVector(
                "coord_type does not match vector storage"
            ))
        ));
    }

    #[test]
    fn rejects_non_finite_float_vector_on_encode() {
        let value = VectorValue {
            coord_type: VectorType::Float64,
            values: VectorStorage::F64(vec![1.0, f64::NAN]),
        };
        assert!(matches!(
            value.encode_blob(),
            Err(EncodeError::NonCanonicalValue(
                "non-finite float in Float64 vector"
            ))
        ));
    }

    #[test]
    fn rejects_non_finite_float_vector_on_decode() {
        let mut blob = vec![tag::VECTOR, VectorType::Float64.code()];
        blob.extend_from_slice(&(2u16).to_le_bytes());
        blob.extend_from_slice(&1.0f64.to_le_bytes());
        blob.extend_from_slice(&f64::INFINITY.to_le_bytes());
        assert!(matches!(
            VectorValue::decode_blob(&blob),
            Err(DecodeError::NonCanonical(
                "non-finite float in Float64 vector"
            ))
        ));
    }
}

impl<'a> Iterator for VectorIter<'a> {
    type Item = VectorElem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::I8(iter) => iter.next().copied().map(VectorElem::I8),
            Self::I16(iter) => iter.next().copied().map(VectorElem::I16),
            Self::I32(iter) => iter.next().copied().map(VectorElem::I32),
            Self::I64(iter) => iter.next().copied().map(VectorElem::I64),
            Self::F32(iter) => iter.next().copied().map(VectorElem::F32),
            Self::F64(iter) => iter.next().copied().map(VectorElem::F64),
        }
    }
}
