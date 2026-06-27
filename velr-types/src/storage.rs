#[derive(Debug, Clone, PartialEq)]
pub enum StorageValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum StorageValueRef<'a> {
    Null,
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl StorageValue {
    pub fn as_ref(&self) -> StorageValueRef<'_> {
        match self {
            Self::Null => StorageValueRef::Null,
            Self::Integer(v) => StorageValueRef::Integer(*v),
            Self::Real(v) => StorageValueRef::Real(*v),
            Self::Text(v) => StorageValueRef::Text(v),
            Self::Blob(v) => StorageValueRef::Blob(v),
        }
    }
}
