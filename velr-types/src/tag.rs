#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TagKind {
    Bool,
    Binary,
    UtilityJson,
    Temporal,
    Spatial,
    List,
    Vector,
    Reserved,
    Extension,
}

pub const BOOL_FALSE: u8 = 0x00;
pub const BOOL_TRUE: u8 = 0x01;
pub const BINARY: u8 = 0x02;
pub const JSON: u8 = 0x03;

pub const DATE: u8 = 0x10;
pub const LOCAL_TIME: u8 = 0x11;
pub const ZONED_TIME: u8 = 0x12;
pub const LOCAL_DATETIME: u8 = 0x13;
pub const ZONED_DATETIME: u8 = 0x14;
pub const DURATION: u8 = 0x15;

pub const POINT: u8 = 0x20;
pub const GEOMETRY: u8 = 0x21;
pub const GEOGRAPHY: u8 = 0x22;

pub const LIST_JSON: u8 = 0x30;
pub const STRING_LIST_JSON: u8 = 0x31;
pub const MAP_JSON: u8 = 0x32;
pub const NODE_JSON: u8 = 0x33;
pub const RELATIONSHIP_JSON: u8 = 0x34;
pub const PATH_JSON: u8 = 0x35;
pub const BOOL_LIST: u8 = 0x36;
pub const INT64_LIST: u8 = 0x37;
pub const FLOAT64_LIST: u8 = 0x38;

pub const VECTOR: u8 = 0x40;

/// Internal-only query-pipeline references.
///
/// These tags must never be persisted as property values or exposed through the
/// driver edge. They let hybrid execution sort and page graph values cheaply,
/// then materialize the public JSON envelopes only at RETURN/WITH boundaries.
pub const RUNTIME_NODE_REF: u8 = 0x80;
pub const RUNTIME_REL_REF: u8 = 0x81;
pub const RUNTIME_PATH_REF: u8 = 0x82;
pub const RUNTIME_NAN: u8 = 0x83;

pub fn classify_tag(tag: u8) -> TagKind {
    match tag {
        BOOL_FALSE | BOOL_TRUE => TagKind::Bool,
        BINARY => TagKind::Binary,
        JSON => TagKind::UtilityJson,
        DATE | LOCAL_TIME | ZONED_TIME | LOCAL_DATETIME | ZONED_DATETIME | DURATION => {
            TagKind::Temporal
        }
        POINT | GEOMETRY | GEOGRAPHY => TagKind::Spatial,
        LIST_JSON | STRING_LIST_JSON | MAP_JSON | NODE_JSON | RELATIONSHIP_JSON | PATH_JSON
        | BOOL_LIST | INT64_LIST | FLOAT64_LIST => TagKind::List,
        VECTOR => TagKind::Vector,
        0x23..=0x2f | 0x41..=0x7f => TagKind::Reserved,
        0x80..=0xff => TagKind::Extension,
        _ => TagKind::Reserved,
    }
}
