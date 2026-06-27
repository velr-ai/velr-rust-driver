use crate::error::{DecodeError, EncodeError};
use crate::tag;
use serde_json::{Map, Number, Value};
use std::fmt::Write;

const EWKB_Z_FLAG: u32 = 0x8000_0000;
const EWKB_M_FLAG: u32 = 0x4000_0000;
const EWKB_SRID_FLAG: u32 = 0x2000_0000;

const EWKB_POINT: u32 = 1;
const EWKB_LINESTRING: u32 = 2;
const EWKB_POLYGON: u32 = 3;
const EWKB_MULTI_POINT: u32 = 4;
const EWKB_MULTI_LINESTRING: u32 = 5;
const EWKB_MULTI_POLYGON: u32 = 6;
const EWKB_GEOMETRY_COLLECTION: u32 = 7;

const DEFAULT_GEOJSON_SRID: u32 = 4326;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Position {
    Xy { x: f64, y: f64 },
    Xyz { x: f64, y: f64, z: f64 },
}

impl Position {
    pub fn dimension(&self) -> usize {
        match self {
            Self::Xy { .. } => 2,
            Self::Xyz { .. } => 3,
        }
    }

    pub fn x(&self) -> f64 {
        match self {
            Self::Xy { x, .. } | Self::Xyz { x, .. } => *x,
        }
    }

    pub fn y(&self) -> f64 {
        match self {
            Self::Xy { y, .. } | Self::Xyz { y, .. } => *y,
        }
    }

    pub fn z(&self) -> Option<f64> {
        match self {
            Self::Xy { .. } => None,
            Self::Xyz { z, .. } => Some(*z),
        }
    }

    fn to_geojson_value(&self) -> Result<Value, EncodeError> {
        let mut out = vec![json_number(self.x())?, json_number(self.y())?];
        if let Some(z) = self.z() {
            out.push(json_number(z)?);
        }
        Ok(Value::Array(out))
    }

    fn from_geojson_value(value: &Value) -> Result<Self, DecodeError> {
        let coords = value
            .as_array()
            .ok_or_else(|| DecodeError::InvalidSpatial("coordinate must be a JSON array".into()))?;
        match coords.as_slice() {
            [x, y] => Ok(Self::Xy {
                x: json_f64(x)?,
                y: json_f64(y)?,
            }),
            [x, y, z] => Ok(Self::Xyz {
                x: json_f64(x)?,
                y: json_f64(y)?,
                z: json_f64(z)?,
            }),
            _ => Err(DecodeError::InvalidSpatial(
                "coordinates must contain exactly 2 or 3 numbers".into(),
            )),
        }
    }

    fn to_wkt_coords(&self) -> Result<String, EncodeError> {
        let mut out = String::new();
        write!(
            &mut out,
            "{} {}",
            format_wkt_number(self.x())?,
            format_wkt_number(self.y())?
        )
        .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT coordinate"))?;
        if let Some(z) = self.z() {
            write!(&mut out, " {}", format_wkt_number(z)?)
                .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT coordinate"))?;
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PointValue {
    pub srid: u32,
    pub position: Position,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LineStringValue {
    positions: Vec<Position>,
}

impl LineStringValue {
    pub fn new(positions: Vec<Position>) -> Result<Self, EncodeError> {
        ensure_consistent_dimensions(&positions)?;
        Ok(Self { positions })
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Position> {
        self.positions.iter()
    }

    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    fn from_positions_unchecked(positions: Vec<Position>) -> Self {
        Self { positions }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LinearRingValue {
    positions: Vec<Position>,
}

impl LinearRingValue {
    pub fn new(positions: Vec<Position>) -> Result<Self, EncodeError> {
        ensure_consistent_dimensions(&positions)?;
        if !positions.is_empty() {
            if positions.len() < 4 {
                return Err(EncodeError::InvalidSpatial(
                    "linear ring must have at least 4 positions",
                ));
            }
            if positions.first() != positions.last() {
                return Err(EncodeError::InvalidSpatial("linear ring must be closed"));
            }
        }
        Ok(Self { positions })
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Position> {
        self.positions.iter()
    }

    pub fn positions(&self) -> &[Position] {
        &self.positions
    }

    fn from_positions_unchecked(positions: Vec<Position>) -> Self {
        Self { positions }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PolygonValue {
    rings: Vec<LinearRingValue>,
}

impl PolygonValue {
    pub fn new(rings: Vec<LinearRingValue>) -> Result<Self, EncodeError> {
        Ok(Self { rings })
    }

    pub fn len(&self) -> usize {
        self.rings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rings.is_empty()
    }

    pub fn rings(&self) -> std::slice::Iter<'_, LinearRingValue> {
        self.rings.iter()
    }

    pub fn ring_slice(&self) -> &[LinearRingValue] {
        &self.rings
    }

    fn from_rings_unchecked(rings: Vec<LinearRingValue>) -> Self {
        Self { rings }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum GeometryShape {
    Point(Position),
    LineString(LineStringValue),
    Polygon(PolygonValue),
    MultiPoint(Vec<Position>),
    MultiLineString(Vec<LineStringValue>),
    MultiPolygon(Vec<PolygonValue>),
    GeometryCollection(Vec<GeometryShape>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeometryValue {
    pub srid: u32,
    pub shape: GeometryShape,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeographyValue {
    pub srid: u32,
    pub shape: GeometryShape,
}

impl PointValue {
    pub fn to_geojson_value(&self) -> Result<Value, EncodeError> {
        let mut obj = Map::new();
        obj.insert("type".into(), Value::String("Point".into()));
        obj.insert("coordinates".into(), self.position.to_geojson_value()?);
        Ok(Value::Object(obj))
    }

    pub fn to_geojson_string(&self) -> Result<String, EncodeError> {
        serde_json::to_string(&self.to_geojson_value()?)
            .map_err(|_| EncodeError::InvalidSpatial("failed to encode GeoJSON point"))
    }

    pub fn from_geojson_value(value: &Value) -> Result<Self, DecodeError> {
        let shape = GeometryShape::from_geojson_value(value)?;
        match shape {
            GeometryShape::Point(position) => Ok(Self {
                srid: DEFAULT_GEOJSON_SRID,
                position,
            }),
            _ => Err(DecodeError::InvalidSpatial(
                "GeoJSON value must be a Point for PointValue".into(),
            )),
        }
    }

    pub fn from_geojson_str(s: &str) -> Result<Self, DecodeError> {
        let value: Value =
            serde_json::from_str(s).map_err(|e| DecodeError::InvalidJson(e.to_string()))?;
        Self::from_geojson_value(&value)
    }

    pub fn to_wkt_string(&self) -> Result<String, EncodeError> {
        geometry_shape_to_wkt(&GeometryShape::Point(self.position))
    }

    pub fn to_ewkt_string(&self) -> Result<String, EncodeError> {
        Ok(format!("SRID={};{}", self.srid, self.to_wkt_string()?))
    }

    pub fn from_wkt_str(s: &str) -> Result<Self, DecodeError> {
        let (srid, shape) = parse_wkt_or_ewkt(s)?;
        match shape {
            GeometryShape::Point(position) => Ok(Self {
                srid: srid.unwrap_or(DEFAULT_GEOJSON_SRID),
                position,
            }),
            _ => Err(DecodeError::InvalidSpatial(
                "WKT value must be a Point for PointValue".into(),
            )),
        }
    }
}

impl GeometryValue {
    pub fn to_geojson_value(&self) -> Result<Value, EncodeError> {
        self.shape.to_geojson_value()
    }

    pub fn to_geojson_string(&self) -> Result<String, EncodeError> {
        serde_json::to_string(&self.to_geojson_value()?)
            .map_err(|_| EncodeError::InvalidSpatial("failed to encode GeoJSON geometry"))
    }

    pub fn from_geojson_value(value: &Value) -> Result<Self, DecodeError> {
        Ok(Self {
            srid: DEFAULT_GEOJSON_SRID,
            shape: GeometryShape::from_geojson_value(value)?,
        })
    }

    pub fn from_geojson_str(s: &str) -> Result<Self, DecodeError> {
        let value: Value =
            serde_json::from_str(s).map_err(|e| DecodeError::InvalidJson(e.to_string()))?;
        Self::from_geojson_value(&value)
    }

    pub fn to_wkt_string(&self) -> Result<String, EncodeError> {
        geometry_shape_to_wkt(&self.shape)
    }

    pub fn to_ewkt_string(&self) -> Result<String, EncodeError> {
        Ok(format!("SRID={};{}", self.srid, self.to_wkt_string()?))
    }

    pub fn from_wkt_str(s: &str) -> Result<Self, DecodeError> {
        let (srid, shape) = parse_wkt_or_ewkt(s)?;
        Ok(Self {
            srid: srid.unwrap_or(DEFAULT_GEOJSON_SRID),
            shape,
        })
    }
}

impl GeographyValue {
    pub fn to_geojson_value(&self) -> Result<Value, EncodeError> {
        self.shape.to_geojson_value()
    }

    pub fn to_geojson_string(&self) -> Result<String, EncodeError> {
        serde_json::to_string(&self.to_geojson_value()?)
            .map_err(|_| EncodeError::InvalidSpatial("failed to encode GeoJSON geography"))
    }

    pub fn from_geojson_value(value: &Value) -> Result<Self, DecodeError> {
        Ok(Self {
            srid: DEFAULT_GEOJSON_SRID,
            shape: GeometryShape::from_geojson_value(value)?,
        })
    }

    pub fn from_geojson_str(s: &str) -> Result<Self, DecodeError> {
        let value: Value =
            serde_json::from_str(s).map_err(|e| DecodeError::InvalidJson(e.to_string()))?;
        Self::from_geojson_value(&value)
    }

    pub fn to_wkt_string(&self) -> Result<String, EncodeError> {
        geometry_shape_to_wkt(&self.shape)
    }

    pub fn to_ewkt_string(&self) -> Result<String, EncodeError> {
        Ok(format!("SRID={};{}", self.srid, self.to_wkt_string()?))
    }

    pub fn from_wkt_str(s: &str) -> Result<Self, DecodeError> {
        let (srid, shape) = parse_wkt_or_ewkt(s)?;
        Ok(Self {
            srid: srid.unwrap_or(DEFAULT_GEOJSON_SRID),
            shape,
        })
    }
}

impl PointValue {
    pub fn encode_blob(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = vec![tag::POINT];
        encode_geometry_record(
            &mut out,
            EWKB_POINT,
            self.srid,
            &GeometryShape::Point(self.position),
            true,
        )?;
        Ok(out)
    }

    pub fn decode_blob(blob: &[u8]) -> Result<Self, DecodeError> {
        let (srid, shape) = decode_top_level_shape(blob, tag::POINT)?;
        match shape {
            GeometryShape::Point(position) => Ok(Self { srid, position }),
            _ => Err(DecodeError::InvalidSpatial(
                "point tag must decode to point shape".into(),
            )),
        }
    }
}

impl GeometryValue {
    pub fn encode_blob(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = vec![tag::GEOMETRY];
        encode_geometry_record(
            &mut out,
            self.shape.base_type(),
            self.srid,
            &self.shape,
            true,
        )?;
        Ok(out)
    }

    pub fn decode_blob(blob: &[u8]) -> Result<Self, DecodeError> {
        let (srid, shape) = decode_top_level_shape(blob, tag::GEOMETRY)?;
        Ok(Self { srid, shape })
    }
}

impl GeographyValue {
    pub fn encode_blob(&self) -> Result<Vec<u8>, EncodeError> {
        let mut out = vec![tag::GEOGRAPHY];
        encode_geometry_record(
            &mut out,
            self.shape.base_type(),
            self.srid,
            &self.shape,
            true,
        )?;
        Ok(out)
    }

    pub fn decode_blob(blob: &[u8]) -> Result<Self, DecodeError> {
        let (srid, shape) = decode_top_level_shape(blob, tag::GEOGRAPHY)?;
        Ok(Self { srid, shape })
    }
}

impl GeometryShape {
    pub fn to_geojson_value(&self) -> Result<Value, EncodeError> {
        let mut obj = Map::new();
        match self {
            Self::Point(position) => {
                obj.insert("type".into(), Value::String("Point".into()));
                obj.insert("coordinates".into(), position.to_geojson_value()?);
            }
            Self::LineString(line) => {
                obj.insert("type".into(), Value::String("LineString".into()));
                obj.insert(
                    "coordinates".into(),
                    Value::Array(
                        line.positions()
                            .iter()
                            .map(Position::to_geojson_value)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            Self::Polygon(poly) => {
                obj.insert("type".into(), Value::String("Polygon".into()));
                obj.insert(
                    "coordinates".into(),
                    Value::Array(
                        poly.ring_slice()
                            .iter()
                            .map(|ring| {
                                ring.positions()
                                    .iter()
                                    .map(Position::to_geojson_value)
                                    .collect::<Result<Vec<_>, _>>()
                                    .map(Value::Array)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            Self::MultiPoint(points) => {
                obj.insert("type".into(), Value::String("MultiPoint".into()));
                obj.insert(
                    "coordinates".into(),
                    Value::Array(
                        points
                            .iter()
                            .map(Position::to_geojson_value)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            Self::MultiLineString(lines) => {
                obj.insert("type".into(), Value::String("MultiLineString".into()));
                obj.insert(
                    "coordinates".into(),
                    Value::Array(
                        lines
                            .iter()
                            .map(|line| {
                                line.positions()
                                    .iter()
                                    .map(Position::to_geojson_value)
                                    .collect::<Result<Vec<_>, _>>()
                                    .map(Value::Array)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            Self::MultiPolygon(polys) => {
                obj.insert("type".into(), Value::String("MultiPolygon".into()));
                obj.insert(
                    "coordinates".into(),
                    Value::Array(
                        polys
                            .iter()
                            .map(|poly| {
                                poly.ring_slice()
                                    .iter()
                                    .map(|ring| {
                                        ring.positions()
                                            .iter()
                                            .map(Position::to_geojson_value)
                                            .collect::<Result<Vec<_>, _>>()
                                            .map(Value::Array)
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                                    .map(Value::Array)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
            Self::GeometryCollection(shapes) => {
                obj.insert("type".into(), Value::String("GeometryCollection".into()));
                obj.insert(
                    "geometries".into(),
                    Value::Array(
                        shapes
                            .iter()
                            .map(GeometryShape::to_geojson_value)
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                );
            }
        }
        Ok(Value::Object(obj))
    }

    pub fn from_geojson_value(value: &Value) -> Result<Self, DecodeError> {
        let obj = value.as_object().ok_or_else(|| {
            DecodeError::InvalidSpatial("GeoJSON geometry must be an object".into())
        })?;
        if obj.contains_key("crs") {
            return Err(DecodeError::InvalidSpatial(
                "GeoJSON CRS objects are out of scope".into(),
            ));
        }
        let kind = obj.get("type").and_then(Value::as_str).ok_or_else(|| {
            DecodeError::InvalidSpatial("GeoJSON geometry must contain a string type".into())
        })?;
        match kind {
            "Point" => Ok(Self::Point(Position::from_geojson_value(required(
                obj,
                "coordinates",
            )?)?)),
            "LineString" => {
                let positions = parse_positions_array(required(obj, "coordinates")?)?;
                Ok(Self::LineString(
                    LineStringValue::new(positions).map_err(map_spatial_encode)?,
                ))
            }
            "Polygon" => {
                let rings = parse_polygon_rings(required(obj, "coordinates")?)?;
                Ok(Self::Polygon(
                    PolygonValue::new(rings).map_err(map_spatial_encode)?,
                ))
            }
            "MultiPoint" => {
                let points = parse_positions_array(required(obj, "coordinates")?)?;
                ensure_consistent_dimensions(&points).map_err(map_spatial_encode)?;
                Ok(Self::MultiPoint(points))
            }
            "MultiLineString" => {
                let coords = required(obj, "coordinates")?.as_array().ok_or_else(|| {
                    DecodeError::InvalidSpatial(
                        "MultiLineString coordinates must be an array".into(),
                    )
                })?;
                let mut lines = Vec::with_capacity(coords.len());
                for line in coords {
                    lines.push(
                        LineStringValue::new(parse_positions_array(line)?)
                            .map_err(map_spatial_encode)?,
                    );
                }
                Ok(Self::MultiLineString(lines))
            }
            "MultiPolygon" => {
                let coords = required(obj, "coordinates")?.as_array().ok_or_else(|| {
                    DecodeError::InvalidSpatial("MultiPolygon coordinates must be an array".into())
                })?;
                let mut polys = Vec::with_capacity(coords.len());
                for poly in coords {
                    polys.push(
                        PolygonValue::new(parse_polygon_rings(poly)?)
                            .map_err(map_spatial_encode)?,
                    );
                }
                Ok(Self::MultiPolygon(polys))
            }
            "GeometryCollection" => {
                let geometries = required(obj, "geometries")?.as_array().ok_or_else(|| {
                    DecodeError::InvalidSpatial(
                        "GeometryCollection geometries must be an array".into(),
                    )
                })?;
                let mut shapes = Vec::with_capacity(geometries.len());
                for child in geometries {
                    shapes.push(Self::from_geojson_value(child)?);
                }
                Ok(Self::GeometryCollection(shapes))
            }
            "Feature" | "FeatureCollection" => Err(DecodeError::InvalidSpatial(
                "GeoJSON Feature wrappers are not spatial scalar values".into(),
            )),
            other => Err(DecodeError::InvalidSpatial(format!(
                "unsupported GeoJSON geometry type {other}"
            ))),
        }
    }

    fn base_type(&self) -> u32 {
        match self {
            Self::Point(_) => EWKB_POINT,
            Self::LineString(_) => EWKB_LINESTRING,
            Self::Polygon(_) => EWKB_POLYGON,
            Self::MultiPoint(_) => EWKB_MULTI_POINT,
            Self::MultiLineString(_) => EWKB_MULTI_LINESTRING,
            Self::MultiPolygon(_) => EWKB_MULTI_POLYGON,
            Self::GeometryCollection(_) => EWKB_GEOMETRY_COLLECTION,
        }
    }

    fn has_z(&self) -> bool {
        match self {
            Self::Point(pos) => pos.dimension() == 3,
            Self::LineString(line) => line.positions.first().map(Position::dimension) == Some(3),
            Self::Polygon(poly) => {
                poly.rings
                    .first()
                    .and_then(|ring| ring.positions.first())
                    .map(Position::dimension)
                    == Some(3)
            }
            Self::MultiPoint(points) => points.first().map(Position::dimension) == Some(3),
            Self::MultiLineString(lines) => {
                lines
                    .first()
                    .and_then(|line| line.positions.first())
                    .map(Position::dimension)
                    == Some(3)
            }
            Self::MultiPolygon(polys) => {
                polys
                    .first()
                    .and_then(|poly| poly.rings.first())
                    .and_then(|ring| ring.positions.first())
                    .map(Position::dimension)
                    == Some(3)
            }
            Self::GeometryCollection(shapes) => {
                shapes.first().map(GeometryShape::has_z).unwrap_or(false)
            }
        }
    }
}

fn geometry_shape_to_wkt(shape: &GeometryShape) -> Result<String, EncodeError> {
    let mut out = String::new();
    match shape {
        GeometryShape::Point(position) => {
            write!(
                &mut out,
                "POINT{} ({})",
                dim_suffix(true, position.dimension() == 3),
                position.to_wkt_coords()?
            )
            .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT point"))?;
        }
        GeometryShape::LineString(line) => {
            write!(&mut out, "LINESTRING{}", dim_suffix(true, shape.has_z()))
                .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT linestring"))?;
            if line.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                push_joined_positions(&mut out, line.positions())?;
                out.push(')');
            }
        }
        GeometryShape::Polygon(poly) => {
            write!(&mut out, "POLYGON{}", dim_suffix(true, shape.has_z()))
                .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT polygon"))?;
            if poly.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                for (idx, ring) in poly.ring_slice().iter().enumerate() {
                    if idx > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    push_joined_positions(&mut out, ring.positions())?;
                    out.push(')');
                }
                out.push(')');
            }
        }
        GeometryShape::MultiPoint(points) => {
            write!(&mut out, "MULTIPOINT{}", dim_suffix(true, shape.has_z()))
                .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT multipoint"))?;
            if points.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                for (idx, point) in points.iter().enumerate() {
                    if idx > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    out.push_str(&point.to_wkt_coords()?);
                    out.push(')');
                }
                out.push(')');
            }
        }
        GeometryShape::MultiLineString(lines) => {
            write!(
                &mut out,
                "MULTILINESTRING{}",
                dim_suffix(true, shape.has_z())
            )
            .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT multilinestring"))?;
            if lines.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                for (idx, line) in lines.iter().enumerate() {
                    if idx > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    push_joined_positions(&mut out, line.positions())?;
                    out.push(')');
                }
                out.push(')');
            }
        }
        GeometryShape::MultiPolygon(polys) => {
            write!(&mut out, "MULTIPOLYGON{}", dim_suffix(true, shape.has_z()))
                .map_err(|_| EncodeError::InvalidSpatial("failed to format WKT multipolygon"))?;
            if polys.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                for (pidx, poly) in polys.iter().enumerate() {
                    if pidx > 0 {
                        out.push_str(", ");
                    }
                    out.push('(');
                    for (ridx, ring) in poly.ring_slice().iter().enumerate() {
                        if ridx > 0 {
                            out.push_str(", ");
                        }
                        out.push('(');
                        push_joined_positions(&mut out, ring.positions())?;
                        out.push(')');
                    }
                    out.push(')');
                }
                out.push(')');
            }
        }
        GeometryShape::GeometryCollection(shapes) => {
            out.push_str("GEOMETRYCOLLECTION");
            if shapes.is_empty() {
                out.push_str(" EMPTY");
            } else {
                out.push_str(" (");
                for (idx, child) in shapes.iter().enumerate() {
                    if idx > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&geometry_shape_to_wkt(child)?);
                }
                out.push(')');
            }
        }
    }
    Ok(out)
}

fn push_joined_positions(out: &mut String, positions: &[Position]) -> Result<(), EncodeError> {
    for (idx, position) in positions.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(&position.to_wkt_coords()?);
    }
    Ok(())
}

fn dim_suffix(allow_collection_marker: bool, has_z: bool) -> &'static str {
    if has_z && allow_collection_marker {
        " Z"
    } else {
        ""
    }
}

fn required<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a Value, DecodeError> {
    obj.get(key)
        .ok_or_else(|| DecodeError::InvalidSpatial(format!("missing GeoJSON field {key}")))
}

fn json_number(value: f64) -> Result<Value, EncodeError> {
    if !value.is_finite() {
        return Err(EncodeError::InvalidSpatial(
            "spatial coordinates must be finite",
        ));
    }
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or(EncodeError::InvalidSpatial(
            "spatial coordinates must be finite",
        ))
}

fn json_f64(value: &Value) -> Result<f64, DecodeError> {
    let number = value
        .as_f64()
        .ok_or_else(|| DecodeError::InvalidSpatial("coordinate must be numeric".into()))?;
    if !number.is_finite() {
        return Err(DecodeError::InvalidSpatial(
            "spatial coordinates must be finite".into(),
        ));
    }
    Ok(number)
}

fn parse_positions_array(value: &Value) -> Result<Vec<Position>, DecodeError> {
    let coords = value
        .as_array()
        .ok_or_else(|| DecodeError::InvalidSpatial("coordinates must be an array".into()))?;
    let mut positions = Vec::with_capacity(coords.len());
    for coord in coords {
        positions.push(Position::from_geojson_value(coord)?);
    }
    Ok(positions)
}

fn parse_polygon_rings(value: &Value) -> Result<Vec<LinearRingValue>, DecodeError> {
    let rings = value.as_array().ok_or_else(|| {
        DecodeError::InvalidSpatial("polygon coordinates must be an array".into())
    })?;
    let mut out = Vec::with_capacity(rings.len());
    for ring in rings {
        out.push(LinearRingValue::new(parse_positions_array(ring)?).map_err(map_spatial_encode)?);
    }
    Ok(out)
}

fn map_spatial_encode(err: EncodeError) -> DecodeError {
    match err {
        EncodeError::InvalidSpatial(msg) => DecodeError::InvalidSpatial(msg.into()),
        _ => DecodeError::InvalidSpatial("invalid spatial value".into()),
    }
}

fn format_wkt_number(value: f64) -> Result<String, EncodeError> {
    if !value.is_finite() {
        return Err(EncodeError::InvalidSpatial(
            "spatial coordinates must be finite",
        ));
    }
    Ok(value.to_string())
}

fn parse_wkt_or_ewkt(s: &str) -> Result<(Option<u32>, GeometryShape), DecodeError> {
    let mut parser = WktParser::new(s);
    parser.skip_ws();
    let srid = if parser.consume_keyword_ci("SRID") {
        parser.skip_ws();
        parser.expect_char('=')?;
        let srid = parser.parse_u32()?;
        parser.skip_ws();
        parser.expect_char(';')?;
        Some(srid)
    } else {
        None
    };
    let shape = parser.parse_geometry_shape()?;
    parser.skip_ws();
    if !parser.is_eof() {
        return Err(DecodeError::NonCanonical("trailing bytes after WKT"));
    }
    Ok((srid, shape))
}

struct WktParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> WktParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.input.len()
    }

    fn skip_ws(&mut self) {
        while let Some(ch) = self.peek_char() {
            if ch.is_whitespace() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn consume_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.peek_char() == Some(expected) {
            self.pos += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), DecodeError> {
        if self.consume_char(expected) {
            Ok(())
        } else {
            Err(DecodeError::InvalidSpatial(format!(
                "expected '{expected}' in WKT"
            )))
        }
    }

    fn consume_keyword_ci(&mut self, keyword: &str) -> bool {
        self.skip_ws();
        let rest = &self.input[self.pos..];
        let Some(prefix) = rest.get(..keyword.len()) else {
            return false;
        };
        if prefix.eq_ignore_ascii_case(keyword) {
            self.pos += keyword.len();
            true
        } else {
            false
        }
    }

    fn parse_ident(&mut self) -> Result<String, DecodeError> {
        self.skip_ws();
        let start = self.pos;
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_alphabetic() {
                self.pos += ch.len_utf8();
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(DecodeError::InvalidSpatial(
                "expected geometry type in WKT".into(),
            ));
        }
        Ok(self.input[start..self.pos].to_ascii_uppercase())
    }

    fn parse_u32(&mut self) -> Result<u32, DecodeError> {
        let start = self.pos;
        self.skip_ws();
        let start = self.pos.max(start);
        while let Some(ch) = self.peek_char() {
            if ch.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(DecodeError::InvalidSpatial("expected integer".into()));
        }
        self.input[start..self.pos]
            .parse::<u32>()
            .map_err(|_| DecodeError::InvalidSpatial("invalid integer".into()))
    }

    fn parse_number(&mut self) -> Result<f64, DecodeError> {
        self.skip_ws();
        let start = self.pos;
        let bytes = self.input.as_bytes();
        while self.pos < bytes.len() {
            let ch = bytes[self.pos] as char;
            if ch.is_ascii_digit() || matches!(ch, '+' | '-' | '.' | 'e' | 'E') {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(DecodeError::InvalidSpatial("expected number in WKT".into()));
        }
        let value = self.input[start..self.pos]
            .parse::<f64>()
            .map_err(|_| DecodeError::InvalidSpatial("invalid number in WKT".into()))?;
        if !value.is_finite() {
            return Err(DecodeError::InvalidSpatial(
                "spatial coordinates must be finite".into(),
            ));
        }
        Ok(value)
    }

    fn parse_optional_dimension_keyword(&mut self) -> Result<(), DecodeError> {
        self.skip_ws();
        let checkpoint = self.pos;
        let ident = self.parse_ident();
        match ident {
            Ok(value) if value == "Z" => Ok(()),
            Ok(value) if value == "M" || value == "ZM" => Err(DecodeError::InvalidSpatial(
                "M dimensions are not supported in the initial subset".into(),
            )),
            Ok(_) => {
                self.pos = checkpoint;
                Ok(())
            }
            Err(_) => {
                self.pos = checkpoint;
                Ok(())
            }
        }
    }

    fn parse_geometry_shape(&mut self) -> Result<GeometryShape, DecodeError> {
        let kind = self.parse_ident()?;
        self.parse_optional_dimension_keyword()?;
        if self.consume_keyword_ci("EMPTY") {
            return match kind.as_str() {
                "POINT" => Err(DecodeError::InvalidSpatial(
                    "POINT EMPTY is not supported".into(),
                )),
                "LINESTRING" => Ok(GeometryShape::LineString(
                    LineStringValue::new(vec![]).map_err(map_spatial_encode)?,
                )),
                "POLYGON" => Ok(GeometryShape::Polygon(
                    PolygonValue::new(vec![]).map_err(map_spatial_encode)?,
                )),
                "MULTIPOINT" => Ok(GeometryShape::MultiPoint(vec![])),
                "MULTILINESTRING" => Ok(GeometryShape::MultiLineString(vec![])),
                "MULTIPOLYGON" => Ok(GeometryShape::MultiPolygon(vec![])),
                "GEOMETRYCOLLECTION" => Ok(GeometryShape::GeometryCollection(vec![])),
                _ => Err(DecodeError::InvalidSpatial(format!(
                    "unsupported WKT geometry type {kind}"
                ))),
            };
        }

        match kind.as_str() {
            "POINT" => {
                self.expect_char('(')?;
                let position = self.parse_position()?;
                self.expect_char(')')?;
                Ok(GeometryShape::Point(position))
            }
            "LINESTRING" => {
                self.expect_char('(')?;
                let positions = self.parse_position_seq(')')?;
                self.expect_char(')')?;
                Ok(GeometryShape::LineString(
                    LineStringValue::new(positions).map_err(map_spatial_encode)?,
                ))
            }
            "POLYGON" => {
                self.expect_char('(')?;
                let mut rings = Vec::new();
                loop {
                    self.expect_char('(')?;
                    let positions = self.parse_position_seq(')')?;
                    self.expect_char(')')?;
                    rings.push(LinearRingValue::new(positions).map_err(map_spatial_encode)?);
                    if self.consume_char(',') {
                        continue;
                    }
                    break;
                }
                self.expect_char(')')?;
                Ok(GeometryShape::Polygon(
                    PolygonValue::new(rings).map_err(map_spatial_encode)?,
                ))
            }
            "MULTIPOINT" => {
                self.expect_char('(')?;
                let mut points = Vec::new();
                loop {
                    self.skip_ws();
                    let point = if self.consume_char('(') {
                        let position = self.parse_position()?;
                        self.expect_char(')')?;
                        position
                    } else {
                        self.parse_position()?
                    };
                    points.push(point);
                    if self.consume_char(',') {
                        continue;
                    }
                    break;
                }
                self.expect_char(')')?;
                ensure_consistent_dimensions(&points).map_err(map_spatial_encode)?;
                Ok(GeometryShape::MultiPoint(points))
            }
            "MULTILINESTRING" => {
                self.expect_char('(')?;
                let mut lines = Vec::new();
                loop {
                    self.expect_char('(')?;
                    let positions = self.parse_position_seq(')')?;
                    self.expect_char(')')?;
                    lines.push(LineStringValue::new(positions).map_err(map_spatial_encode)?);
                    if self.consume_char(',') {
                        continue;
                    }
                    break;
                }
                self.expect_char(')')?;
                Ok(GeometryShape::MultiLineString(lines))
            }
            "MULTIPOLYGON" => {
                self.expect_char('(')?;
                let mut polys = Vec::new();
                loop {
                    self.expect_char('(')?;
                    let mut rings = Vec::new();
                    loop {
                        self.expect_char('(')?;
                        let positions = self.parse_position_seq(')')?;
                        self.expect_char(')')?;
                        rings.push(LinearRingValue::new(positions).map_err(map_spatial_encode)?);
                        if self.consume_char(',') {
                            continue;
                        }
                        break;
                    }
                    self.expect_char(')')?;
                    polys.push(PolygonValue::new(rings).map_err(map_spatial_encode)?);
                    if self.consume_char(',') {
                        continue;
                    }
                    break;
                }
                self.expect_char(')')?;
                Ok(GeometryShape::MultiPolygon(polys))
            }
            "GEOMETRYCOLLECTION" => {
                self.expect_char('(')?;
                let mut shapes = Vec::new();
                loop {
                    shapes.push(self.parse_geometry_shape()?);
                    if self.consume_char(',') {
                        continue;
                    }
                    break;
                }
                self.expect_char(')')?;
                Ok(GeometryShape::GeometryCollection(shapes))
            }
            _ => Err(DecodeError::InvalidSpatial(format!(
                "unsupported WKT geometry type {kind}"
            ))),
        }
    }

    fn parse_position_seq(&mut self, until: char) -> Result<Vec<Position>, DecodeError> {
        let mut positions = Vec::new();
        loop {
            self.skip_ws();
            if self.peek_char() == Some(until) {
                break;
            }
            positions.push(self.parse_position()?);
            if self.consume_char(',') {
                continue;
            }
            break;
        }
        Ok(positions)
    }

    fn parse_position(&mut self) -> Result<Position, DecodeError> {
        let x = self.parse_number()?;
        let y = self.parse_number()?;
        self.skip_ws();
        if self.peek_is_number_start() {
            let z = self.parse_number()?;
            self.skip_ws();
            if self.peek_is_number_start() {
                return Err(DecodeError::InvalidSpatial(
                    "only XY and XYZ coordinates are supported".into(),
                ));
            }
            Ok(Position::Xyz { x, y, z })
        } else {
            Ok(Position::Xy { x, y })
        }
    }

    fn peek_is_number_start(&self) -> bool {
        matches!(
            self.peek_char(),
            Some('+') | Some('-') | Some('.') | Some('0'..='9')
        )
    }
}

fn ensure_consistent_dimensions(positions: &[Position]) -> Result<(), EncodeError> {
    let Some(first) = positions.first() else {
        return Ok(());
    };
    let dim = first.dimension();
    if positions.iter().any(|pos| pos.dimension() != dim) {
        return Err(EncodeError::InvalidSpatial(
            "all positions must have the same dimension",
        ));
    }
    Ok(())
}

fn encode_geometry_record(
    out: &mut Vec<u8>,
    base_type: u32,
    srid: u32,
    shape: &GeometryShape,
    include_srid: bool,
) -> Result<(), EncodeError> {
    out.push(0x01);
    let mut type_word = base_type;
    if shape.has_z() {
        type_word |= EWKB_Z_FLAG;
    }
    if include_srid {
        type_word |= EWKB_SRID_FLAG;
    }
    out.extend_from_slice(&type_word.to_le_bytes());
    if include_srid {
        out.extend_from_slice(&srid.to_le_bytes());
    }
    encode_shape_body(out, shape, srid)?;
    Ok(())
}

fn encode_shape_body(
    out: &mut Vec<u8>,
    shape: &GeometryShape,
    srid: u32,
) -> Result<(), EncodeError> {
    match shape {
        GeometryShape::Point(position) => encode_position(out, position),
        GeometryShape::LineString(line) => {
            ensure_consistent_dimensions(line.positions())?;
            out.extend_from_slice(&(line.len() as u32).to_le_bytes());
            for position in line.positions() {
                encode_position(out, position);
            }
        }
        GeometryShape::Polygon(poly) => {
            out.extend_from_slice(&(poly.len() as u32).to_le_bytes());
            for ring in poly.ring_slice() {
                ensure_consistent_dimensions(ring.positions())?;
                out.extend_from_slice(&(ring.len() as u32).to_le_bytes());
                for position in ring.positions() {
                    encode_position(out, position);
                }
            }
        }
        GeometryShape::MultiPoint(points) => {
            ensure_consistent_dimensions(points)?;
            out.extend_from_slice(&(points.len() as u32).to_le_bytes());
            for position in points {
                encode_geometry_record(
                    out,
                    EWKB_POINT,
                    srid,
                    &GeometryShape::Point(*position),
                    false,
                )?;
            }
        }
        GeometryShape::MultiLineString(lines) => {
            out.extend_from_slice(&(lines.len() as u32).to_le_bytes());
            for line in lines {
                encode_geometry_record(
                    out,
                    EWKB_LINESTRING,
                    srid,
                    &GeometryShape::LineString(line.clone()),
                    false,
                )?;
            }
        }
        GeometryShape::MultiPolygon(polys) => {
            out.extend_from_slice(&(polys.len() as u32).to_le_bytes());
            for poly in polys {
                encode_geometry_record(
                    out,
                    EWKB_POLYGON,
                    srid,
                    &GeometryShape::Polygon(poly.clone()),
                    false,
                )?;
            }
        }
        GeometryShape::GeometryCollection(shapes) => {
            out.extend_from_slice(&(shapes.len() as u32).to_le_bytes());
            for child in shapes {
                encode_geometry_record(out, child.base_type(), srid, child, false)?;
            }
        }
    }
    Ok(())
}

fn encode_position(out: &mut Vec<u8>, position: &Position) {
    out.extend_from_slice(&position.x().to_le_bytes());
    out.extend_from_slice(&position.y().to_le_bytes());
    if let Some(z) = position.z() {
        out.extend_from_slice(&z.to_le_bytes());
    }
}

fn decode_top_level_shape(
    blob: &[u8],
    expected_tag: u8,
) -> Result<(u32, GeometryShape), DecodeError> {
    if blob.is_empty() {
        return Err(DecodeError::EmptyInput);
    }
    if blob[0] != expected_tag {
        return Err(DecodeError::UnexpectedTag {
            expected: "spatial top-level tag",
            actual: blob[0],
        });
    }
    let mut reader = Reader::new(&blob[1..]);
    let (shape, srid) = decode_geometry_record(&mut reader, true, None)?;
    if !reader.is_empty() {
        return Err(DecodeError::NonCanonical(
            "trailing bytes after spatial value",
        ));
    }
    let srid =
        srid.ok_or_else(|| DecodeError::NonCanonical("top-level spatial value must carry SRID"))?;
    Ok((srid, shape))
}

fn decode_geometry_record(
    reader: &mut Reader<'_>,
    require_srid: bool,
    parent_srid: Option<u32>,
) -> Result<(GeometryShape, Option<u32>), DecodeError> {
    let byte_order = reader.read_u8()?;
    if byte_order != 0x01 {
        return Err(DecodeError::NonCanonical(
            "only little-endian EWKB is canonical",
        ));
    }
    let type_word = reader.read_u32_le()?;
    if type_word & EWKB_M_FLAG != 0 {
        return Err(DecodeError::InvalidSpatial(
            "M dimensions are not supported in the initial subset".into(),
        ));
    }
    let has_z = type_word & EWKB_Z_FLAG != 0;
    let has_srid = type_word & EWKB_SRID_FLAG != 0;
    let base_type = type_word & 0x0000_00ff;
    let srid = if has_srid {
        Some(reader.read_u32_le()?)
    } else {
        None
    };
    if require_srid && srid.is_none() {
        return Err(DecodeError::NonCanonical(
            "top-level spatial value must include SRID",
        ));
    }
    if let (Some(parent), Some(child)) = (parent_srid, srid) {
        if parent != child {
            return Err(DecodeError::InvalidSpatial(
                "nested geometry SRID must match parent".into(),
            ));
        }
    }
    let effective_srid = srid.or(parent_srid);
    let shape = match base_type {
        EWKB_POINT => GeometryShape::Point(decode_position(reader, has_z)?),
        EWKB_LINESTRING => {
            let count = reader.read_u32_le()? as usize;
            let mut positions = Vec::with_capacity(count);
            for _ in 0..count {
                positions.push(decode_position(reader, has_z)?);
            }
            GeometryShape::LineString(LineStringValue::from_positions_unchecked(positions))
        }
        EWKB_POLYGON => {
            let ring_count = reader.read_u32_le()? as usize;
            let mut rings = Vec::with_capacity(ring_count);
            for _ in 0..ring_count {
                let point_count = reader.read_u32_le()? as usize;
                let mut positions = Vec::with_capacity(point_count);
                for _ in 0..point_count {
                    positions.push(decode_position(reader, has_z)?);
                }
                rings.push(LinearRingValue::from_positions_unchecked(positions));
            }
            GeometryShape::Polygon(PolygonValue::from_rings_unchecked(rings))
        }
        EWKB_MULTI_POINT => {
            let count = reader.read_u32_le()? as usize;
            let mut points = Vec::with_capacity(count);
            for _ in 0..count {
                let (child, _) = decode_geometry_record(reader, false, effective_srid)?;
                match child {
                    GeometryShape::Point(point) => points.push(point),
                    _ => {
                        return Err(DecodeError::InvalidSpatial(
                            "multipoint child must be point".into(),
                        ))
                    }
                }
            }
            GeometryShape::MultiPoint(points)
        }
        EWKB_MULTI_LINESTRING => {
            let count = reader.read_u32_le()? as usize;
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                let (child, _) = decode_geometry_record(reader, false, effective_srid)?;
                match child {
                    GeometryShape::LineString(line) => lines.push(line),
                    _ => {
                        return Err(DecodeError::InvalidSpatial(
                            "multilinestring child must be linestring".into(),
                        ))
                    }
                }
            }
            GeometryShape::MultiLineString(lines)
        }
        EWKB_MULTI_POLYGON => {
            let count = reader.read_u32_le()? as usize;
            let mut polys = Vec::with_capacity(count);
            for _ in 0..count {
                let (child, _) = decode_geometry_record(reader, false, effective_srid)?;
                match child {
                    GeometryShape::Polygon(poly) => polys.push(poly),
                    _ => {
                        return Err(DecodeError::InvalidSpatial(
                            "multipolygon child must be polygon".into(),
                        ))
                    }
                }
            }
            GeometryShape::MultiPolygon(polys)
        }
        EWKB_GEOMETRY_COLLECTION => {
            let count = reader.read_u32_le()? as usize;
            let mut shapes = Vec::with_capacity(count);
            for _ in 0..count {
                let (child, _) = decode_geometry_record(reader, false, effective_srid)?;
                shapes.push(child);
            }
            GeometryShape::GeometryCollection(shapes)
        }
        _ => {
            return Err(DecodeError::InvalidSpatial(format!(
                "unsupported EWKB base type {base_type}"
            )))
        }
    };
    Ok((shape, srid))
}

fn decode_position(reader: &mut Reader<'_>, has_z: bool) -> Result<Position, DecodeError> {
    let x = reader.read_f64_le()?;
    let y = reader.read_f64_le()?;
    if has_z {
        let z = reader.read_f64_le()?;
        Ok(Position::Xyz { x, y, z })
    } else {
        Ok(Position::Xy { x, y })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.offset >= self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn read_u32_le(&mut self) -> Result<u32, DecodeError> {
        let bytes = self.read_exact::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_f64_le(&mut self) -> Result<f64, DecodeError> {
        let bytes = self.read_exact::<8>()?;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        if self.offset + N > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::convert::TryInto;

    fn sample_ring() -> LinearRingValue {
        LinearRingValue::new(vec![
            Position::Xy { x: 0.0, y: 0.0 },
            Position::Xy { x: 4.0, y: 0.0 },
            Position::Xy { x: 4.0, y: 4.0 },
            Position::Xy { x: 0.0, y: 0.0 },
        ])
        .unwrap()
    }

    fn sample_polygon() -> PolygonValue {
        PolygonValue::new(vec![sample_ring()]).unwrap()
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

    fn sample_xyz_polygon() -> PolygonValue {
        PolygonValue::new(vec![sample_xyz_ring()]).unwrap()
    }

    fn sample_shapes() -> Vec<GeometryShape> {
        vec![
            GeometryShape::Point(Position::Xy { x: 12.5, y: 55.7 }),
            GeometryShape::LineString(
                LineStringValue::new(vec![
                    Position::Xy { x: 12.5, y: 55.7 },
                    Position::Xy { x: 13.1, y: 56.0 },
                ])
                .unwrap(),
            ),
            GeometryShape::Polygon(sample_polygon()),
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
                    Position::Xy { x: 0.0, y: 0.0 },
                    Position::Xy { x: 1.0, y: 1.0 },
                ])
                .unwrap(),
                LineStringValue::new(vec![
                    Position::Xy { x: 2.0, y: 2.0 },
                    Position::Xy { x: 3.0, y: 3.0 },
                ])
                .unwrap(),
            ]),
            GeometryShape::MultiPolygon(vec![
                sample_polygon(),
                PolygonValue::new(vec![LinearRingValue::new(vec![
                    Position::Xy { x: 10.0, y: 10.0 },
                    Position::Xy { x: 12.0, y: 10.0 },
                    Position::Xy { x: 12.0, y: 12.0 },
                    Position::Xy { x: 10.0, y: 10.0 },
                ])
                .unwrap()])
                .unwrap(),
            ]),
            GeometryShape::GeometryCollection(vec![
                GeometryShape::Point(Position::Xy { x: 12.5, y: 55.7 }),
                GeometryShape::LineString(
                    LineStringValue::new(vec![
                        Position::Xy { x: 12.5, y: 55.7 },
                        Position::Xy { x: 13.1, y: 56.0 },
                    ])
                    .unwrap(),
                ),
            ]),
        ]
    }

    fn sample_xyz_non_point_shapes() -> Vec<GeometryShape> {
        vec![
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
            GeometryShape::Polygon(sample_xyz_polygon()),
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
                sample_xyz_polygon(),
                PolygonValue::new(vec![LinearRingValue::new(vec![
                    Position::Xyz {
                        x: 10.0,
                        y: 10.0,
                        z: 5.0,
                    },
                    Position::Xyz {
                        x: 12.0,
                        y: 10.0,
                        z: 5.0,
                    },
                    Position::Xyz {
                        x: 12.0,
                        y: 12.0,
                        z: 5.0,
                    },
                    Position::Xyz {
                        x: 10.0,
                        y: 10.0,
                        z: 5.0,
                    },
                ])
                .unwrap()])
                .unwrap(),
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

    fn empty_non_point_shapes() -> Vec<(&'static str, GeometryShape)> {
        vec![
            (
                "LINESTRING EMPTY",
                GeometryShape::LineString(LineStringValue::new(vec![]).unwrap()),
            ),
            (
                "POLYGON EMPTY",
                GeometryShape::Polygon(PolygonValue::new(vec![]).unwrap()),
            ),
            ("MULTIPOINT EMPTY", GeometryShape::MultiPoint(vec![])),
            (
                "MULTILINESTRING EMPTY",
                GeometryShape::MultiLineString(vec![]),
            ),
            ("MULTIPOLYGON EMPTY", GeometryShape::MultiPolygon(vec![])),
            (
                "GEOMETRYCOLLECTION EMPTY",
                GeometryShape::GeometryCollection(vec![]),
            ),
        ]
    }

    fn overwrite_u32_le(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    #[test]
    fn point_blob_geojson_and_wkt_roundtrip_xyz() {
        let point = PointValue {
            srid: 4326,
            position: Position::Xyz {
                x: 12.5,
                y: 55.7,
                z: 9.0,
            },
        };
        assert_eq!(
            PointValue::decode_blob(&point.encode_blob().unwrap()).unwrap(),
            point
        );
        assert_eq!(
            PointValue::from_geojson_str(&point.to_geojson_string().unwrap()).unwrap(),
            point
        );
        assert_eq!(
            PointValue::from_wkt_str(&point.to_ewkt_string().unwrap()).unwrap(),
            point
        );
        assert_eq!(
            PointValue::from_wkt_str(&point.to_wkt_string().unwrap()).unwrap(),
            point
        );
    }

    #[test]
    fn geometry_family_roundtrips_through_blob_geojson_and_wkt() {
        for shape in sample_shapes() {
            let geometry = GeometryValue {
                srid: 4326,
                shape: shape.clone(),
            };
            assert_eq!(
                GeometryValue::decode_blob(&geometry.encode_blob().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_geojson_str(&geometry.to_geojson_string().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_wkt_str(&geometry.to_ewkt_string().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_wkt_str(&geometry.to_wkt_string().unwrap()).unwrap(),
                geometry
            );
        }
    }

    #[test]
    fn geography_family_roundtrips_through_blob_geojson_and_wkt() {
        for shape in sample_shapes() {
            let geography = GeographyValue {
                srid: 4326,
                shape: shape.clone(),
            };
            assert_eq!(
                GeographyValue::decode_blob(&geography.encode_blob().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_geojson_str(&geography.to_geojson_string().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_wkt_str(&geography.to_ewkt_string().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_wkt_str(&geography.to_wkt_string().unwrap()).unwrap(),
                geography
            );
        }
    }

    #[test]
    fn geometry_family_roundtrips_xyz_non_point_shapes() {
        for shape in sample_xyz_non_point_shapes() {
            let geometry = GeometryValue {
                srid: 4326,
                shape: shape.clone(),
            };
            assert_eq!(
                GeometryValue::decode_blob(&geometry.encode_blob().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_geojson_str(&geometry.to_geojson_string().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_wkt_str(&geometry.to_ewkt_string().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_wkt_str(&geometry.to_wkt_string().unwrap()).unwrap(),
                geometry
            );
        }
    }

    #[test]
    fn geography_family_roundtrips_xyz_non_point_shapes() {
        for shape in sample_xyz_non_point_shapes() {
            let geography = GeographyValue {
                srid: 4326,
                shape: shape.clone(),
            };
            assert_eq!(
                GeographyValue::decode_blob(&geography.encode_blob().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_geojson_str(&geography.to_geojson_string().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_wkt_str(&geography.to_ewkt_string().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_wkt_str(&geography.to_wkt_string().unwrap()).unwrap(),
                geography
            );
        }
    }

    #[test]
    fn geojson_rejects_feature_wrappers() {
        let value = json!({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [1.0, 2.0]}
        });
        assert!(matches!(
            GeometryValue::from_geojson_value(&value),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn geojson_rejects_crs_objects() {
        let value = json!({
            "type": "Point",
            "coordinates": [1.0, 2.0],
            "crs": {"type": "name", "properties": {"name": "EPSG:4326"}}
        });
        assert!(matches!(
            GeometryValue::from_geojson_value(&value),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn multipoint_wkt_accepts_compact_form() {
        let value = GeometryValue::from_wkt_str("SRID=4326;MULTIPOINT (1 2, 3 4)").unwrap();
        assert_eq!(
            value,
            GeometryValue {
                srid: 4326,
                shape: GeometryShape::MultiPoint(vec![
                    Position::Xy { x: 1.0, y: 2.0 },
                    Position::Xy { x: 3.0, y: 4.0 },
                ]),
            }
        );
    }

    #[test]
    fn geometry_empty_wkt_roundtrips_through_blob_and_wkt() {
        for (wkt, shape) in empty_non_point_shapes() {
            let geometry = GeometryValue::from_wkt_str(wkt).unwrap();
            assert_eq!(
                geometry,
                GeometryValue {
                    srid: 4326,
                    shape: shape.clone(),
                }
            );
            assert_eq!(
                GeometryValue::decode_blob(&geometry.encode_blob().unwrap()).unwrap(),
                geometry
            );
            assert_eq!(
                GeometryValue::from_wkt_str(&geometry.to_wkt_string().unwrap()).unwrap(),
                geometry
            );
        }
    }

    #[test]
    fn geography_empty_wkt_roundtrips_through_blob_and_wkt() {
        for (wkt, shape) in empty_non_point_shapes() {
            let geography = GeographyValue::from_wkt_str(wkt).unwrap();
            assert_eq!(
                geography,
                GeographyValue {
                    srid: 4326,
                    shape: shape.clone(),
                }
            );
            assert_eq!(
                GeographyValue::decode_blob(&geography.encode_blob().unwrap()).unwrap(),
                geography
            );
            assert_eq!(
                GeographyValue::from_wkt_str(&geography.to_wkt_string().unwrap()).unwrap(),
                geography
            );
        }
    }

    #[test]
    fn geography_geojson_defaults_to_4326_without_transforming_coordinates() {
        let geography = GeographyValue::from_geojson_value(&json!({
            "type": "LineString",
            "coordinates": [[12.5, 55.7], [13.1, 56.0]]
        }))
        .unwrap();
        assert_eq!(
            geography,
            GeographyValue {
                srid: 4326,
                shape: GeometryShape::LineString(
                    LineStringValue::new(vec![
                        Position::Xy { x: 12.5, y: 55.7 },
                        Position::Xy { x: 13.1, y: 56.0 },
                    ])
                    .unwrap(),
                ),
            }
        );
    }

    #[test]
    fn point_empty_is_rejected_in_wkt() {
        assert!(matches!(
            PointValue::from_wkt_str("POINT EMPTY"),
            Err(DecodeError::InvalidSpatial(_))
        ));
        assert!(matches!(
            GeometryValue::from_wkt_str("POINT EMPTY"),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn wkt_rejects_m_and_zm_dimensions() {
        assert!(matches!(
            GeometryValue::from_wkt_str("POINT M (1 2 3)"),
            Err(DecodeError::InvalidSpatial(_))
        ));
        assert!(matches!(
            GeometryValue::from_wkt_str("POINT ZM (1 2 3 4)"),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn rejects_big_endian_ewkb_blob() {
        let mut blob = PointValue {
            srid: 4326,
            position: Position::Xy { x: 12.5, y: 55.7 },
        }
        .encode_blob()
        .unwrap();
        blob[1] = 0x00;
        assert!(matches!(
            PointValue::decode_blob(&blob),
            Err(DecodeError::NonCanonical(_))
        ));
    }

    #[test]
    fn rejects_blob_without_top_level_srid() {
        let mut blob = PointValue {
            srid: 4326,
            position: Position::Xy { x: 12.5, y: 55.7 },
        }
        .encode_blob()
        .unwrap();
        let type_word = u32::from_le_bytes(blob[2..6].try_into().unwrap()) & !EWKB_SRID_FLAG;
        overwrite_u32_le(&mut blob, 2, type_word);
        blob.drain(6..10);
        assert!(matches!(
            PointValue::decode_blob(&blob),
            Err(DecodeError::NonCanonical(_))
        ));
    }

    #[test]
    fn rejects_nested_geometry_srid_mismatch() {
        let mut blob = GeometryValue {
            srid: 4326,
            shape: GeometryShape::MultiPoint(vec![Position::Xy { x: 1.0, y: 2.0 }]),
        }
        .encode_blob()
        .unwrap();
        let child_type_word = u32::from_le_bytes(blob[15..19].try_into().unwrap()) | EWKB_SRID_FLAG;
        overwrite_u32_le(&mut blob, 15, child_type_word);
        blob.splice(19..19, 3857u32.to_le_bytes());
        assert!(matches!(
            GeometryValue::decode_blob(&blob),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn rejects_multipoint_child_with_wrong_subtype() {
        let mut blob = GeometryValue {
            srid: 4326,
            shape: GeometryShape::MultiPoint(vec![Position::Xy { x: 0.0, y: 0.0 }]),
        }
        .encode_blob()
        .unwrap();
        overwrite_u32_le(&mut blob, 15, EWKB_LINESTRING);
        assert!(matches!(
            GeometryValue::decode_blob(&blob),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }

    #[test]
    fn rejects_ewkb_m_dimension_flag() {
        let mut blob = PointValue {
            srid: 4326,
            position: Position::Xy { x: 12.5, y: 55.7 },
        }
        .encode_blob()
        .unwrap();
        let type_word = u32::from_le_bytes(blob[2..6].try_into().unwrap()) | EWKB_M_FLAG;
        overwrite_u32_le(&mut blob, 2, type_word);
        assert!(matches!(
            PointValue::decode_blob(&blob),
            Err(DecodeError::InvalidSpatial(_))
        ));
    }
}
