use crate::arrow2::geoparquet::GeoParquetMetadata;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::infer_schema;
use arrow2::io::parquet::write::FileMetaData;
use geoarrow::array::{GeometryArray, MultiPolygonArray, WKBArray};
use geoarrow::GeometryArrayTrait;
use parquet_wasm::arrow2::error::Result;
use parquet_wasm::arrow2::reader::{read_parquet, read_parquet_metadata};

enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

impl From<&str> for GeometryType {
    fn from(value: &str) -> Self {
        match value {
            "Point" => GeometryType::Point,
            "LineString" => GeometryType::LineString,
            "Polygon" => GeometryType::Polygon,
            "MultiPoint" => GeometryType::MultiPoint,
            "MultiLineString" => GeometryType::MultiLineString,
            "MultiPolygon" => GeometryType::MultiPolygon,
            _ => panic!(),
        }
    }
}

fn parse_wkb_to_geoarrow(
    schema: &Schema,
    chunk: Chunk<Box<dyn Array>>,
    should_return_schema: bool,
    geometry_column_index: usize,
    geometry_type: &GeometryType,
) -> (Option<Schema>, Chunk<Box<dyn Array>>) {
    let mut arrays = chunk.into_arrays();

    let wkb_array: WKBArray<i32> = arrays[geometry_column_index].as_ref().try_into().unwrap();
    let geom_array = match geometry_type {
        GeometryType::Point => GeometryArray::Point(wkb_array.try_into().unwrap()),
        GeometryType::LineString => GeometryArray::LineString(wkb_array.try_into().unwrap()),
        GeometryType::Polygon => GeometryArray::Polygon(wkb_array.try_into().unwrap()),
        GeometryType::MultiPoint => GeometryArray::MultiPoint(wkb_array.try_into().unwrap()),
        GeometryType::MultiLineString => {
            GeometryArray::MultiLineString(wkb_array.try_into().unwrap())
        }
        GeometryType::MultiPolygon => GeometryArray::MultiPolygon(wkb_array.try_into().unwrap()),
    };

    let extension_type = geom_array.extension_type();
    let geom_arr = geom_array.into_boxed_arrow();
    arrays[geometry_column_index] = geom_arr;

    let returned_schema = if should_return_schema {
        let existing_field = &schema.fields[geometry_column_index];
        let mut new_field = existing_field.clone();
        new_field.data_type = extension_type;
        let mut new_schema = schema.clone();
        new_schema.fields[geometry_column_index] = new_field;
        Some(new_schema)
    } else {
        None
    };

    (returned_schema, Chunk::new(arrays))
}

fn parse_geoparquet_metadata(metadata: &FileMetaData) -> GeoParquetMetadata {
    let kv_metadata = metadata.key_value_metadata();

    if let Some(metadata) = kv_metadata {
        for kv in metadata {
            if kv.key == "geo" {
                if let Some(value) = &kv.value {
                    return serde_json::from_str(value).unwrap();
                }
            }
        }
    }

    panic!("expected a 'geo' key in GeoParquet metadata")
}

fn infer_geometry_type(meta: GeoParquetMetadata) -> GeometryType {
    let primary_column = meta.primary_column;
    let column_meta = meta.columns.get(&primary_column).unwrap();
    let geom_types = &column_meta.geometry_types;

    if geom_types.len() == 1 {
        return geom_types[0].as_str().into();
    }

    todo!()
}

pub fn read_geoparquet(parquet_file: &[u8]) -> Result<Vec<u8>> {
    let metadata = read_parquet_metadata(parquet_file)?;
    let mut arrow_schema = infer_schema(&metadata)?;
    let geo_metadata = parse_geoparquet_metadata(&metadata);
    let geometry_column_index = arrow_schema
        .fields
        .iter()
        .position(|field| field.name == geo_metadata.primary_column)
        .unwrap();

    let new_data_type = MultiPolygonArray::<i32>::default().extension_type();
    let existing_field = &arrow_schema.fields[geometry_column_index];
    let mut new_field = existing_field.clone();
    new_field.data_type = new_data_type;
    arrow_schema.fields[geometry_column_index] = new_field;

    let inferred_geometry_type = infer_geometry_type(geo_metadata);

    read_parquet(parquet_file, |schema, chunk, should_return_schema| {
        parse_wkb_to_geoarrow(
            schema,
            chunk,
            should_return_schema,
            geometry_column_index,
            &inferred_geometry_type,
        )
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;

    #[test]
    fn nybb() {
        let buf = fs::read("fixtures/nybb.parquet").unwrap();
        let _output_ipc = read_geoparquet(&buf).unwrap();
    }
}
