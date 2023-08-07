use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::io::parquet::read::infer_schema;
use arrow2::io::parquet::write::FileMetaData;
use geoarrow::GeometryArrayTrait;
use parquet_wasm::arrow2::error::Result;
use parquet_wasm::arrow2::reader::{read_parquet, read_parquet_metadata};
use geoarrow::array::{WKBArray, GeometryArray};
use crate::arrow2::geoparquet::GeoParquetMetadata;

fn parse_wkb_to_geoarrow(
    chunk: Chunk<Box<dyn Array>>,
    geometry_column_index: usize,
) -> Chunk<Box<dyn Array>> {
    let mut arrays = chunk.into_arrays();

    let wkb_array: WKBArray<i32> = arrays[geometry_column_index].as_ref().try_into().unwrap();
    let geom_array: GeometryArray<i32> = wkb_array.try_into().unwrap();

    arrays[geometry_column_index] = geom_array.into_boxed_arrow();
    Chunk::new(arrays)
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

pub fn read_geoparquet(parquet_file: &[u8]) -> Result<Vec<u8>> {
    let metadata = read_parquet_metadata(parquet_file)?;
    let arrow_schema = infer_schema(&metadata)?;
    let geo_metadata = parse_geoparquet_metadata(&metadata);
    let geometry_column_index = arrow_schema
        .fields
        .iter()
        .position(|field| field.name == geo_metadata.primary_column)
        .unwrap();

    read_parquet(parquet_file, |chunk| {
        parse_wkb_to_geoarrow(chunk, geometry_column_index)
    })
}
