use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    pub version: String,
    pub primary_column: String,
    pub columns: HashMap<String, GeoParquetColumnMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct GeoParquetColumnMetadata {
    pub encoding: String,
    pub geometry_types: Vec<String>,
    pub crs: Option<Value>,
    pub orientation: String,
    pub edges: String,
    pub bbox: Vec<f64>,
    pub epoch: i32,
}
