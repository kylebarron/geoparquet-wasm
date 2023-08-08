use parquet_wasm::arrow2::error::WasmResult;
use parquet_wasm::utils::assert_parquet_file_not_empty;
use wasm_bindgen::prelude::*;

/// Read a Parquet file into Arrow data using the [`arrow2`](https://crates.io/crates/arrow2) and
/// [`parquet2`](https://crates.io/crates/parquet2) Rust crates.
///
/// Example:
///
/// ```js
/// import { tableFromIPC } from "apache-arrow";
/// // Edit the `parquet-wasm` import as necessary
/// import { readParquet } from "parquet-wasm/node2";
///
/// const resp = await fetch("https://example.com/file.parquet");
/// const parquetUint8Array = new Uint8Array(await resp.arrayBuffer());
/// const arrowUint8Array = readParquet(parquetUint8Array);
/// const arrowTable = tableFromIPC(arrowUint8Array);
/// ```
///
/// @param parquet_file Uint8Array containing Parquet data
/// @returns Uint8Array containing Arrow data in [IPC Stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format). To parse this into an Arrow table, pass to `tableFromIPC` in the Arrow JS bindings.
#[wasm_bindgen(js_name = readGeoParquet)]
pub fn read_geoparquet(parquet_file: &[u8]) -> WasmResult<Vec<u8>> {
    assert_parquet_file_not_empty(parquet_file)?;
    Ok(crate::arrow2::reader::read_geoparquet(parquet_file)?)
}
