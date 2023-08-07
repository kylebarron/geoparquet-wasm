mod utils;

use wasm_bindgen::prelude::*;

// #[cfg(feature = "arrow2")]
pub mod arrow2;

#[wasm_bindgen]
extern "C" {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, js!");
}
