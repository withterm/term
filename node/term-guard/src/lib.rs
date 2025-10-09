#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub fn hello_term() -> String {
    "Hello from Term Guard! Data validation powered by Rust.".to_string()
}

#[napi]
pub fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

#[napi(object)]
pub struct ValidationInfo {
    pub name: String,
    pub version: String,
    pub rust_version: String,
}

#[napi]
pub fn get_info() -> ValidationInfo {
    ValidationInfo {
        name: "term-guard".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        rust_version: "1.70+".to_string(),
    }
}

#[napi]
pub async fn validate_sample_data() -> Result<String> {
    Ok("Sample validation completed successfully!".to_string())
}
