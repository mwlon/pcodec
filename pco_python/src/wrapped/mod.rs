use pyo3::types::PyModule;
use pyo3::{Bound, PyResult, Python};

pub mod compressor;
pub mod decompressor;

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
  compressor::register(m)?;
  decompressor::register(m)?;

  Ok(())
}
