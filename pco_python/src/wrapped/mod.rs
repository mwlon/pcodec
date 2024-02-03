use pyo3::types::PyModule;
use pyo3::{PyResult, Python};

pub mod compressor;
pub mod decompressor;

pub fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
  compressor::register(py, m)?;
  decompressor::register(py, m)?;

  Ok(())
}
