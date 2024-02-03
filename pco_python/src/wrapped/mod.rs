pub mod compressor;
pub mod decompressor;

use pco::data_types::NumberLike;
use pco::with_core_dtypes;
use pyo3::buffer::Element;
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pyfunction, pymethods, wrap_pyfunction, PyObject, PyResult, Python};

use pco::wrapped::{ChunkCompressor, FileCompressor, FileDecompressor};

use crate::array_handler::array_to_handler;
use crate::wrapped::compressor::PyWrappedFc;
use crate::wrapped::decompressor::PyWrappedFd;
use crate::{pco_err_to_py, DynTypedPyArrayDyn};

pub fn register(py: Python<'_>, m: &PyModule) -> PyResult<()> {
  compressor::register(py, m)?;
  decompressor::register(py, m)?;

  Ok(())
}
