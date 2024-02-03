use numpy::PyArrayDyn;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyResult, Python};
use pyo3::{pyclass, PyErr};

use pco::errors::PcoError;

pub mod standalone;
pub mod wrapped;

#[pyclass(get_all)]
pub struct Progress {
  /// count of decompressed numbers.
  n_processed: usize,
  /// whether the compressed data was finished.
  finished: bool,
}

pub fn pco_err_to_py(pco: PcoError) -> PyErr {
  PyRuntimeError::new_err(format!("pco error: {}", pco))
}

// The Numpy crate recommends using this type of enum to write functions that accept different Numpy dtypes
// https://github.com/PyO3/rust-numpy/blob/32740b33ec55ef0b7ebec726288665837722841d/examples/simple/src/lib.rs#L113
// The first dyn refers to dynamic dtype; the second to dynamic shape
#[derive(FromPyObject)]
pub enum DynTypedPyArrayDyn<'py> {
  F32(&'py PyArrayDyn<f32>),
  F64(&'py PyArrayDyn<f64>),
  I32(&'py PyArrayDyn<i32>),
  I64(&'py PyArrayDyn<i64>),
  U32(&'py PyArrayDyn<u32>),
  U64(&'py PyArrayDyn<u64>),
}

/// Pcodec is a codec for numerical sequences.
#[pymodule]
fn pcodec(py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add("__version__", env!("CARGO_PKG_VERSION"))?;
  m.add_class::<Progress>()?;
  m.add(
    "DEFAULT_COMPRESSION_LEVEL",
    pco::DEFAULT_COMPRESSION_LEVEL,
  )?;

  // =========== STANDALONE ===========
  let standalone_module = PyModule::new(py, "standalone")?;
  standalone::register(py, standalone_module)?;
  m.add_submodule(standalone_module)?;

  // =========== WRAPPED ===========
  let wrapped_module = PyModule::new(py, "wrapped")?;
  wrapped::register(py, wrapped_module)?;
  m.add_submodule(wrapped_module)?;

  Ok(())
}
