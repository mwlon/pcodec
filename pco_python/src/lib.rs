use numpy::PyArrayDyn;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyObject, PyResult, Python};
use pyo3::types::PyBytes;
use pyo3::{pyclass, PyErr};

use pco::errors::PcoError;

use crate::array_handler::array_to_handler;

mod array_handler;

#[pyclass(get_all)]
pub struct Progress {
  n_processed: usize,
  finished: bool,
}

#[pyclass(get_all, set_all)]
pub struct ChunkConfig {
  compression_level: usize,
  delta_encoding_order: Option<usize>,
  int_mult_spec: String,
  float_mult_spec: String,
  max_page_size: usize,
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

#[pymodule]
fn pcodec(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add("__version__", env!("CARGO_PKG_VERSION"))?;

  #[pyo3(signature = (nums, compression_level=pco::DEFAULT_COMPRESSION_LEVEL))]
  #[pyfn(m)]
  fn auto_compress<'py>(
    py: Python<'py>,
    nums: DynTypedPyArrayDyn<'py>,
    compression_level: usize,
  ) -> PyResult<PyObject> {
    array_to_handler(nums).auto_compress(py, compression_level)
  }

  #[pyfn(m)]
  fn simple_decompress_into(compressed: &PyBytes, dst: DynTypedPyArrayDyn) -> PyResult<Progress> {
    array_to_handler(dst).simple_decompress_into(compressed)
  }

  Ok(())
}
