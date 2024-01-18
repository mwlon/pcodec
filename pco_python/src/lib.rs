use numpy::PyArrayDyn;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyObject, PyResult, Python};
use pyo3::types::PyBytes;
use pyo3::{pyclass, PyErr};

use pco::errors::PcoError;
use pco::{ChunkConfig, FloatMultSpec, IntMultSpec, PagingSpec};

use crate::array_handler::array_to_handler;

mod array_handler;

#[pyclass(get_all)]
pub struct Progress {
  n_processed: usize,
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

#[pymodule]
fn pcodec(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add("__version__", env!("CARGO_PKG_VERSION"))?;

  #[pyo3(signature = (
    nums,
    compression_level=pco::DEFAULT_COMPRESSION_LEVEL,
    delta_encoding_order=None,
    int_mult_spec="enabled",
    float_mult_spec="disabled",
    max_page_size=262144
  ))]
  #[pyfn(m)]
  fn auto_compress<'py>(
    py: Python<'py>,
    nums: DynTypedPyArrayDyn<'py>,
    compression_level: usize,
    delta_encoding_order: Option<usize>,
    int_mult_spec: &str,
    float_mult_spec: &str,
    max_page_size: usize,
  ) -> PyResult<PyObject> {
    let int_mult_spec = match int_mult_spec.to_lowercase().as_str() {
      "enabled" => IntMultSpec::Enabled,
      "disabled" => IntMultSpec::Disabled,
      other => {
        return Err(PyRuntimeError::new_err(format!(
          "unknown int mult spec: {}",
          other
        )))
      }
    };
    let float_mult_spec = match float_mult_spec.to_lowercase().as_str() {
      "enabled" => FloatMultSpec::Enabled,
      "disabled" => FloatMultSpec::Disabled,
      other => {
        return Err(PyRuntimeError::new_err(format!(
          "unknown float mult spec: {}",
          other
        )))
      }
    };
    let config = ChunkConfig::default()
      .with_compression_level(compression_level)
      .with_delta_encoding_order(delta_encoding_order)
      .with_int_mult_spec(int_mult_spec)
      .with_float_mult_spec(float_mult_spec)
      .with_paging_spec(PagingSpec::EqualPagesUpTo(max_page_size));

    array_to_handler(nums).simple_compress(py, &config)
  }

  #[pyfn(m)]
  fn simple_decompress_into(compressed: &PyBytes, dst: DynTypedPyArrayDyn) -> PyResult<Progress> {
    array_to_handler(dst).simple_decompress_into(compressed)
  }

  Ok(())
}
