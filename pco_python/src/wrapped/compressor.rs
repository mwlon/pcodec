use crate::array_handler::array_to_handler;
use crate::{pco_err_to_py, DynTypedPyArrayDyn};
use pco::wrapped::{ChunkCompressor, FileCompressor};
use pyo3::types::{PyBytes, PyModule};
use pyo3::{wrap_pyfunction, PyObject, PyResult, Python};
use pco::data_types::UnsignedLike;

#[pyclass]
pub struct PyWrappedFc {
  inner: FileCompressor,
}

#[pymethods]
impl PyWrappedFc {
  fn header(&self, py: Python) -> PyResult<PyObject> {
    let mut res = Vec::new();
    self.inner.write_header(&mut res).map_err(pco_err_to_py)?;
    Ok(PyBytes::new(py, &res).into())
  }

  fn chunk_compressor<'py>(
    &self,
    py: Python<'py>,
    nums: DynTypedPyArrayDyn<'py>,
    // config: PyChunkConfig,
  ) -> PyResult<PyWrappedCc> {
    array_to_handler(nums).wrapped_chunk_compressor(py, &self.inner)
  }
}

#[pyclass]
pub enum PyWrappedCc {
  U32(ChunkCompressor<u32>),
  U64(ChunkCompressor<u64>),
}

impl<U: UnsignedLike> From<ChunkCompressor<U>>

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  #[pyfunction]
  pub fn file_compressor() -> PyWrappedFc {
    let py_fc = PyWrappedFc {
      inner: FileCompressor::default(),
    };
    py_fc.into()
  }
  m.add_function(wrap_pyfunction!(file_compressor, m)?)?;

  Ok(())
}
