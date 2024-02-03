use pco::data_types::NumberLike;
use pco::with_core_dtypes;
use pyo3::buffer::Element;
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pyfunction, pymethods, wrap_pyfunction, PyObject, PyResult, Python};

use pco::wrapped::{ChunkCompressor, FileCompressor, FileDecompressor};

use crate::array_handler::array_to_handler;
use crate::{pco_err_to_py, DynTypedPyArrayDyn};

#[pyclass]
pub struct PyWrappedFc {
  inner: FileCompressor,
}

macro_rules! impl_cc {
  {$($names:ident => $types:ty,)+} => {
    #[pyclass]
    pub enum PyWrappedCc {
      $($names(ChunkCompressor<$types>),)+
    }
  }
}
with_core_dtypes!(impl_cc);

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
pub struct PyWrappedFd {
  #[pyo3(get)]
  n_bytes_read: usize,
  inner: FileDecompressor,
}

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  #[pyfunction]
  pub fn file_compressor() -> PyWrappedFc {
    let py_fc = PyWrappedFc {
      inner: FileCompressor::default(),
    };
    py_fc.into()
  }
  m.add_function(wrap_pyfunction!(file_compressor, m)?)?;

  #[pyfunction]
  pub fn file_decompressor(compressed: &PyBytes) -> PyResult<PyWrappedFd> {
    let compressed = compressed.as_bytes();
    let (fd, rest) = FileDecompressor::new(compressed).map_err(pco_err_to_py)?;
    let py_fd = PyWrappedFd {
      inner: fd,
      n_bytes_read: compressed.len() - rest.len(),
    };
    Ok(py_fd.into())
  }
  m.add_function(wrap_pyfunction!(file_decompressor, m)?)?;

  Ok(())
}
