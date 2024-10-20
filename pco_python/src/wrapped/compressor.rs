use std::convert::TryInto;

use numpy::{Element, PyArray1, PyArrayMethods, PyUntypedArray, PyUntypedArrayMethods};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pymethods, Bound, PyResult, Python};

use pco::data_types::{CoreDataType, Latent, NumberLike};
use pco::wrapped::{ChunkCompressor, FileCompressor};
use pco::{match_latent_enum, match_number_like_enum, ChunkConfig};

use crate::utils::pco_err_to_py;
use crate::{utils, PyChunkConfig};

/// The top-level object for creating wrapped pcodec files.
#[pyclass(name = "FileCompressor")]
struct PyFc {
  inner: FileCompressor,
}

pco::define_latent_enum!(
  #[derive()]
  DynCc(ChunkCompressor)
);

// can't pass inner directly since pyo3 only supports unit variant enums
/// Holds metadata about a chunk and supports compressing one page at a time.
#[pyclass(name = "ChunkCompressor")]
struct PyCc(DynCc);

impl PyFc {
  fn chunk_compressor_generic<T: NumberLike + Element>(
    &self,
    py: Python,
    arr: &Bound<PyArray1<T>>,
    config: &ChunkConfig,
  ) -> PyResult<ChunkCompressor<T::L>> {
    let arr_ro = arr.readonly();
    let src = arr_ro.as_slice()?;
    py.allow_threads(|| self.inner.chunk_compressor(src, config))
      .map_err(pco_err_to_py)
  }
}

#[pymethods]
impl PyFc {
  /// :returns: a new FileCompressor.
  #[new]
  pub fn new() -> PyFc {
    PyFc {
      inner: FileCompressor::default(),
    }
  }

  /// :returns: a bytes object containing the encoded header
  ///
  /// :raises: TypeError, RuntimeError
  fn write_header<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
    let mut res = Vec::new();
    self.inner.write_header(&mut res).map_err(pco_err_to_py)?;
    Ok(PyBytes::new_bound(py, &res))
  }

  /// Create a chunk compressor, computing the chunk metadata necessary to
  /// compress the provided nums.
  ///
  /// This does the bulk of the work of compression.
  ///
  /// :param nums: numpy array to compress. This may have any shape.
  /// However, it must be contiguous, and only the following data types are
  /// supported: float16, float32, float64, int16, int32, int64, uint16, uint32, uint64.
  /// :param config: a ChunkConfig object containing compression level and
  /// other settings.
  ///
  /// :returns: a ChunkCompressor
  ///
  /// :raises: TypeError, RuntimeError
  fn chunk_compressor(
    &self,
    py: Python,
    nums: Bound<PyUntypedArray>,
    config: &PyChunkConfig,
  ) -> PyResult<PyCc> {
    let config = config.try_into()?;
    let dtype = utils::core_dtype_from_numpy(py, &nums.dtype())?;
    match_number_like_enum!(
      dtype,
      CoreDataType<T> => {
        let cc = self.chunk_compressor_generic::<T>(py, nums.downcast::<PyArray1<T>>()?, &config)?;
        Ok(PyCc(DynCc::new(cc).unwrap()))
      }
    )
  }
}

#[pymethods]
impl PyCc {
  /// :returns: a bytes object containing the encoded chunk metadata.
  ///
  /// :raises: TypeError, RuntimeError
  fn write_chunk_meta<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
    match_latent_enum!(
      &self.0,
      DynCc<T>(cc) => {
        let mut res = Vec::new();
        cc.write_chunk_meta(&mut res).map_err(pco_err_to_py)?;
        Ok(PyBytes::new_bound(py, &res))
      }
    )
  }

  /// :returns: a list containing the count of numbers in each page.
  fn n_per_page(&self) -> Vec<usize> {
    match_latent_enum!(
      &self.0,
      DynCc<T>(cc) => { cc.n_per_page() }
    )
  }

  /// :param page_idx: an int for which page you want to write.
  ///
  /// :returns: a bytes object containing the encoded page.
  ///
  /// :raises: TypeError, RuntimeError
  fn write_page<'py>(&self, py: Python<'py>, page_idx: usize) -> PyResult<Bound<'py, PyBytes>> {
    match_latent_enum!(
      &self.0,
      DynCc<T>(cc) => {
        let mut res = Vec::new();
        py.allow_threads(|| cc.write_page(page_idx, &mut res))
          .map_err(pco_err_to_py)?;
        Ok(PyBytes::new_bound(py, &res))
      }
    )
  }
}

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
  m.add_class::<PyFc>()?;
  m.add_class::<PyCc>()?;

  Ok(())
}
