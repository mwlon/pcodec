use std::convert::TryInto;

use numpy::{Element, PyArrayDyn};
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pymethods, PyObject, PyResult, Python};

use pco::data_types::{Latent, NumberLike};
use pco::wrapped::{ChunkCompressor, FileCompressor};
use pco::{with_core_dtypes, with_core_latents, ChunkConfig};

use crate::{pco_err_to_py, DynTypedPyArrayDyn, PyChunkConfig};

/// The top-level object for creating wrapped pcodec files.
#[pyclass(name = "FileCompressor")]
struct PyFc {
  inner: FileCompressor,
}

enum DynCc {
  U16(ChunkCompressor<u16>),
  U32(ChunkCompressor<u32>),
  U64(ChunkCompressor<u64>),
}

// can't pass inner directly since pyo3 only supports unit variant enums
/// Holds metadata about a chunk and supports compressing one page at a time.
#[pyclass(name = "ChunkCompressor")]
struct PyCc(DynCc);

impl PyFc {
  fn chunk_compressor_generic<T: NumberLike + Element>(
    &self,
    arr: &PyArrayDyn<T>,
    config: &ChunkConfig,
  ) -> PyResult<ChunkCompressor<T::L>> {
    let arr_ro = arr.readonly();
    let src = arr_ro.as_slice()?;
    self
      .inner
      .chunk_compressor(src, config)
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
  fn write_header(&self, py: Python) -> PyResult<PyObject> {
    let mut res = Vec::new();
    self.inner.write_header(&mut res).map_err(pco_err_to_py)?;
    Ok(PyBytes::new(py, &res).into())
  }

  /// Create a chunk compressor, computing the chunk metadata necessary to
  /// compress the provided nums.
  ///
  /// This does the bulk of the work of compression.
  ///
  /// :param nums: numpy array to compress. This may have any shape.
  /// However, it must be contiguous, and only the following data types are
  /// supported: float32, float64, int32, int64, uint32, uint64.
  /// :param config: a ChunkConfig object containing compression level and
  /// other settings.
  ///
  /// :returns: a ChunkCompressor
  ///
  /// :raises: TypeError, RuntimeError
  fn chunk_compressor(&self, nums: DynTypedPyArrayDyn, config: &PyChunkConfig) -> PyResult<PyCc> {
    let config = config.try_into()?;
    macro_rules! match_nums {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        match nums {
          $(DynTypedPyArrayDyn::$name(arr) => DynCc::$lname(self.chunk_compressor_generic::<$t>(arr, &config)?),)+
        }
      }
    }
    let dyn_cc = with_core_dtypes!(match_nums);
    Ok(PyCc(dyn_cc))
  }
}

fn chunk_meta_py<U: Latent>(py: Python, cc: &ChunkCompressor<U>) -> PyResult<PyObject> {
  let mut res = Vec::new();
  cc.write_chunk_meta(&mut res).map_err(pco_err_to_py)?;
  Ok(PyBytes::new(py, &res).into())
}

fn page_py<U: Latent>(py: Python, cc: &ChunkCompressor<U>, page_idx: usize) -> PyResult<PyObject> {
  let mut res = Vec::new();
  cc.write_page(page_idx, &mut res).map_err(pco_err_to_py)?;
  Ok(PyBytes::new(py, &res).into())
}

#[pymethods]
impl PyCc {
  /// :returns: a bytes object containing the encoded chunk metadata.
  ///
  /// :raises: TypeError, RuntimeError
  fn write_chunk_meta(&self, py: Python) -> PyResult<PyObject> {
    let dyn_cc = &self.0;
    macro_rules! match_cc {
      {$($name:ident => $t:ty,)+} => {
        match dyn_cc {
          $(DynCc::$name(cc) => chunk_meta_py(py, cc),)+
        }
      }
    }
    with_core_latents!(match_cc)
  }

  /// :returns: a list containing the count of numbers in each page.
  fn n_per_page(&self) -> Vec<usize> {
    let dyn_cc = &self.0;
    macro_rules! match_cc {
      {$($name:ident => $t:ty,)+} => {
        match dyn_cc {
          $(DynCc::$name(cc) => cc.n_per_page(),)+
        }
      }
    }
    with_core_latents!(match_cc)
  }

  /// :param page_idx: an int for which page you want to write.
  ///
  /// :returns: a bytes object containing the encoded page.
  ///
  /// :raises: TypeError, RuntimeError
  fn write_page(&self, py: Python, page_idx: usize) -> PyResult<PyObject> {
    let dyn_cc = &self.0;
    macro_rules! match_cc {
      {$($name:ident => $t:ty,)+} => {
        match dyn_cc {
          $(DynCc::$name(cc) => page_py(py, cc, page_idx),)+
        }
      }
    }
    with_core_latents!(match_cc)
  }
}

pub fn register(_py: Python, m: &PyModule) -> PyResult<()> {
  m.add_class::<PyFc>()?;
  m.add_class::<PyCc>()?;

  Ok(())
}
