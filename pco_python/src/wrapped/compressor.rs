use std::convert::TryInto;

use numpy::{Element, PyArrayDyn};
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pyfunction, pymethods, wrap_pyfunction, PyObject, PyResult, Python};

use pco::data_types::{NumberLike, UnsignedLike};
use pco::wrapped::{ChunkCompressor, FileCompressor};
use pco::{with_core_dtypes, with_core_unsigneds, ChunkConfig};

use crate::{pco_err_to_py, DynTypedPyArrayDyn, PyChunkConfig};

#[pyclass(name = "FileCompressor")]
struct PyFc {
  inner: FileCompressor,
}

enum DynCc {
  U32(ChunkCompressor<u32>),
  U64(ChunkCompressor<u64>),
}

// can't pass inner directly since pyo3 only supports unit variant enums
#[pyclass(name = "ChunkCompressor")]
struct PyCc(DynCc);

impl PyFc {
  fn chunk_compressor_generic<T: NumberLike + Element>(
    &self,
    arr: &PyArrayDyn<T>,
    config: &ChunkConfig,
  ) -> PyResult<ChunkCompressor<T::Unsigned>> {
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
  #[new]
  pub fn new() -> PyFc {
    PyFc {
      inner: FileCompressor::default(),
    }
  }

  fn write_header(&self, py: Python) -> PyResult<PyObject> {
    let mut res = Vec::new();
    self.inner.write_header(&mut res).map_err(pco_err_to_py)?;
    Ok(PyBytes::new(py, &res).into())
  }

  fn chunk_compressor(&self, nums: DynTypedPyArrayDyn, config: &PyChunkConfig) -> PyResult<PyCc> {
    let config = config.try_into()?;
    macro_rules! match_nums {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
        match nums {
          $(DynTypedPyArrayDyn::$name(arr) => DynCc::$uname(self.chunk_compressor_generic::<$t>(arr, &config)?),)+
        }
      }
    }
    let dyn_cc = with_core_dtypes!(match_nums);
    Ok(PyCc(dyn_cc))
  }
}

pub fn chunk_meta_py<U: UnsignedLike>(py: Python, cc: &ChunkCompressor<U>) -> PyResult<PyObject> {
  let mut res = Vec::new();
  cc.write_chunk_meta(&mut res).map_err(pco_err_to_py)?;
  Ok(PyBytes::new(py, &res).into())
}

pub fn page_py<U: UnsignedLike>(
  py: Python,
  cc: &ChunkCompressor<U>,
  page_idx: usize,
) -> PyResult<PyObject> {
  let mut res = Vec::new();
  cc.write_page(page_idx, &mut res).map_err(pco_err_to_py)?;
  Ok(PyBytes::new(py, &res).into())
}

#[pymethods]
impl PyCc {
  fn write_chunk_meta(&self, py: Python) -> PyResult<PyObject> {
    let dyn_cc = &self.0;
    macro_rules! match_cc {
      {$($name:ident => $t:ty,)+} => {
        match dyn_cc {
          $(DynCc::$name(cc) => chunk_meta_py(py, cc),)+
        }
      }
    }
    with_core_unsigneds!(match_cc)
  }

  fn write_page(&self, py: Python, page_idx: usize) -> PyResult<PyObject> {
    let dyn_cc = &self.0;
    macro_rules! match_cc {
      {$($name:ident => $t:ty,)+} => {
        match dyn_cc {
          $(DynCc::$name(cc) => page_py(py, cc, page_idx),)+
        }
      }
    }
    with_core_unsigneds!(match_cc)
  }
}

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add_class::<PyFc>()?;
  m.add_class::<PyCc>()?;

  Ok(())
}
