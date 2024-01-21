use crate::{pco_err_to_py, DynTypedPyArrayDyn, Progress};
use numpy::{Element, PyArrayDyn};
use pco::data_types::NumberLike;
use pco::standalone::{simple_compress, simple_decompress_into};

use pco::ChunkConfig;
use pyo3::types::PyBytes;
use pyo3::{PyObject, PyResult, Python};

pub trait ArrayHandler<'py> {
  fn simple_compress(&self, py: Python<'py>, config: &ChunkConfig) -> PyResult<PyObject>;
  fn simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress>;
}

impl<'py, T: NumberLike + Element> ArrayHandler<'py> for &'py PyArrayDyn<T> {
  fn simple_compress(&self, py: Python<'py>, config: &ChunkConfig) -> PyResult<PyObject> {
    let arr_ro = self.readonly();
    let src = arr_ro.as_slice()?;
    let compressed = simple_compress(src, config).map_err(pco_err_to_py)?;
    Ok(PyBytes::new(py, &compressed).into())
  }

  fn simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress> {
    let mut out_rw = self.readwrite();
    let dst = out_rw.as_slice_mut()?;
    let src = compressed.as_bytes();
    let progress = simple_decompress_into(src, dst).map_err(pco_err_to_py)?;
    Ok(Progress {
      n_processed: progress.n_processed,
      finished: progress.finished,
    })
  }
}

pub fn array_to_handler<'py>(arr: DynTypedPyArrayDyn<'py>) -> Box<dyn ArrayHandler<'py> + 'py> {
  match arr {
    DynTypedPyArrayDyn::F32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::F64(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::I32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::I64(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::U32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::U64(py_arr) => Box::new(py_arr),
  }
}
