use crate::{pco_err_to_py, DynTypedPyArrayDyn, Progress};
use numpy::{Element, PyArrayDyn};
use pco::data_types::NumberLike;
use pco::standalone::{simple_compress, simple_decompress_into};

use crate::r#mod::PyWrappedCc;
use crate::wrapped::compressor::PyWrappedCc;
use pco::{wrapped, ChunkConfig};
use pyo3::types::PyBytes;
use pyo3::{PyObject, PyResult, Python};

pub trait ArrayHandler<'py> {
  fn standalone_simple_compress(&self, py: Python<'py>, config: &ChunkConfig)
    -> PyResult<PyObject>;
  fn standalone_simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress>;

  fn wrapped_chunk_compressor(&self, fc: &wrapped::FileCompressor) -> PyResult<PyObject>;
}

impl<'py, T: NumberLike + Element> ArrayHandler<'py> for &'py PyArrayDyn<T> {
  fn standalone_simple_compress(
    &self,
    py: Python<'py>,
    config: &ChunkConfig,
  ) -> PyResult<PyObject> {
    let arr_ro = self.readonly();
    let src = arr_ro.as_slice()?;
    let compressed = simple_compress(src, config).map_err(pco_err_to_py)?;
    // TODO apparently all the places we use PyBytes::new() copy the data.
    // Maybe there's a zero-copy way to do this.
    Ok(PyBytes::new(py, &compressed).into())
  }

  fn standalone_simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress> {
    let mut out_rw = self.readwrite();
    let dst = out_rw.as_slice_mut()?;
    let src = compressed.as_bytes();
    let progress = simple_decompress_into(src, dst).map_err(pco_err_to_py)?;
    Ok(Progress {
      n_processed: progress.n_processed,
      finished: progress.finished,
    })
  }

  fn wrapped_chunk_compressor(&self, fc: &wrapped::FileCompressor) -> PyResult<PyWrappedCc> {
    let arr_ro = self.readonly();
    let src = arr_ro.as_slice()?;
    let cc = fc
      .chunk_compressor(src, &ChunkConfig::default())
      .map_err(pco_err_to_py)?;
    Ok(cc.into())
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
