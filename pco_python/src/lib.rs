use numpy::{Element, PyArrayDyn};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyObject, PyResult, Python};
use pyo3::pyclass;
use pyo3::types::PyBytes;

use pco::data_types::NumberLike;
use pco::standalone::{auto_compress, simple_decompress_into};

#[pyclass(get_all)]
struct Progress {
  n_processed: usize,
  finished: bool,
}

#[pyclass(get_all, set_all)]
struct ChunkConfig {
  compression_level: usize,
  delta_encoding_order: Option<usize>,
  int_mult_spec: String,
  float_mult_spec: String,
  max_page_size: usize,
}

trait ArrayHandler<'py> {
  fn auto_compress(&self, py: Python<'py>, compression_level: usize) -> PyResult<PyObject>;
  fn simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress>;
}

impl<'py, T: NumberLike + Element> ArrayHandler<'py> for &'py PyArrayDyn<T> {
  fn auto_compress(&self, py: Python<'py>, compression_level: usize) -> PyResult<PyObject> {
    let arr_ro = self.readonly();
    let src = arr_ro.as_slice()?;
    let compressed = auto_compress(src, compression_level);
    Ok(PyBytes::new(py, &compressed).into())
  }

  fn simple_decompress_into(&self, compressed: &PyBytes) -> PyResult<Progress> {
    let mut out_rw = self.readwrite();
    let dst = out_rw.as_slice_mut()?;
    let src = compressed.as_bytes();
    let progress = simple_decompress_into(src, dst)
      .map_err(|e| PyRuntimeError::new_err(format!("pco decompression error: {}", e)))?;
    Ok(Progress {
      n_processed: progress.n_processed,
      finished: progress.finished,
    })
  }
}

// The Numpy crate recommends using this type of enum to write functions that accept different Numpy dtypes
// https://github.com/PyO3/rust-numpy/blob/32740b33ec55ef0b7ebec726288665837722841d/examples/simple/src/lib.rs#L113
// The first dyn refers to dynamic dtype; the second to dynamic shape
#[derive(FromPyObject)]
enum DynTypedPyArrayDyn<'py> {
  F32(&'py PyArrayDyn<f32>),
  F64(&'py PyArrayDyn<f64>),
  I32(&'py PyArrayDyn<i32>),
  I64(&'py PyArrayDyn<i64>),
  U32(&'py PyArrayDyn<u32>),
  U64(&'py PyArrayDyn<u64>),
}

fn array_to_handler<'py>(arr: DynTypedPyArrayDyn<'py>) -> Box<dyn ArrayHandler<'py> + 'py> {
  match arr {
    DynTypedPyArrayDyn::F32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::F64(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::I32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::I64(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::U32(py_arr) => Box::new(py_arr),
    DynTypedPyArrayDyn::U64(py_arr) => Box::new(py_arr),
  }
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
