use numpy::{Element, IntoPyArray, PyArray1, PyArrayDyn};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyObject, PyResult, Python};
use pyo3::pyclass;
use pyo3::types::PyBytes;

use pco::data_types::NumberLike;
use pco::standalone::{auto_compress, auto_decompress, simple_decompress_into};
use pco::DEFAULT_COMPRESSION_LEVEL;

#[pyclass]
struct PyProgress {
  #[pyo3(get)]
  n_processed: usize,
  #[pyo3(get)]
  finished: bool,
}

// The Numpy crate recommends using this type of enum to write functions that accept different Numpy dtypes
// https://github.com/PyO3/rust-numpy/blob/32740b33ec55ef0b7ebec726288665837722841d/examples/simple/src/lib.rs#L113
#[derive(FromPyObject)]
enum ArrayDynFloat<'py> {
  F32(&'py PyArrayDyn<f32>),
  F64(&'py PyArrayDyn<f64>),
  I32(&'py PyArrayDyn<i32>),
  I64(&'py PyArrayDyn<i64>),
  U32(&'py PyArrayDyn<u32>),
  U64(&'py PyArrayDyn<u64>),
}

fn compress_typed<T: NumberLike + Element>(py: Python, arr: &PyArrayDyn<T>) -> PyResult<PyObject> {
  let arr_ro = arr.readonly();
  let src = arr_ro.as_slice()?;
  let compressed = auto_compress(src, DEFAULT_COMPRESSION_LEVEL);
  Ok(PyBytes::new(py, &compressed).into())
}

fn decompress_typed<T: NumberLike + Element>(
  compressed: &PyBytes,
  out: &PyArrayDyn<T>,
) -> PyResult<PyProgress> {
  let mut out_rw = out.readwrite();
  let dst = out_rw.as_slice_mut()?;
  let src = compressed.as_bytes();
  let progress = simple_decompress_into(src, dst)
    .map_err(|e| PyRuntimeError::new_err(format!("pco decompression error: {}", e)))?;
  Ok(PyProgress {
    n_processed: progress.n_processed,
    finished: progress.finished,
  })
}

#[pymodule]
fn pcodec(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add("__version__", env!("CARGO_PKG_VERSION"))?;

  #[pyfn(m)]
  fn auto_compress<'py>(py: Python<'py>, x: ArrayDynFloat<'py>) -> PyResult<PyObject> {
    match x {
      ArrayDynFloat::F32(py_arr) => compress_typed(py, py_arr),
      ArrayDynFloat::F64(py_arr) => compress_typed(py, py_arr),
      ArrayDynFloat::I32(py_arr) => compress_typed(py, py_arr),
      ArrayDynFloat::I64(py_arr) => compress_typed(py, py_arr),
      ArrayDynFloat::U32(py_arr) => compress_typed(py, py_arr),
      ArrayDynFloat::U64(py_arr) => compress_typed(py, py_arr),
    }
  }

  #[pyfn(m)]
  fn simple_decompress_into(compressed: &PyBytes, out: ArrayDynFloat) -> PyResult<PyProgress> {
    match out {
      ArrayDynFloat::F32(out) => decompress_typed(compressed, out),
      ArrayDynFloat::F64(out) => decompress_typed(compressed, out),
      ArrayDynFloat::I32(out) => decompress_typed(compressed, out),
      ArrayDynFloat::I64(out) => decompress_typed(compressed, out),
      ArrayDynFloat::U32(out) => decompress_typed(compressed, out),
      ArrayDynFloat::U64(out) => decompress_typed(compressed, out),
    }
  }

  #[pyfn(m)]
  fn auto_decompress_f32<'py>(
    py: Python<'py>,
    compressed: &PyBytes,
  ) -> PyResult<&'py PyArray1<f32>> {
    let src = compressed.as_bytes();
    let decompressed = auto_decompress::<f32>(src)
      .map_err(|e| PyRuntimeError::new_err(format!("pco decompression error: {}", e)))?;
    let py_array = decompressed.into_pyarray(py);
    Ok(py_array)
  }

  Ok(())
}
