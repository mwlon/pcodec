use std::convert::TryInto;

use numpy::{
  Element, IntoPyArray, PyArray1, PyArrayDescrMethods, PyArrayMethods, PyUntypedArray,
  PyUntypedArrayMethods,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule, PyNone};
use pyo3::{pyfunction, wrap_pyfunction, Bound, PyObject, PyResult, Python};

use pco::data_types::NumberLike;
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::{standalone, with_core_dtypes, ChunkConfig};

use crate::{pco_err_to_py, PyChunkConfig, PyProgress};

fn decompress_chunks<'py, T: NumberLike + Element>(
  py: Python<'py>,
  mut src: &[u8],
  file_decompressor: FileDecompressor,
) -> PyResult<Bound<'py, PyArray1<T>>> {
  let res = py
    .allow_threads(|| {
      let n_hint = file_decompressor.n_hint();
      let mut res: Vec<T> = Vec::with_capacity(n_hint);
      while let MaybeChunkDecompressor::Some(mut chunk_decompressor) =
        file_decompressor.chunk_decompressor::<T, &[u8]>(src)?
      {
        let initial_len = res.len(); // probably always zero to start, since we just created res
        let remaining = chunk_decompressor.n();
        unsafe {
          res.set_len(initial_len + remaining);
        }
        let progress = chunk_decompressor.decompress(&mut res[initial_len..])?;
        assert!(progress.finished);
        src = chunk_decompressor.into_src();
      }
      Ok(res)
    })
    .map_err(pco_err_to_py)?;
  let py_array = res.into_pyarray_bound(py);
  Ok(py_array)
}

fn simple_compress_generic<'py, T: NumberLike + Element>(
  py: Python<'py>,
  arr: &Bound<'_, PyArray1<T>>,
  config: &ChunkConfig,
) -> PyResult<Bound<'py, PyBytes>> {
  let arr = arr.readonly();
  let src = arr.as_slice()?;
  let compressed = py
    .allow_threads(|| standalone::simple_compress(src, config))
    .map_err(pco_err_to_py)?;
  // TODO apparently all the places we use PyBytes::new() copy the data.
  // Maybe there's a zero-copy way to do this.
  Ok(PyBytes::new_bound(py, &compressed))
}

fn simple_decompress_into_generic<T: NumberLike + Element>(
  py: Python,
  compressed: &Bound<PyBytes>,
  arr: &Bound<PyArray1<T>>,
) -> PyResult<PyProgress> {
  let mut out_rw = arr.readwrite();
  let dst = out_rw.as_slice_mut()?;
  let src = compressed.as_bytes();
  let progress = py
    .allow_threads(|| standalone::simple_decompress_into(src, dst))
    .map_err(pco_err_to_py)?;
  Ok(PyProgress::from(progress))
}

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
  /// Compresses an array into a standalone format.
  ///
  /// :param nums: numpy array to compress. This may have any shape.
  /// However, it must be contiguous, and only the following data types are
  /// supported: float16, float32, float64, int16, int32, int64, uint16, uint32, uint64.
  /// :param config: a ChunkConfig object containing compression level and
  /// other settings.
  ///
  /// :returns: compressed bytes for an entire standalone file
  ///
  /// :raises: TypeError, RuntimeError
  #[pyfunction]
  fn simple_compress<'py>(
    py: Python<'py>,
    nums: &Bound<'_, PyUntypedArray>,
    config: &PyChunkConfig,
  ) -> PyResult<Bound<'py, PyBytes>> {
    let config: ChunkConfig = config.try_into()?;
    let dtype = nums.dtype();
    macro_rules! match_py_array {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        $(
        if dtype.is_equiv_to(&numpy::dtype_bound::<$t>(py)) {
          return simple_compress_generic(py, nums.downcast::<PyArray1<$t>>()?, &config)
        }
        )+
      }
    }
    with_core_dtypes!(match_py_array);

    Err(crate::unsupported_type_err(dtype))
  }
  m.add_function(wrap_pyfunction!(simple_compress, m)?)?;

  /// Decompresses pcodec compressed bytes into a pre-existing array.
  ///
  /// :param compressed: a bytes object a full standalone file of compressed data.
  /// :param dst: a numpy array to fill with the decompressed values. May have
  /// any shape, but must be contiguous.
  ///
  /// :returns: progress, an object with a count of elements written and
  /// whether the compressed data was finished. If dst is shorter than the
  /// numbers in compressed, writes as much as possible and leaves the rest
  /// untouched. If dst is longer, fills dst and does nothing with the
  /// remaining data.
  ///
  /// :raises: TypeError, RuntimeError
  #[pyfunction]
  fn simple_decompress_into(
    py: Python,
    compressed: &Bound<PyBytes>,
    dst: &Bound<PyUntypedArray>,
  ) -> PyResult<PyProgress> {
    let dtype = dst.dtype();
    macro_rules! match_py_array {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        $(
        if dtype.is_equiv_to(&numpy::dtype_bound::<$t>(py)) {
          return simple_decompress_into_generic(py, compressed, dst.downcast::<PyArray1<$t>>()?);
        }
        )+
      }
    }
    with_core_dtypes!(match_py_array);

    Err(crate::unsupported_type_err(dtype))
  }
  m.add_function(wrap_pyfunction!(simple_decompress_into, m)?)?;

  /// Decompresses pcodec compressed bytes into a new Numpy array.
  ///
  /// :param compressed: a bytes object a full standalone file of compressed data.
  ///
  /// :returns: data, either a 1D numpy array of the decompressed values or, in
  /// the event that there are no values, a None.
  /// The array's data type will be set appropriately based on the contents of
  /// the file header.
  ///
  /// :raises: TypeError, RuntimeError
  #[pyfunction]
  fn simple_decompress(py: Python, compressed: &Bound<PyBytes>) -> PyResult<PyObject> {
    use pco::data_types::CoreDataType::*;
    use pco::standalone::DataTypeOrTermination::*;

    let src = compressed.as_bytes();
    let (file_decompressor, src) = FileDecompressor::new(src).map_err(pco_err_to_py)?;
    let dtype = file_decompressor
      .peek_dtype_or_termination(src)
      .map_err(pco_err_to_py)?;
    macro_rules! match_dtype {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        match dtype {
          $(Known($name) => Ok(decompress_chunks::<$t>(py, src, file_decompressor)?.to_object(py)),)+
          Termination => Ok(PyNone::get_bound(py).to_object(py)),
          Unknown(other) => Err(PyRuntimeError::new_err(format!(
            "unrecognized dtype byte {:?}",
            other,
          ))),
        }
      }
    }
    with_core_dtypes!(match_dtype)
  }
  m.add_function(wrap_pyfunction!(simple_decompress, m)?)?;

  Ok(())
}
