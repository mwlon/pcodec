use std::convert::TryInto;

use numpy::{Element, IntoPyArray, PyArray1, PyArrayDyn};
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyBytes, PyModule, PyNone};
use pyo3::{pyfunction, wrap_pyfunction, PyObject, PyResult, Python};

use pco::data_types::NumberLike;
use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::{standalone, with_core_dtypes, ChunkConfig};

use crate::{pco_err_to_py, DynTypedPyArrayDyn, PyChunkConfig, PyProgress};

fn decompress_chunks<'py, T: NumberLike + Element>(
  py: Python<'py>,
  mut src: &[u8],
  file_decompressor: FileDecompressor,
) -> PyResult<&'py PyArray1<T>> {
  let n_hint = file_decompressor.n_hint();
  let mut res: Vec<T> = Vec::with_capacity(n_hint);
  while let MaybeChunkDecompressor::Some(mut chunk_decompressor) = file_decompressor
    .chunk_decompressor::<T, &[u8]>(src)
    .map_err(pco_err_to_py)?
  {
    let initial_len = res.len(); // probably always zero to start, since we just created res
    let remaining = chunk_decompressor.n();
    unsafe {
      res.set_len(initial_len + remaining);
    }
    let progress = chunk_decompressor
      .decompress(&mut res[initial_len..])
      .map_err(pco_err_to_py)?;
    assert!(progress.finished);
    src = chunk_decompressor.into_src();
  }
  let py_array = res.into_pyarray(py);
  Ok(py_array)
}

fn simple_compress_generic<'py, T: NumberLike + Element>(
  py: Python<'py>,
  arr: &'py PyArrayDyn<T>,
  config: &ChunkConfig,
) -> PyResult<PyObject> {
  let arr_ro = arr.readonly();
  let src = arr_ro.as_slice()?;
  let compressed = standalone::simple_compress(src, config).map_err(pco_err_to_py)?;
  // TODO apparently all the places we use PyBytes::new() copy the data.
  // Maybe there's a zero-copy way to do this.
  Ok(PyBytes::new(py, &compressed).into())
}

fn simple_decompress_into_generic<T: NumberLike + Element>(
  compressed: &PyBytes,
  arr: &PyArrayDyn<T>,
) -> PyResult<PyProgress> {
  let mut out_rw = arr.readwrite();
  let dst = out_rw.as_slice_mut()?;
  let src = compressed.as_bytes();
  let progress = standalone::simple_decompress_into(src, dst).map_err(pco_err_to_py)?;
  Ok(PyProgress::from(progress))
}

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  /// Compresses an array into a standalone format.
  ///
  /// :param nums: numpy array to compress. This may have any shape.
  /// However, it must be contiguous, and only the following data types are
  /// supported: float32, float64, int32, int64, uint32, uint64.
  /// :param config: a ChunkConfig object containing compression level and
  /// other settings.
  ///
  /// :returns: compressed bytes for an entire standalone file
  ///
  /// :raises: TypeError, RuntimeError
  #[pyfunction]
  fn simple_compress<'py>(
    py: Python<'py>,
    nums: DynTypedPyArrayDyn<'py>,
    config: &PyChunkConfig,
  ) -> PyResult<PyObject> {
    let config: ChunkConfig = config.try_into()?;
    macro_rules! match_py_array {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
        match nums {
          $(DynTypedPyArrayDyn::$name(arr) => simple_compress_generic(py, arr, &config),)+
        }
      }
    }
    with_core_dtypes!(match_py_array)
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
  fn simple_decompress_into(compressed: &PyBytes, dst: DynTypedPyArrayDyn) -> PyResult<PyProgress> {
    macro_rules! match_py_array {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
        match dst {
          $(DynTypedPyArrayDyn::$name(arr) => simple_decompress_into_generic(compressed, arr),)+
        }
      }
    }
    with_core_dtypes!(match_py_array)
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
  fn simple_decompress(py: Python, compressed: &PyBytes) -> PyResult<PyObject> {
    use pco::data_types::CoreDataType::*;
    use pco::standalone::DataTypeOrTermination::*;

    let src = compressed.as_bytes();
    let (file_decompressor, src) = FileDecompressor::new(src).map_err(pco_err_to_py)?;
    let dtype = file_decompressor
      .peek_dtype_or_termination(src)
      .map_err(pco_err_to_py)?;
    macro_rules! match_dtype {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
        match dtype {
          $(Known($name) => Ok(decompress_chunks::<$t>(py, src, file_decompressor)?.into()),)+
          Termination => Ok(PyNone::get(py).into()),
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
