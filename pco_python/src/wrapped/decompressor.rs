use numpy::{PyArray1, PyArrayDescrMethods, PyArrayMethods, PyUntypedArray, PyUntypedArrayMethods};
use pco::data_types::CoreDataType;
use pco::{match_number_like_enum, with_core_dtypes, Progress};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pymethods, AsPyPointer, Bound, PyResult, Python};

use pco::data_types::NumberLike;
use pco::wrapped::{ChunkCompressor, ChunkDecompressor, FileDecompressor};

use crate::{core_dtype_from_str, pco_err_to_py, DynTypedPyArrayDyn, PyProgress};

#[pyclass(name = "FileDecompressor")]
struct PyFd(FileDecompressor);

pco::define_number_like_enum!(
  #[derive()]
  DynCd,
  ChunkDecompressor
);

#[pyclass(name = "ChunkDecompressor")]
struct PyCd {
  inner: DynCd,
  dtype: CoreDataType,
}

/// The top-level object for decompressing wrapped pcodec files.
#[pymethods]
impl PyFd {
  /// Creates a FileDecompressor.
  ///
  /// :param src: a bytes object containing the encoded header
  ///
  /// :returns: a tuple containing a FileDecompressor and the number of bytes
  /// read
  ///
  /// :raises: TypeError, RuntimeError
  #[staticmethod]
  fn new(src: &Bound<PyBytes>) -> PyResult<(Self, usize)> {
    let src = src.as_bytes();
    let (fd, rest) = FileDecompressor::new(src).map_err(pco_err_to_py)?;
    let py_fd = PyFd(fd);

    let n_bytes_read = src.len() - rest.len();
    Ok((py_fd, n_bytes_read))
  }

  /// Creates a ChunkDecompressor by reading encoded chunk metadata.
  ///
  /// :param src: a bytes object containing the encoded chunk metadata
  /// :param dtype: a data type supported by pcodec; e.g. 'f32' or 'i64'
  ///
  /// :returns: a tuple containing a ChunkDecompressor and the number of bytes
  /// read
  ///
  /// :raises: TypeError, RuntimeError
  fn read_chunk_meta(&self, src: &Bound<PyBytes>, dtype: &str) -> PyResult<(PyCd, usize)> {
    let src = src.as_bytes();
    let inner = &self.0;
    let dtype = core_dtype_from_str(dtype)?;

    macro_rules! match_dtype {
      {$($name:ident($lname:ident) => $t:ty,)+} => {
        match dtype {
          $(CoreDataType::$name => {
            let (generic_cd, rest) = inner
              .chunk_decompressor::<$t, _>(src)
              .map_err(pco_err_to_py)?;
            (DynCd::$name(generic_cd), rest)
          })+
        }
      }
    }

    let (inner, rest) = with_core_dtypes!(match_dtype);
    let res = PyCd { inner, dtype };
    let n_bytes_read = src.len() - rest.len();
    Ok((res, n_bytes_read))
  }
}

#[pymethods]
impl PyCd {
  // TODO find a way to reuse docstring content
  /// Decompresses a page into the provided array. If dst is shorter than
  /// page_n, writes as much as possible and leaves the rest
  /// untouched. If dst is longer, fills dst and does nothing with the
  /// remaining data.
  ///
  /// :param page: the encoded page
  /// :param page_n: the total count of numbers in the encoded page. It is
  /// expected that the wrapping format provides this information.
  /// :param dst: a numpy array to fill with the decompressed values. Must be
  /// contiguous, and its length must either be
  /// * >= page_n, or
  /// * a multiple of 256.
  ///
  /// :returns: a tuple containing progress and the number of bytes read.
  /// Progress is an object with a count of elements written and
  /// whether the compressed data was finished.
  ///
  /// :raises: TypeError, RuntimeError
  fn read_page_into(
    &self,
    py: Python,
    page: &Bound<PyBytes>,
    page_n: usize,
    dst: &Bound<PyUntypedArray>,
  ) -> PyResult<(PyProgress, usize)> {
    let src = page.as_bytes();

    let (progress, rest) = match_number_like_enum!(
      &self.inner,
      DynCd<T>(cd) => {
        let arr = dst.downcast::<PyArray1<T>>()?;
        let mut arr_rw = arr.readwrite();
        let dst = arr_rw.as_slice_mut()?;
        py.allow_threads(|| {
          let mut pd = cd.page_decompressor(src, page_n)?;
          let progress = pd.decompress(dst)?;
          Ok((progress, pd.into_src()))
        }).map_err(pco_err_to_py)?
      }
    );
    let n_bytes_read = src.len() - rest.len();
    Ok((PyProgress::from(progress), n_bytes_read))
  }
}

pub fn register(m: &Bound<PyModule>) -> PyResult<()> {
  m.add_class::<PyFd>()?;
  m.add_class::<PyCd>()?;

  Ok(())
}
