use numpy::Element;
use pco::data_types::{CoreDataType, NumberLike};
use pco::with_core_dtypes;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::{PyBytes, PyModule};
use pyo3::{pyclass, pyfunction, pymethods, wrap_pyfunction, PyResult, Python};

use pco::wrapped::{ChunkDecompressor, FileDecompressor};

use crate::{core_dtype_from_str, pco_err_to_py, DynTypedPyArrayDyn, PyProgress};

#[pyclass(name = "FileDecompressor")]
struct PyFd(FileDecompressor);

macro_rules! impl_dyn_cd {
  {$($name:ident($uname:ident) => $t:ty,)+} => {
    #[derive(Debug)]
    enum DynCd {
      $($name(ChunkDecompressor<$t>),)+
    }
  }
}
with_core_dtypes!(impl_dyn_cd);

#[pyclass(name = "ChunkDecompressor")]
struct PyCd {
  inner: DynCd,
  dtype: CoreDataType,
}

#[pymethods]
impl PyFd {
  #[staticmethod]
  fn from_header(header: &PyBytes) -> PyResult<(Self, usize)> {
    let src = header.as_bytes();
    let (fd, rest) = FileDecompressor::new(src).map_err(pco_err_to_py)?;
    let py_fd = PyFd(fd);

    let n_bytes_read = src.len() - rest.len();
    Ok((py_fd, n_bytes_read))
  }

  fn read_chunk_meta(&self, chunk_meta: &PyBytes, dtype: &str) -> PyResult<(PyCd, usize)> {
    let src = chunk_meta.as_bytes();
    let inner = &self.0;
    let dtype = core_dtype_from_str(dtype)?;

    macro_rules! match_dtype {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
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
  fn read_page_into(
    &self,
    page: &PyBytes,
    page_n: usize,
    dst: DynTypedPyArrayDyn,
  ) -> PyResult<(PyProgress, usize)> {
    let src = page.as_bytes();
    let inner = &self.inner;
    macro_rules! match_cd_and_dst {
      {$($name:ident($uname:ident) => $t:ty,)+} => {
        match (inner, dst) {
          $((DynCd::$name(cd), DynTypedPyArrayDyn::$name(arr)) => {
            let mut arr_rw = arr.readwrite();
            let dst = arr_rw.as_slice_mut()?;
            let mut pd = cd.page_decompressor(src, page_n).map_err(pco_err_to_py)?;
            let progress = pd.decompress(dst).map_err(pco_err_to_py)?;
            (progress, pd.into_src())
          })+
          _ => {
            return Err(PyRuntimeError::new_err(format!(
              "incompatible data types; chunk decompressor expected {:?}",
              self.dtype
            )))
          }
        };
      }
    }
    let (progress, rest) = with_core_dtypes!(match_cd_and_dst);

    let n_bytes_read = src.len() - rest.len();
    Ok((PyProgress::from(progress), n_bytes_read))
  }
}

pub fn register(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
  m.add_class::<PyFd>()?;
  m.add_class::<PyCd>()?;

  Ok(())
}
