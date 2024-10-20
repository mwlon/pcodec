use numpy::{PyArrayDescr, PyArrayDescrMethods};
use pco::data_types::CoreDataType;
use pco::errors::PcoError;
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::{Bound, PyErr, PyResult, Python};

pub fn core_dtype_from_str(s: &str) -> PyResult<CoreDataType> {
  match s.to_uppercase().as_str() {
    "F16" => Ok(CoreDataType::F16),
    "F32" => Ok(CoreDataType::F32),
    "F64" => Ok(CoreDataType::F64),
    "I16" => Ok(CoreDataType::I16),
    "I32" => Ok(CoreDataType::I32),
    "I64" => Ok(CoreDataType::I64),
    "U16" => Ok(CoreDataType::U16),
    "U32" => Ok(CoreDataType::U32),
    "U64" => Ok(CoreDataType::U64),
    _ => Err(PyRuntimeError::new_err(format!(
      "unknown data type: {}",
      s,
    ))),
  }
}

pub fn core_dtype_from_numpy(py: Python, dtype: &Bound<PyArrayDescr>) -> PyResult<CoreDataType> {
  let res = if dtype.is_equiv_to(&numpy::dtype_bound::<u16>(py)) {
    CoreDataType::U16
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<u32>(py)) {
    CoreDataType::U32
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<u64>(py)) {
    CoreDataType::U64
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<i16>(py)) {
    CoreDataType::I16
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<i32>(py)) {
    CoreDataType::I32
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<i64>(py)) {
    CoreDataType::I64
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<half::f16>(py)) {
    CoreDataType::F16
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<f32>(py)) {
    CoreDataType::F32
  } else if dtype.is_equiv_to(&numpy::dtype_bound::<f64>(py)) {
    CoreDataType::F64
  } else {
    return Err(PyTypeError::new_err(format!(
      "Unsupported data type: {:?}",
      dtype
    )));
  };
  Ok(res)
}

pub fn pco_err_to_py(pco: PcoError) -> PyErr {
  PyRuntimeError::new_err(format!("pco error: {}", pco))
}
