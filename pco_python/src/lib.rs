use half::f16;
use numpy::PyArrayDyn;
use pco::data_types::CoreDataType;
use pco::{ChunkConfig, FloatMultSpec, FloatQuantSpec, IntMultSpec, PagingSpec, Progress};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{pymodule, FromPyObject, PyModule, PyResult, Python};
use pyo3::{py_run, pyclass, pymethods, PyErr};

use pco::errors::PcoError;

pub mod standalone;
pub mod wrapped;

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

#[pyclass(get_all, name = "Progress")]
pub struct PyProgress {
  /// count of decompressed numbers.
  n_processed: usize,
  /// whether the compressed data was finished.
  finished: bool,
}

impl From<Progress> for PyProgress {
  fn from(progress: Progress) -> Self {
    Self {
      n_processed: progress.n_processed,
      finished: progress.finished,
    }
  }
}

pub fn pco_err_to_py(pco: PcoError) -> PyErr {
  PyRuntimeError::new_err(format!("pco error: {}", pco))
}

#[pyclass(name = "IntMultSpec")]
#[derive(Clone, Default)]
pub struct PyIntMultSpec(IntMultSpec);

/// Specifies how pcodec should handle modulo compression for integer types.
#[pymethods]
impl PyIntMultSpec {
  /// :returns: a IntMultSpec disabling modulo compression.
  #[staticmethod]
  fn disabled() -> Self {
    Self(IntMultSpec::Disabled)
  }

  /// :returns: a IntMultSpec enabling modulo compression.
  #[staticmethod]
  fn enabled() -> Self {
    Self(IntMultSpec::Enabled)
  }

  /// :returns: a IntMultSpec with a specific `base` for modulo compression.
  #[staticmethod]
  fn provided(base: u64) -> Self {
    Self(IntMultSpec::Provided(base))
  }
}

#[pyclass(name = "FloatMultSpec")]
#[derive(Clone, Default)]
pub struct PyFloatMultSpec(FloatMultSpec);

/// Specifies how pcodec should handle floating point multiplication
/// compression.
#[pymethods]
impl PyFloatMultSpec {
  /// :returns: a FloatMultSpec disabling floating point multiplication.
  #[staticmethod]
  fn disabled() -> Self {
    Self(FloatMultSpec::Disabled)
  }

  /// :returns: a FloatMultSpec enabling floating point multiplication.
  #[staticmethod]
  fn enabled() -> Self {
    Self(FloatMultSpec::Enabled)
  }

  /// :returns: a FloatMultSpec with a specific `base` for floating point
  #[staticmethod]
  fn provided(base: f64) -> Self {
    Self(FloatMultSpec::Provided(base))
  }
}

#[pyclass(name = "FloatQuantSpec")]
#[derive(Clone, Default)]
pub struct PyFloatQuantSpec(FloatQuantSpec);

/// Specifies how pcodec should handle floating point quantization compression.
#[pymethods]
impl PyFloatQuantSpec {
  /// :returns: a FloatQuantSpec disabling floating point quantization.
  #[staticmethod]
  fn disabled() -> Self {
    Self(FloatQuantSpec::Disabled)
  }

  /// :returns: a FloatQuantSpec with a specific `bits` for floating point
  #[staticmethod]
  fn provided(bits: u32) -> Self {
    Self(FloatQuantSpec::Provided(bits))
  }
}

#[pyclass(name = "PagingSpec")]
#[derive(Clone, Default)]
pub struct PyPagingSpec(PagingSpec);

/// Determines how pcodec splits a chunk into pages. In
/// standalone.simple_compress, this instead controls how pcodec splits a file
/// into chunks.
#[pymethods]
impl PyPagingSpec {
  /// :returns: a PagingSpec configuring a roughly count of numbers in each
  /// page.
  #[staticmethod]
  fn equal_pages_up_to(n: usize) -> Self {
    Self(PagingSpec::EqualPagesUpTo(n))
  }

  /// :returns: a PagingSpec with the exact, provided count of numbers in each
  /// page.
  #[staticmethod]
  fn exact_page_sizes(sizes: Vec<usize>) -> Self {
    Self(PagingSpec::Exact(sizes))
  }
}

#[pyclass(get_all, set_all, name = "ChunkConfig")]
pub struct PyChunkConfig {
  compression_level: usize,
  delta_encoding_order: Option<usize>,
  int_mult_spec: PyIntMultSpec,
  float_mult_spec: PyFloatMultSpec,
  float_quant_spec: PyFloatQuantSpec,
  paging_spec: PyPagingSpec,
}

#[pymethods]
impl PyChunkConfig {
  /// Creates a ChunkConfig.
  ///
  /// :param compression_level: a compression level from 0-12, where 12 takes
  /// the longest and compresses the most.
  /// :param delta_encoding_order: either a delta encoding level from 0-7 or
  /// None. If set to None, pcodec will try to infer the optimal delta encoding
  /// order.
  /// :param int_mult_spec: a IntMultSpec disabling, enabling, or providing a
  /// base for modulo compression. If enabled, pcodec will consider using int
  /// mult mode, which can substantially improve compression ratio but decrease
  /// speed in some cases for integer types. If a base is provided, pcodec will
  /// use that base for modulo compression. Enabled by default.
  /// :param float_mult_spec: a FloatMultSpec disabling, enabling, or providing
  /// a base for floating point multiplication compression. If enabled,
  /// pcodec will consider using float mult mode, which can substantially
  /// improve compression ratio but decrease speed in some cases for floating
  /// point types. If a base is provided, pcodec will use that base for
  /// floating point multiplication compression. Enabled by default.
  /// :param float_quant_spec: a FloatQuantSpec disabling or providing the
  /// `bits` for floating point quantization compression. If provided, pcodec
  /// will use that number of bits for floating point quantization compression.
  /// To use quantization, float mult must be disabled. Disabled by default.
  /// :param paging_spec: a PagingSpec describing how many numbers should
  /// go into each page.
  ///
  /// :returns: A new ChunkConfig object.
  #[new]
  #[pyo3(signature = (
    compression_level=pco::DEFAULT_COMPRESSION_LEVEL,
    delta_encoding_order=None,
    int_mult_spec=PyIntMultSpec::default(),
    float_mult_spec=PyFloatMultSpec::default(),
    float_quant_spec=PyFloatQuantSpec::default(),
    paging_spec=PyPagingSpec::default(),
  ))]
  fn new(
    compression_level: usize,
    delta_encoding_order: Option<usize>,
    int_mult_spec: PyIntMultSpec,
    float_mult_spec: PyFloatMultSpec,
    float_quant_spec: PyFloatQuantSpec,
    paging_spec: PyPagingSpec,
  ) -> Self {
    Self {
      compression_level,
      delta_encoding_order,
      int_mult_spec,
      float_mult_spec,
      float_quant_spec,
      paging_spec,
    }
  }
}

impl TryFrom<&PyChunkConfig> for ChunkConfig {
  type Error = PyErr;

  fn try_from(py_config: &PyChunkConfig) -> Result<Self, Self::Error> {
    let res = ChunkConfig::default()
      .with_compression_level(py_config.compression_level)
      .with_delta_encoding_order(py_config.delta_encoding_order)
      .with_int_mult_spec(py_config.int_mult_spec.0)
      .with_float_mult_spec(py_config.float_mult_spec.0)
      .with_float_quant_spec(py_config.float_quant_spec.0)
      .with_paging_spec(py_config.paging_spec.0.clone());
    Ok(res)
  }
}

// The Numpy crate recommends using this type of enum to write functions that accept different Numpy dtypes
// https://github.com/PyO3/rust-numpy/blob/32740b33ec55ef0b7ebec726288665837722841d/examples/simple/src/lib.rs#L113
// The first dyn refers to dynamic dtype; the second to dynamic shape
#[derive(Debug, FromPyObject)]
pub enum DynTypedPyArrayDyn<'py> {
  F16(&'py PyArrayDyn<f16>),
  F32(&'py PyArrayDyn<f32>),
  F64(&'py PyArrayDyn<f64>),
  I16(&'py PyArrayDyn<i16>),
  I32(&'py PyArrayDyn<i32>),
  I64(&'py PyArrayDyn<i64>),
  U16(&'py PyArrayDyn<u16>),
  U32(&'py PyArrayDyn<u32>),
  U64(&'py PyArrayDyn<u64>),
}

/// Pcodec is a codec for numerical sequences.
#[pymodule]
fn pcodec(py: Python, m: &PyModule) -> PyResult<()> {
  m.add("__version__", env!("CARGO_PKG_VERSION"))?;
  m.add_class::<PyProgress>()?;
  m.add_class::<PyIntMultSpec>()?;
  m.add_class::<PyFloatMultSpec>()?;
  m.add_class::<PyFloatQuantSpec>()?;
  m.add_class::<PyPagingSpec>()?;
  m.add_class::<PyChunkConfig>()?;
  m.add(
    "DEFAULT_COMPRESSION_LEVEL",
    pco::DEFAULT_COMPRESSION_LEVEL,
  )?;

  // =========== STANDALONE ===========
  let standalone_module = PyModule::new(py, "pcodec.standalone")?;
  standalone::register(py, standalone_module)?;
  // hackery from https://github.com/PyO3/pyo3/issues/1517#issuecomment-808664021
  // to make modules work nicely
  py_run!(
    py,
    standalone_module,
    "import sys; sys.modules['pcodec.standalone'] = standalone_module"
  );
  m.add_submodule(standalone_module)?;

  // =========== WRAPPED ===========
  let wrapped_module = PyModule::new(py, "pcodec.wrapped")?;
  wrapped::register(py, wrapped_module)?;
  py_run!(
    py,
    wrapped_module,
    "import sys; sys.modules['pcodec.wrapped'] = wrapped_module"
  );
  m.add_submodule(wrapped_module)?;

  Ok(())
}
