use half::f16;
use numpy::PyArrayDyn;
use pco::data_types::CoreDataType;
use pco::{ChunkConfig, DeltaSpec, ModeSpec, PagingSpec, Progress};
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

#[pyclass(name = "ModeSpec")]
#[derive(Clone, Default)]
pub struct PyModeSpec(ModeSpec);

/// Specifies how Pcodec should choose the mode.
#[pymethods]
impl PyModeSpec {
  /// :returns: a ModeSpec that automatically detects a good mode.
  #[staticmethod]
  fn auto() -> Self {
    Self(ModeSpec::Auto)
  }

  /// :returns: a ModeSpec that always uses the simplest mode.
  #[staticmethod]
  fn classic() -> Self {
    Self(ModeSpec::Classic)
  }

  /// :returns: a ModeSpec that tries to use the IntMult mode with the given
  /// base, if possible.
  #[staticmethod]
  fn try_float_mult(base: f64) -> Self {
    Self(ModeSpec::TryFloatMult(base))
  }

  /// :returns: a ModeSpec that tries to use the IntMult mode with the given
  /// base, if possible.
  #[staticmethod]
  fn try_float_quant(k: u32) -> Self {
    Self(ModeSpec::TryFloatQuant(k))
  }

  /// :returns: a ModeSpec that tries to use the IntMult mode with the given
  /// base, if possible.
  #[staticmethod]
  fn try_int_mult(base: u64) -> Self {
    Self(ModeSpec::TryIntMult(base))
  }
}

#[pyclass(name = "ModeSpec")]
#[derive(Clone, Default)]
pub struct PyDeltaSpec(DeltaSpec);

/// Specifies how Pcodec should choose the delta encoding.
#[pymethods]
impl PyDeltaSpec {
  /// :returns: a DeltaSpec that automatically detects a good choice.
  #[staticmethod]
  fn auto() -> Self {
    Self(DeltaSpec::Auto)
  }

  /// :returns: a DeltaSpec that never does delta encoding.
  #[staticmethod]
  fn none() -> Self {
    Self(DeltaSpec::None)
  }

  /// :returns: a DeltaSpec that tries to use the specified delta encoding
  /// order, if possible.
  #[staticmethod]
  fn try_consecutive(order: usize) -> Self {
    Self(DeltaSpec::TryConsecutive(order))
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
  mode_spec: PyModeSpec,
  delta_spec: PyDeltaSpec,
  paging_spec: PyPagingSpec,
}

#[pymethods]
impl PyChunkConfig {
  /// Creates a ChunkConfig.
  ///
  /// :param compression_level: a compression level from 0-12, where 12 takes
  /// the longest and compresses the most.
  ///
  /// :param delta_spec: either a delta encoding level from 0-7 or
  /// None. If set to None, pcodec will try to infer the optimal delta encoding
  /// order.
  ///
  /// :param int_mult_spec: a IntMultSpec that configures whether integer
  /// multiplier detection is enabled.
  ///
  /// Examples where this helps:
  /// * nanosecond-precision timestamps that are mostly whole numbers of
  /// microseconds, with a few exceptions
  /// * integers `[7, 107, 207, 307, ... 100007]` shuffled
  ///
  /// When this is helpful, compression and decompression speeds can be
  /// substantially reduced. This configuration may hurt
  /// compression speed slightly even when it isn't helpful.
  /// However, the compression ratio improvements tend to be large.
  ///
  /// :param float_mult_spec: a FloatMultSpec that configures whether float
  /// multiplier detection is enabled.
  ///
  /// Examples where this helps:
  /// * approximate multiples of 0.01
  /// * approximate multiples of pi
  ///
  /// Float mults can work even when there are NaNs and infinities.
  /// When this is helpful, compression and decompression speeds can be
  /// substantially reduced. In rare cases, this configuration
  /// may reduce compression speed somewhat even when it isn't helpful.
  /// However, the compression ratio improvements tend to be large.
  ///
  /// :param float_quant_spec: a FloatQuantSpec that configures whether
  /// quantized-float detection is enabled.
  ///
  /// Examples where this helps:
  /// * float-valued data stored in a type that is unnecessarily wide (e.g.
  /// stored as `f64`s where only a `f32` worth of precision is used)
  ///
  /// :param paging_spec: a PagingSpec describing how many numbers should
  /// go into each page.
  ///
  /// :returns: A new ChunkConfig object.
  #[new]
  #[pyo3(signature = (
    compression_level=pco::DEFAULT_COMPRESSION_LEVEL,
    mode_spec=PyModeSpec::default(),
    delta_spec=PyDeltaSpec::default(),
    paging_spec=PyPagingSpec::default(),
  ))]
  fn new(
    compression_level: usize,
    mode_spec: PyModeSpec,
    delta_spec: PyDeltaSpec,
    paging_spec: PyPagingSpec,
  ) -> Self {
    Self {
      compression_level,
      delta_spec,
      mode_spec,
      paging_spec,
    }
  }
}

impl TryFrom<&PyChunkConfig> for ChunkConfig {
  type Error = PyErr;

  fn try_from(py_config: &PyChunkConfig) -> Result<Self, Self::Error> {
    let res = ChunkConfig::default()
      .with_compression_level(py_config.compression_level)
      .with_delta_spec(py_config.delta_spec.0)
      .with_mode_spec(py_config.mode_spec.0)
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
  m.add_class::<PyModeSpec>()?;
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
