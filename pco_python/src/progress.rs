use pco::Progress;
use pyo3::pyclass;

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
