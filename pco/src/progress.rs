use std::ops::AddAssign;

#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize,  // # of numbers decompressed
  pub finished_page: bool, // all numbers have been decompressed
}

impl AddAssign for Progress {
  fn add_assign(&mut self, rhs: Self) {
    self.n_processed += rhs.n_processed;
    self.finished_page = rhs.finished_page;
  }
}
