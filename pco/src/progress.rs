use std::ops::AddAssign;

#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize,      // # of numbers decompressed
  pub finished_body: bool,     // all numbers have been decompressed
  pub insufficient_data: bool, // all bytes have been read
}

impl AddAssign for Progress {
  fn add_assign(&mut self, rhs: Self) {
    self.n_processed += rhs.n_processed;
    self.finished_body = rhs.finished_body;
    self.insufficient_data = rhs.insufficient_data;
  }
}
