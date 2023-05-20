use std::ops::AddAssign;

#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize,
  pub finished_body: bool,
  pub insufficient_data: bool,
}

impl AddAssign for Progress {
  fn add_assign(&mut self, rhs: Self) {
    self.n_processed += rhs.n_processed;
    self.finished_body |= rhs.finished_body;
    self.insufficient_data |= rhs.insufficient_data;
  }
}
