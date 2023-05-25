#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize,
  pub finished_body: bool,
  pub insufficient_data: bool,
}
