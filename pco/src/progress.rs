#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize,  // # of numbers decompressed
  pub finished_page: bool, // all numbers have been decompressed
}
