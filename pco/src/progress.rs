#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
  pub n_processed: usize, // # of numbers decompressed
  pub finished: bool,     // all numbers have been decompressed
}
