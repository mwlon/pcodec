use anyhow::Result;
use arrow::array::{ArrayRef, AsArray};

use crate::arrow_handlers::ArrowHandlerImpl;
use crate::bench::codecs::{CodecConfig, CodecSurface};
use crate::bench::num_vec::NumVec;
use crate::bench::{BenchOpt, core_dtype_to_str, PrintStat};
use crate::dtypes::ArrowNumberLike;

pub trait BenchHandler {
  fn bench(&self, nums: &NumVec, dataset: &str, opt: &BenchOpt) -> Result<Vec<PrintStat>>;
  fn bench_from_arrow(
    &self,
    arrays: &[ArrayRef],
    name: &str,
    opt: &BenchOpt,
  ) -> Result<Vec<PrintStat>>;
}

fn handle_for_codec(
  num_vec: &NumVec,
  name: &str,
  codec: &CodecConfig,
  opt: &BenchOpt,
) -> Result<PrintStat> {
  let dataset = format!(
    "{}_{}",
    core_dtype_to_str(num_vec.dtype()),
    name,
  );
  let precomputed = codec.warmup_iter(num_vec, &dataset, &opt.iter_opt)?;

  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(codec.stats_iter(&num_vec, &precomputed, &opt.iter_opt)?);
  }
  Ok(PrintStat::compute(
    dataset,
    codec.to_string(),
    &benches,
  ))
}

impl<P: ArrowNumberLike> BenchHandler for ArrowHandlerImpl<P> {
  fn bench(&self, num_vec: &NumVec, name: &str, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
    let mut stats = Vec::new();
    for codec in &opt.codecs {
      stats.push(handle_for_codec(
        &num_vec,
        name,
        codec,
        opt,
      )?);
    }

    Ok(stats)
  }

  fn bench_from_arrow(
    &self,
    arrays: &[ArrayRef],
    name: &str,
    opt: &BenchOpt,
  ) -> Result<Vec<PrintStat>> {
    let arrow_nums: Vec<P::Native> = arrays
      .iter()
      .flat_map(|arr| arr.as_primitive::<P>().values().iter())
      .collect::<Vec<_>>();

    let nums = P::native_vec_to_pco(arrow_nums);
    let num_vec: NumVec = ;

    self.bench(&num_vec, name, opt)
  }
}
