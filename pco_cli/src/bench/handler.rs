use anyhow::Result;
use arrow::array::{ArrayRef, AsArray};

use crate::arrow_handlers::ArrowHandlerImpl;
use crate::bench::codecs::{CodecConfig, CodecSurface};
use crate::bench::{BenchOpt, BenchStat, PrintStat};
use crate::dtypes::{ArrowNumberLike, PcoNumberLike};

pub trait BenchHandler {
  fn bench(arrays: &[ArrayRef], dataset: &str, opt: &BenchOpt) -> Result<Vec<PrintStat>>;
}

fn handle_nums<T: PcoNumberLike>(
  nums: &[T],
  dataset: String,
  codec: &CodecConfig,
  opt: &BenchOpt,
) -> PrintStat {
  let precomputed = codec.warmup_iter(nums, &dataset, &opt.iter_opt);

  let mut benches = Vec::with_capacity(opt.iters);
  for _ in 0..opt.iters {
    benches.push(codec.stats_iter(&nums, &precomputed, &opt.iter_opt));
  }
  PrintStat::compute(dataset, codec.to_string(), &benches)
}

impl<P: ArrowNumberLike> BenchHandler for ArrowHandlerImpl<P> {
  fn bench(arrays: &[ArrayRef], dataset: &str, opt: &BenchOpt) -> Result<Vec<PrintStat>> {
    let arrow_nums: Vec<P::Native> = arrays
      .iter()
      .flat_map(|arr| arr.as_primitive::<P>().values())
      .collect::<Vec<_>>();

    let nums = P::native_vec_to_pco(arrow_nums);

    let mut stats = Vec::new();
    for codec in &opt.codecs {
      stats.push(handle_nums(
        &nums,
        dataset.to_string(),
        codec,
        opt,
      ));
    }

    Ok(stats)
  }
}
