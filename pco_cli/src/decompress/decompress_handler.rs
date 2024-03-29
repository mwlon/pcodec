use std::cmp::min;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::PrimitiveArray;
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::datatypes::{ArrowPrimitiveType, Field, Schema};
use arrow::record_batch::RecordBatch;
use better_io::BetterBufReader;

use pco::standalone::{FileDecompressor, MaybeChunkDecompressor};
use pco::FULL_BATCH_N;

use crate::core_handlers::CoreHandlerImpl;
use crate::dtypes::PcoNumberLike;
use crate::opt::DecompressOpt;

pub trait DecompressHandler {
  fn decompress(&self, opt: &DecompressOpt) -> Result<()>;
}

impl<T: PcoNumberLike> DecompressHandler for CoreHandlerImpl<T> {
  fn decompress(&self, opt: &DecompressOpt) -> Result<()> {
    let file = OpenOptions::new().read(true).open(&opt.pco_path)?;
    let src = BetterBufReader::from_read_simple(file);
    let (fd, mut src) = FileDecompressor::new(src)?;

    let mut writer = new_column_writer::<T>(opt)?;
    let mut remaining_limit = opt.limit.unwrap_or(usize::MAX);
    let mut nums = Vec::new();

    loop {
      if remaining_limit == 0 {
        break;
      }

      if let MaybeChunkDecompressor::Some(mut cd) = fd.chunk_decompressor::<T, _>(src)? {
        let n = cd.n();
        let batch_size = min(n, remaining_limit);
        // how many pco should decompress
        let pco_size = (1 + batch_size / FULL_BATCH_N) * FULL_BATCH_N;
        nums.resize(pco_size, T::default());
        let _ = cd.decompress(&mut nums)?;
        src = cd.into_src();
        let arrow_nums = nums
          .iter()
          .take(batch_size)
          .map(|&x| T::to_arrow_native(x))
          .collect::<Vec<_>>();
        writer.write(arrow_nums)?;
        remaining_limit -= batch_size;
      } else {
        break;
      }
    }

    writer.close()?;
    Ok(())
  }
}

fn new_column_writer<T: PcoNumberLike>(opt: &DecompressOpt) -> Result<Box<dyn ColumnWriter<T>>> {
  // eventually we'll likely have a txt writer and a parquet writer, etc.
  Ok(Box::new(StdoutWriter::from_opt(opt)))
}

trait ColumnWriter<T: PcoNumberLike> {
  fn from_opt(opt: &DecompressOpt) -> Self
  where
    Self: Sized;
  fn write(&mut self, nums: Vec<<T::Arrow as ArrowPrimitiveType>::Native>) -> Result<()>;
  fn close(&mut self) -> Result<()>;
}

#[derive(Default)]
struct StdoutWriter<T: PcoNumberLike> {
  phantom: PhantomData<T>,
}

impl<T: PcoNumberLike> ColumnWriter<T> for StdoutWriter<T> {
  fn from_opt(_opt: &DecompressOpt) -> Self {
    Self {
      phantom: PhantomData,
    }
  }

  fn write(&mut self, arrow_natives: Vec<<T::Arrow as ArrowPrimitiveType>::Native>) -> Result<()> {
    let schema = Schema::new(vec![Field::new("c0", T::ARROW_DTYPE, false)]);
    let c0 = PrimitiveArray::<T::Arrow>::from_iter_values(arrow_natives);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c0)])?;
    let mut stdout_bytes = Vec::<u8>::new();
    {
      let mut writer = CsvWriterBuilder::new()
        .with_header(false)
        .build(&mut stdout_bytes);
      writer.write(&batch)?;
    }
    print!("{}", String::from_utf8(stdout_bytes)?);
    Ok(())
  }

  fn close(&mut self) -> Result<()> {
    Ok(())
  }
}
