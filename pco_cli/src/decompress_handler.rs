use std::cmp::min;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::PrimitiveArray;
use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use pco::standalone::FileDecompressor;

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::DecompressOpt;

pub trait DecompressHandler {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()>;
}

impl<P: NumberLikeArrow> DecompressHandler for HandlerImpl<P> {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()> {
    let (fd, mut consumed) = FileDecompressor::new(bytes)?;

    let mut writer = new_column_writer::<P>(opt)?;
    let mut remaining_limit = opt.limit.unwrap_or(usize::MAX);

    loop {
      if remaining_limit == 0 {
        break;
      }

      let (maybe_cd, additional) = fd.chunk_decompressor::<P::Num>(&bytes[consumed..])?;
      consumed += additional;

      if let Some(mut cd) = maybe_cd {
        let n = cd.n();
        let batch_size = min(n, remaining_limit);
        // TODO this doesn't work for certain batch sizes
        let mut nums = vec![P::Num::default(); batch_size];
        let (_, additional) = cd.decompress(&bytes[consumed..], &mut nums)?;
        consumed += additional;
        writer.write(nums.into_iter().map(P::num_to_native).collect::<Vec<_>>())?;
        remaining_limit -= batch_size;
      } else {
        break;
      }
    }

    writer.close()?;
    Ok(())
  }
}

fn new_column_writer<P: NumberLikeArrow>(opt: &DecompressOpt) -> Result<Box<dyn ColumnWriter<P>>> {
  // eventually we'll likely have a txt writer and a parquet writer, etc.
  Ok(Box::new(StdoutWriter::from_opt(opt)))
}

trait ColumnWriter<P: NumberLikeArrow> {
  fn from_opt(opt: &DecompressOpt) -> Self
  where
    Self: Sized;
  fn write(&mut self, nums: Vec<P::Native>) -> Result<()>;
  fn close(&mut self) -> Result<()>;
}

#[derive(Default)]
struct StdoutWriter<P: NumberLikeArrow> {
  timestamp_format: String,
  phantom: PhantomData<P>,
}

impl<P: NumberLikeArrow> ColumnWriter<P> for StdoutWriter<P> {
  fn from_opt(opt: &DecompressOpt) -> Self {
    Self {
      timestamp_format: opt.timestamp_format.clone(),
      phantom: PhantomData,
    }
  }

  fn write(&mut self, arrow_natives: Vec<P::Native>) -> Result<()> {
    let schema = Schema::new(vec![Field::new("c0", P::DATA_TYPE, false)]);
    let c0 = PrimitiveArray::<P>::from_iter_values(arrow_natives);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c0)])?;
    let mut stdout_bytes = Vec::<u8>::new();
    {
      let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .with_timestamp_format(self.timestamp_format.clone())
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
