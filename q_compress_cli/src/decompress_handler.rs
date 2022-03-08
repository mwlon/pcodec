use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::PrimitiveArray;
use arrow::datatypes::{Field, Schema};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::record_batch::RecordBatch;
use arrow::csv::WriterBuilder as CsvWriterBuilder;

use q_compress::{BitReader, BitWords, Decompressor};

use crate::arrow_number_like::ArrowNumberLike;
use crate::handlers::HandlerImpl;
use crate::opt::DecompressOpt;

pub trait DecompressHandler {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()>;
}

impl<T: ArrowNumberLike> DecompressHandler for HandlerImpl<T> {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()> {
    let decompressor = Decompressor::<T>::default();
    let words = BitWords::from(bytes);
    let mut reader = BitReader::from(&words);
    let flags = decompressor.header(&mut reader)?;

    let mut writer = new_column_writer(opt)?;
    let mut remaining_limit = opt.limit.unwrap_or(usize::MAX);

    loop {
      if remaining_limit == 0 {
        break;
      }

      if let Some(chunk) = decompressor.chunk(&mut reader, &flags)? {
        let nums = chunk.nums;
        let num_slice = if nums.len() <= remaining_limit {
          remaining_limit -= nums.len();
          nums.as_slice()
        } else {
          let res = &nums[0..remaining_limit];
          remaining_limit = 0;
          res
        };
        writer.write(num_slice)?;
      } else {
        break;
      }
    }

    writer.close()?;
    Ok(())
  }
}

fn new_column_writer<T: ArrowNumberLike>(opt: &DecompressOpt) -> Result<Box<dyn ColumnWriter<T>>> {
  // eventually we'll likely have a txt writer and a parquet writer, etc.
  Ok(Box::new(StdoutWriter::from_opt(opt)))
}

trait ColumnWriter<T: ArrowNumberLike> {
  fn from_opt(opt: &DecompressOpt) -> Self where Self: Sized;
  fn write(&mut self, nums: &[T]) -> Result<()>;
  fn close(&mut self) -> Result<()>;
}

#[derive(Default)]
struct StdoutWriter<T: ArrowNumberLike> {
  timestamp_format: String,
  phantom: PhantomData<T>,
}

impl<T: ArrowNumberLike> ColumnWriter<T> for StdoutWriter<T> {
  fn from_opt(opt: &DecompressOpt) -> Self {
    Self {
      timestamp_format: opt.timestamp_format.clone(),
      ..Default::default()
    }
  }

  fn write(&mut self, nums: &[T]) -> Result<()> {
    if T::IS_ARROW {
      let schema = Schema::new(vec![
        Field::new("c0", T::ArrowPrimitive::DATA_TYPE, false)
      ]);
      let arrow_natives = nums.iter()
        .map(|x| x.to_arrow());
      let c0 = PrimitiveArray::<T::ArrowPrimitive>::from_iter_values(
        arrow_natives
      );
      let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(c0)],
      )?;
      let mut stdout_bytes = Vec::<u8>::new();
      {
        let mut writer = CsvWriterBuilder::new()
          .has_headers(false)
          .with_timestamp_format(self.timestamp_format.clone())
          .build(&mut stdout_bytes);
          // &mut stdout_bytes);
        writer.write(&batch)?;
      }
      print!("{}", String::from_utf8(stdout_bytes)?);
    } else {
      for num in nums {
        println!("{}", num);
      }
    }
    Ok(())
  }

  fn close(&mut self) -> Result<()> {
    Ok(())
  }
}
