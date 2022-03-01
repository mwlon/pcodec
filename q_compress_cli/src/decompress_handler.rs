use std::marker::PhantomData;
use std::path::Path;

use anyhow::{anyhow, Result};

use q_compress::{BitReader, Decompressor};

use crate::arrow_number_like::ArrowNumberLike;
use crate::handlers::HandlerImpl;
use crate::opt::DecompressOpt;

pub trait DecompressHandler {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()>;
}

impl<T: ArrowNumberLike> DecompressHandler for HandlerImpl<T> {
  fn decompress(&self, opt: &DecompressOpt, bytes: &[u8]) -> Result<()> {
    let decompressor = Decompressor::<T>::default();
    let mut reader = BitReader::from(bytes);
    let flags = decompressor.header(&mut reader)?;
    let mut remaining = if let Some(limit) = opt.limit {
      limit
    } else {
      usize::MAX
    };

    let mut writer = new_column_writer(opt)?;

    loop {
      if remaining == 0 {
        break;
      }

      if let Some(chunk) = decompressor.chunk(&mut reader, &flags)? {
        let nums = chunk.nums;
        let num_slice = if nums.len() <= remaining {
          remaining -= nums.len();
          nums.as_slice()
        } else {
          let res = &nums[0..remaining];
          remaining = 0;
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

fn new_column_writer<T: ArrowNumberLike>(opt: &DecompressOpt) -> Result<Box<dyn ColumnWriter<T>>>{
  match (&opt.use_stdout, &opt.txt_path) {
    (true, None) => Ok(Box::new(StdoutWriter::default())),
    (false, Some(txt_path)) => Ok(Box::new(TxtWriter::new(opt, txt_path)?)),
    _ => Err(anyhow!("missing or incomplete input options")),
  }
}

trait ColumnWriter<T: ArrowNumberLike> {
  fn write(&mut self, nums: &[T]) -> Result<()>;
  fn close(&mut self) -> Result<()>;
}

#[derive(Default)]
struct StdoutWriter<T: ArrowNumberLike> {
  phantom: PhantomData<T>,
}

impl<T: ArrowNumberLike> ColumnWriter<T> for StdoutWriter<T> {
  fn write(&mut self, nums: &[T]) -> Result<()> {
    for num in nums {
      println!("{}", num);
    }
    Ok(())
  }

  fn close(&mut self) -> Result<()> {
    Ok(())
  }
}

#[derive(Default)]
struct TxtWriter<T: ArrowNumberLike> {
  phantom: PhantomData<T>,
}

impl<T: ArrowNumberLike> TxtWriter<T> {
  fn new(opt: &DecompressOpt, path: &Path) -> Result<Self> {
    Ok(Self::default())
  }
}

impl<T: ArrowNumberLike> ColumnWriter<T> for TxtWriter<T> {
  fn write(&mut self, nums: &[T]) -> Result<()> {
    for num in nums {
      println!("{}", num);
    }
    Ok(())
  }

  fn close(&mut self) -> Result<()> {
    Ok(())
  }
}
