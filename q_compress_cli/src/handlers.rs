use std::convert::TryFrom;
use std::marker::PhantomData;

use anyhow::Result;

use q_compress::data_types::{TimestampMicros, TimestampNanos, TimestampMicros96, TimestampNanos96};

use crate::arrow_number_like::ArrowNumberLike;
use crate::compress_handler::CompressHandler;
use crate::decompress_handler::DecompressHandler;
use crate::dtype::DType;
use crate::inspect_handler::InspectHandler;

fn new_boxed_handler<T: ArrowNumberLike>() -> Box<dyn Handler> {
  Box::new(HandlerImpl::<T>::default())
}

pub fn from_header_byte(header_byte: u8) -> Result<Box<dyn Handler>> {
  let dtype = DType::try_from(header_byte)?;
  Ok(from_dtype(dtype))
}

pub fn from_dtype(dtype: DType) -> Box<dyn Handler> {
  match dtype {
    DType::Bool => new_boxed_handler::<bool>(),
    DType::F32 => new_boxed_handler::<f32>(),
    DType::F64 => new_boxed_handler::<f64>(),
    DType::I16 => new_boxed_handler::<i16>(),
    DType::I32 => new_boxed_handler::<i32>(),
    DType::I64 => new_boxed_handler::<i64>(),
    DType::I128 => new_boxed_handler::<i128>(),
    DType::TimestampMicros => new_boxed_handler::<TimestampMicros>(),
    DType::TimestampMicros96 => new_boxed_handler::<TimestampMicros96>(),
    DType::TimestampNanos => new_boxed_handler::<TimestampNanos>(),
    DType::TimestampNanos96 => new_boxed_handler::<TimestampNanos96>(),
    DType::U16 => new_boxed_handler::<u16>(),
    DType::U32 => new_boxed_handler::<u32>(),
    DType::U64 => new_boxed_handler::<u64>(),
    DType::U128 => new_boxed_handler::<u128>(),
  }
}

pub trait Handler: CompressHandler + DecompressHandler + InspectHandler {}

#[derive(Clone, Debug, Default)]
pub struct HandlerImpl<T> {
  phantom: PhantomData<T>,
}

impl<T: ArrowNumberLike> Handler for HandlerImpl<T> {}

#[cfg(test)]
mod tests {
  use std::collections::HashSet;
  use anyhow::Result;
  use enum_iterator::IntoEnumIterator;

  use crate::dtype::DType;

  #[test]
  fn test_handlers_have_distinct_header_bytes() -> Result<()> {
    let mut bytes_seen = HashSet::new();
    for dtype in DType::into_enum_iter() {
      let handler = super::from_dtype(dtype);
      let byte = handler.header_byte();
      if bytes_seen.contains(&byte) {
        panic!("saw multiple dtype handlers with header byte {}", byte);
      }
      bytes_seen.insert(byte);
    }
    Ok(())
  }
}
