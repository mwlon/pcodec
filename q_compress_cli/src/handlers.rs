use std::convert::TryFrom;
use std::marker::PhantomData;

use anyhow::Result;
use q_compress::data_types::{TimestampMicros, TimestampNanos};
use crate::compress_handler::CompressHandler;

use crate::dtype::DType;
use crate::inspect_handler::InspectHandler;
use crate::arrow_number_like::ArrowNumberLike;

fn new_boxed_handler<T: ArrowNumberLike>() -> Box<dyn Handler> {
  Box::new(HandlerImpl::<T>::default())
}

pub fn from_header_byte(header_byte: u8) -> Result<Box<dyn Handler>> {
  let dtype = DType::try_from(header_byte)?;
  from_dtype(dtype)
}

pub fn from_dtype(dtype: DType) -> Result<Box<dyn Handler>> {
  Ok(match dtype {
    DType::Bool => new_boxed_handler::<bool>(),
    DType::F32 => new_boxed_handler::<f32>(),
    DType::F64 => new_boxed_handler::<f64>(),
    DType::I32 => new_boxed_handler::<i32>(),
    DType::I64 => new_boxed_handler::<i64>(),
    DType::I128 => new_boxed_handler::<i128>(),
    DType::Micros => new_boxed_handler::<TimestampMicros>(),
    DType::Nanos => new_boxed_handler::<TimestampNanos>(),
    DType::U32 => new_boxed_handler::<u32>(),
    DType::U64 => new_boxed_handler::<u64>(),
  })
}

pub trait Handler: InspectHandler + CompressHandler {}

#[derive(Clone, Debug, Default)]
pub struct HandlerImpl<T> {
  phantom: PhantomData<T>,
}

impl<T: ArrowNumberLike> Handler for HandlerImpl<T> {}
