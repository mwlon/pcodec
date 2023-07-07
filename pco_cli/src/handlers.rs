use std::convert::TryFrom;
use std::marker::PhantomData;

use anyhow::Result;
use arrow::datatypes::{
  Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
  TimestampNanosecondType, UInt16Type, UInt32Type, UInt64Type,
};

use crate::compress_handler::CompressHandler;
use crate::decompress_handler::DecompressHandler;
use crate::dtype::DType;
use crate::inspect_handler::InspectHandler;
use crate::number_like_arrow::NumberLikeArrow;

fn new_boxed_handler<P: NumberLikeArrow>() -> Box<dyn Handler> {
  Box::new(HandlerImpl {
    phantom: PhantomData::<P>,
  })
}

pub fn from_header_byte(header_byte: u8) -> Result<Box<dyn Handler>> {
  let dtype = DType::try_from(header_byte)?;
  Ok(from_dtype(dtype))
}

pub fn from_dtype(dtype: DType) -> Box<dyn Handler> {
  match dtype {
    DType::F32 => new_boxed_handler::<Float32Type>(),
    DType::F64 => new_boxed_handler::<Float64Type>(),
    DType::I16 => new_boxed_handler::<Int16Type>(),
    DType::I32 => new_boxed_handler::<Int32Type>(),
    DType::I64 => new_boxed_handler::<Int64Type>(),
    DType::TimestampMicros => new_boxed_handler::<TimestampMicrosecondType>(),
    DType::TimestampNanos => new_boxed_handler::<TimestampNanosecondType>(),
    DType::U16 => new_boxed_handler::<UInt16Type>(),
    DType::U32 => new_boxed_handler::<UInt32Type>(),
    DType::U64 => new_boxed_handler::<UInt64Type>(),
  }
}

pub trait Handler: CompressHandler + DecompressHandler + InspectHandler {}

#[derive(Clone, Debug, Default)]
pub struct HandlerImpl<P> {
  phantom: PhantomData<P>,
}

impl<P: NumberLikeArrow> Handler for HandlerImpl<P> {}
