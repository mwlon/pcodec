use std::cmp::min;
use std::mem;

use pco::data_types::CoreDataType;

use crate::dtypes::PcoNumberLike;

pub enum NumVec {
  F32(Vec<f32>),
  F64(Vec<f64>),
  I32(Vec<i32>),
  I64(Vec<i64>),
  U32(Vec<u32>),
  U64(Vec<u64>),
}

fn cast_to_nums<T: PcoNumberLike>(bytes: Vec<u8>, limit: Option<usize>) -> Vec<T> {
  // Here we're assuming the bytes are in the right format for our data type.
  // For instance, chunks of 8 little-endian bytes on most platforms for
  // i64's.
  // This is fast and should work across platforms.
  let n = bytes.len() / mem::size_of::<T>();
  let nums = unsafe {
    let mut nums = mem::transmute::<_, Vec<T>>(bytes);
    nums.set_len(n);
    nums
  };

  if let Some(limit) = limit {
    let limited = min(n, limit);
    if limited < n {
      nums[..limited].to_vec()
    } else {
      nums
    }
  } else {
    nums
  }
}

impl NumVec {
  pub fn new(dtype: CoreDataType, raw_bytes: Vec<u8>, limit: Option<usize>) -> Self {
    use CoreDataType::*;
    match dtype {
      F32 => NumVec::F32(cast_to_nums(raw_bytes, limit)),
      F64 => NumVec::F64(cast_to_nums(raw_bytes, limit)),
      I32 => NumVec::I32(cast_to_nums(raw_bytes, limit)),
      I64 => NumVec::I64(cast_to_nums(raw_bytes, limit)),
      U32 => NumVec::U32(cast_to_nums(raw_bytes, limit)),
      U64 => NumVec::U64(cast_to_nums(raw_bytes, limit)),
    }
  }

  pub fn dtype(&self) -> CoreDataType {
    use CoreDataType::*;
    match self {
      NumVec::F32(_) => F32,
      NumVec::F64(_) => F64,
      NumVec::I32(_) => I32,
      NumVec::I64(_) => I64,
      NumVec::U32(_) => U32,
      NumVec::U64(_) => U64,
    }
  }
}
