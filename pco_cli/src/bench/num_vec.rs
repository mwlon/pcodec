use q_compress::data_types::TimestampMicros;
use std::cmp::min;

use crate::bench::dtypes::Dtype;

pub enum NumVec {
  U32(Vec<u32>),
  I32(Vec<i32>),
  I64(Vec<i64>),
  F32(Vec<f32>),
  F64(Vec<f64>),
  Micros(Vec<TimestampMicros>),
}

fn cast_to_nums<T: Dtype>(bytes: Vec<u8>, limit: Option<usize>) -> Vec<T> {
  // Here we're assuming the bytes are in the right format for our data type.
  // For instance, chunks of 8 little-endian bytes on most platforms for
  // i64's.
  // This is fast and should work across platforms.
  let n = bytes.len() / (T::PHYSICAL_BITS / 8);
  let nums = unsafe {
    let mut nums = std::mem::transmute::<_, Vec<T>>(bytes);
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
  pub fn new(dtype: &str, raw_bytes: Vec<u8>, limit: Option<usize>) -> Self {
    match dtype {
      "u32" => NumVec::U32(cast_to_nums(raw_bytes, limit)),
      "i32" => NumVec::I32(cast_to_nums(raw_bytes, limit)),
      "i64" => NumVec::I64(cast_to_nums(raw_bytes, limit)),
      "f32" => NumVec::F32(cast_to_nums(raw_bytes, limit)),
      "f64" => NumVec::F64(cast_to_nums(raw_bytes, limit)),
      "micros" => NumVec::Micros(cast_to_nums(raw_bytes, limit)),
      _ => panic!("unknown dtype {}", dtype),
    }
  }

  pub fn dtype_str(&self) -> &'static str {
    match self {
      NumVec::U32(_) => "u32",
      NumVec::I32(_) => "i32",
      NumVec::I64(_) => "i64",
      NumVec::F32(_) => "f32",
      NumVec::F64(_) => "f64",
      NumVec::Micros(_) => "micros",
    }
  }
}
