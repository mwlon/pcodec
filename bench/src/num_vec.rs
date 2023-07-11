use crate::dtypes::Dtype;
use q_compress::data_types::TimestampMicros;

pub enum NumVec {
  I64(Vec<i64>),
  F64(Vec<f64>),
  Micros(Vec<TimestampMicros>),
}

fn cast_to_nums<T: Dtype>(bytes: Vec<u8>) -> Vec<T> {
  // Here we're assuming the bytes are in the right format for our data type.
  // For instance, chunks of 8 little-endian bytes on most platforms for
  // i64's.
  // This is fast and should work across platforms.
  let n = bytes.len() / (T::PHYSICAL_BITS / 8);
  unsafe {
    let mut nums = std::mem::transmute::<_, Vec<T>>(bytes);
    nums.set_len(n);
    nums
  }
}

impl NumVec {
  pub fn new(dtype: &str, raw_bytes: Vec<u8>) -> Self {
    match dtype {
      "i64" => NumVec::I64(cast_to_nums(raw_bytes)),
      "f64" => NumVec::F64(cast_to_nums(raw_bytes)),
      "micros" => NumVec::Micros(cast_to_nums(raw_bytes)),
      _ => panic!("unknown dtype {}", dtype),
    }
  }
}
