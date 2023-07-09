use crate::NumberLike;
use q_compress::data_types::TimestampMicros;

pub enum NumVec {
  I64(Vec<i64>),
  F64(Vec<f64>),
  Micros(Vec<TimestampMicros>),
}

fn byte_vec_to_nums<T: NumberLike>(raw_bytes: Vec<u8>) -> Vec<T> {
  let bytes_per_num = T::PHYSICAL_BITS / 8;
  raw_bytes
    .chunks_exact(bytes_per_num)
    .map(|chunk| T::from_bytes(chunk).unwrap())
    .collect::<Vec<_>>()
}

impl NumVec {
  pub fn new(dtype: &str, raw_bytes: Vec<u8>) -> Self {
    match dtype {
      "i64" => NumVec::I64(byte_vec_to_nums(raw_bytes)),
      "f64" => NumVec::F64(byte_vec_to_nums(raw_bytes)),
      "micros" => NumVec::Micros(byte_vec_to_nums(raw_bytes)),
      _ => panic!("unknown dtype {}", dtype),
    }
  }
}
