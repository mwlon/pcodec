use std::mem;

use parquet::data_type as parq;

use pco::data_types::NumberLike as PNumberLike;
use q_compress::data_types::{NumberLike as QNumberLike, TimestampMicros};
use crate::num_vec::NumVec;

pub trait Dtype: QNumberLike {
  type Pco: PNumberLike;
  type Parquet: parquet::data_type::DataType;

  const PARQUET_DTYPE_STR: &'static str;

  fn num_vec(nums: Vec<Self>) -> NumVec;
  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T];
  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco];
  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self>;
  fn vec_from_parquet(v: Vec<<Self::Parquet as parq::DataType>::T>) -> Vec<Self>;
}

// This is technically not correct for parquet.
// Parquet only has signed ints; here we just transmute between them.
impl Dtype for u32 {
  type Pco = u32;
  type Parquet = parq::Int32Type;

  const PARQUET_DTYPE_STR: &'static str = "INT32";

  fn num_vec(nums: Vec<Self>) -> NumVec {
    NumVec::U32(nums)
  }

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    unsafe { mem::transmute(slice) }
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    slice
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }

  fn vec_from_parquet(v: Vec<i32>) -> Vec<Self> {
    unsafe { mem::transmute(v) }
  }
}

impl Dtype for i64 {
  type Pco = i64;
  type Parquet = parq::Int64Type;

  const PARQUET_DTYPE_STR: &'static str = "INT64";

  fn num_vec(nums: Vec<Self>) -> NumVec {
    NumVec::I64(nums)
  }

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    slice
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    slice
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }

  fn vec_from_parquet(v: Vec<Self>) -> Vec<Self> {
    v
  }
}

impl Dtype for f64 {
  type Pco = f64;
  type Parquet = parq::DoubleType;

  const PARQUET_DTYPE_STR: &'static str = "DOUBLE";

  fn num_vec(nums: Vec<Self>) -> NumVec {
    NumVec::F64(nums)
  }

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    slice
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    slice
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }

  fn vec_from_parquet(v: Vec<Self>) -> Vec<Self> {
    v
  }
}

impl Dtype for TimestampMicros {
  type Pco = i64;
  type Parquet = parq::Int64Type;

  const PARQUET_DTYPE_STR: &'static str = "INT64";

  fn num_vec(nums: Vec<Self>) -> NumVec {
    NumVec::I64(unsafe { mem::transmute(nums) })
  }

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    unsafe { mem::transmute(slice) }
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    unsafe { mem::transmute(slice) }
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    unsafe { mem::transmute(v) }
  }

  fn vec_from_parquet(v: Vec<i64>) -> Vec<Self> {
    unsafe { mem::transmute(v) }
  }
}
