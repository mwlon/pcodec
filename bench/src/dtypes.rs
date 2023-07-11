use std::mem;
use pco::data_types::NumberLike as PNumberLike;
use q_compress::data_types::{NumberLike as QNumberLike, TimestampMicros};
use parquet::data_type as parq;

pub fn dtype_str(dataset: &str) -> &str {
  dataset.split('_').next().unwrap()
}

pub trait Dtype: QNumberLike {
  type Pco: PNumberLike;
  type Parquet: parquet::data_type::DataType;

  const PARQUET_DTYPE_STR: &'static str;

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T];
  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco];
  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self>;
  fn vec_from_parquet(v: Vec<<Self::Parquet as parq::DataType>::T>) -> Vec<Self>;
}

impl Dtype for i64 {
  type Pco = i64;
  type Parquet = parq::Int64Type;

  const PARQUET_DTYPE_STR: &'static str = "INT64";

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    slice
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    slice
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }

  fn vec_from_parquet(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }
}

impl Dtype for f64 {
  type Pco = f64;
  type Parquet = parq::DoubleType;

  const PARQUET_DTYPE_STR: &'static str = "DOUBLE";

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    slice
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    slice
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }

  fn vec_from_parquet(v: Vec<Self::Pco>) -> Vec<Self> {
    v
  }
}

impl Dtype for TimestampMicros {
  type Pco = i64;
  type Parquet = parq::Int64Type;

  const PARQUET_DTYPE_STR: &'static str = "INT64";

  fn slice_to_parquet(slice: &[Self]) -> &[<Self::Parquet as parq::DataType>::T] {
    unsafe { mem::transmute(slice) }
  }

  fn slice_to_pco(slice: &[Self]) -> &[Self::Pco] {
    unsafe { mem::transmute(slice) }
  }

  fn vec_from_pco(v: Vec<Self::Pco>) -> Vec<Self> {
    unsafe { mem::transmute(v) }
  }

  fn vec_from_parquet(v: Vec<Self::Pco>) -> Vec<Self> {
    unsafe { mem::transmute(v) }
  }
}
