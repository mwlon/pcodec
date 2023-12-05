use std::any;

use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;

use pco::data_types::NumberLike;
use pco::standalone::FileDecompressor;

use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::CompressOpt;

pub fn get_header_byte(src: &[u8]) -> Result<u8> {
  let (_, src) = FileDecompressor::new(src)?;
  if src.is_empty() {
    Err(anyhow::anyhow!(
      "file too short to identify dtype"
    ))
  } else {
    Ok(src[0])
  }
}

pub fn arrow_to_vec<P: NumberLikeArrow>(arr: &ArrayRef) -> Vec<P::Num> {
  let primitive = arrow::array::as_primitive_array::<P>(arr);
  primitive
    .values()
    .iter()
    .map(|&x| P::native_to_num(x))
    .collect()
}

pub fn find_col_idx(schema: &Schema, opt: &CompressOpt) -> usize {
  match (&opt.col_idx, &opt.col_name) {
    (Some(col_idx), _) => *col_idx,
    (_, Some(col_name)) => schema
      .fields()
      .iter()
      .position(|f| f.name() == col_name)
      .unwrap(),
    _ => unreachable!(),
  }
}

pub fn dtype_name<T: NumberLike>() -> String {
  any::type_name::<T>().split(':').last().unwrap().to_string()
}
