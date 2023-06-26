use std::any;

use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;

use durendal::data_types::NumberLike;

use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::CompressOpt;

pub fn get_header_byte(bytes: &[u8]) -> Result<u8> {
  if bytes.len() >= 5 {
    Ok(bytes[4])
  } else {
    Err(anyhow::anyhow!(
      "only {} bytes found in file",
      bytes.len()
    ))
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
