use std::any;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::Schema;

use pco::data_types::{CoreDataType, NumberLike};
use pco::standalone::FileDecompressor;

use crate::dtypes::ArrowNumberLike;

pub fn get_standalone_dtype(initial_bytes: &[u8]) -> Result<Option<CoreDataType>> {
  let (fd, src) = FileDecompressor::new(initial_bytes)?;

  use pco::standalone::DataTypeOrTermination::*;
  match fd.peek_dtype_or_termination(src)? {
    Termination => Ok(None),
    Known(dtype) => Ok(Some(dtype)),
    Unknown(byte) => Err(anyhow!("unknown dtype byte: {}", byte)),
  }
}

pub fn find_col_idx(
  schema: &Schema,
  col_idx: Option<usize>,
  col_name: &Option<String>,
) -> Result<usize> {
  let col_idx = match (col_idx, col_name) {
    (Some(col_idx), _) => col_idx,
    (_, Some(col_name)) => schema
      .fields()
      .iter()
      .position(|f| f.name() == col_name)
      .ok_or_else(|| {
        anyhow!(
          "Could not find column {}. Existing columns: {:?}",
          col_name,
          schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>()
        )
      })?,
    _ => {
      if schema.fields.len() == 1 {
        0
      } else {
        return Err(anyhow!(
          "incomplete or incompatible col name and col idx"
        ));
      }
    }
  };
  Ok(col_idx)
}

pub fn dtype_name<T: NumberLike>() -> String {
  any::type_name::<T>().split(':').last().unwrap().to_string()
}

pub fn arrow_to_nums<P: ArrowNumberLike>(arrow_array: ArrayRef) -> Vec<P::Pco> {
  arrow_array
    .as_primitive::<P>()
    .values()
    .iter()
    .map(|&x| P::native_to_pco(x))
    .collect()
}
