use std::any;

use anyhow::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;

use q_compress::data_types::NumberLike;

use crate::opt::CompressOpt;
use crate::universal_number_like::ArrowLike;

// const AUTO_DELTA_LIMIT: usize = 1000;

pub fn get_header_byte(bytes: &[u8]) -> Result<u8> {
  if bytes.len() >= 5 {
    Ok(bytes[4])
  } else {
    Err(anyhow::anyhow!("only {} bytes found in file", bytes.len()))
  }
}

pub fn arrow_to_vec<T: ArrowLike>(arr: &ArrayRef) -> Vec<T> {
  let primitive = arrow::array::as_primitive_array::<T::ArrowPrimitive>(arr);
  primitive.values().iter()
    .map(|x| T::from_arrow(*x))
    .collect()
}

pub fn find_col_idx(schema: &Schema, opt: &CompressOpt) -> usize {
  match (&opt.col_idx, &opt.col_name) {
    (Some(col_idx), _) => *col_idx,
    (_, Some(col_name)) => schema.fields().iter()
      .position(|f| f.name() == col_name)
      .unwrap(),
    _ => unreachable!()
  }
}

pub fn dtype_name<T: NumberLike>() -> String {
  any::type_name::<T>().split(':').last().unwrap().to_string()
}

// TODO add this in
// pub fn choose_delta_encoding_order<T: NumberLike>(nums: &[T]) -> Result<usize> {
//   let head_nums = &nums[0..min(nums.len(), AUTO_DELTA_LIMIT)];
//   println!(
//     "automatically choosing delta encoding order based on first {} nums (specify --delta-order to skip)",
//     head_nums.len(),
//   );
//   let mut best_order = usize::MAX;
//   let mut best_size = usize::MAX;
//   for delta_encoding_order in 0..8 {
//     let config = CompressorConfig {
//       delta_encoding_order,
//       ..Default::default()
//     };
//     let compressor = Compressor::<T>::from_config(config);
//     let mut writer = BitWriter::default();
//     compressor.chunk(&head_nums, &mut writer)?;
//     let size = writer.byte_size();
//     if size < best_size {
//       best_order = delta_encoding_order;
//       best_size = size;
//     } else {
//       // it's almost always monotonic
//       break;
//     }
//   }
//   Ok(best_order)
// }
