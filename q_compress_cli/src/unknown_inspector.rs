use std::convert::TryFrom;

use anyhow::Result;

use q_compress::{BitReader, Decompressor, PrefixMetadata};
use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

use crate::dtype::DType;
use crate::opt::InspectOpt;
use crate::utils;

fn new_boxed_inspector<T: NumberLike>() -> Box<dyn UnknownInspector> {
  Box::new(Decompressor::<T>::default())
}

pub fn new(header_byte: u8) -> Result<Box<dyn UnknownInspector>> {
  let dtype = DType::try_from(header_byte)?;
  Ok(match dtype {
    DType::Bool => new_boxed_inspector::<bool>(),
    DType::F32 => new_boxed_inspector::<f32>(),
    DType::F64 => new_boxed_inspector::<f64>(),
    DType::I32 => new_boxed_inspector::<i32>(),
    DType::I64 => new_boxed_inspector::<i64>(),
    DType::I128 => new_boxed_inspector::<i128>(),
    DType::Micros => new_boxed_inspector::<TimestampMicros>(),
    DType::Nanos => new_boxed_inspector::<TimestampNanos>(),
    DType::U32 => new_boxed_inspector::<u32>(),
    DType::U64 => new_boxed_inspector::<u64>(),
  })
}

pub trait UnknownInspector {
  fn header_byte(&self) -> u8;
  fn inspect(&self, bytes: &[u8], opt: InspectOpt) -> Result<()>;
}

// we can't combine this with UnknownDecompressor because they have different
// bounds on `T`
impl<T: NumberLike> UnknownInspector for Decompressor<T> {
  fn header_byte(&self) -> u8 {
    T::HEADER_BYTE
  }

  fn inspect(&self, bytes: &[u8], opt: InspectOpt) -> Result<()> {
    println!("inspecting {:?}", opt.path);

    let mut reader = BitReader::from(bytes);
    let flags = self.header(&mut reader)?;
    println!("=================\n");
    println!("data type: {}", utils::dtype_name::<T>());
    println!("flags: {:?}", flags);
    let header_size = reader.aligned_byte_idx()?;
    let mut metadata_size = 0;

    let mut metadatas = Vec::new();
    let mut start_byte_idx = reader.aligned_byte_idx()?;
    while let Some(meta) = self.chunk_metadata(&mut reader, &flags)? {
      let byte_idx = reader.aligned_byte_idx()?;
      metadata_size += byte_idx - start_byte_idx;

      reader.seek(meta.compressed_body_size * 8);
      metadatas.push(meta);
      start_byte_idx = byte_idx;
    }
    let compressed_size = reader.aligned_byte_idx()?;

    println!("number of chunks: {}", metadatas.len());
    let total_n: usize = metadatas.iter()
      .map(|m| m.n)
      .sum();
    println!("total n: {}", total_n);
    let uncompressed_size = T::PHYSICAL_BITS / 8 * total_n;
    println!("uncompressed byte size: {}", uncompressed_size);
    println!(
      "compressed byte size: {} (ratio: {})",
      compressed_size,
      uncompressed_size as f64 / compressed_size as f64,
    );
    println!("\theader size: {}", header_size);
    println!("\tchunk metadata size: {}", metadata_size);
    println!(
      "\tchunk body size: {}",
      metadatas.iter().map(|m| m.compressed_body_size).sum::<usize>()
    );
    println!("\tfooter size: 1");
    println!("\tunknown trailing bytes: {}", bytes.len() - compressed_size);

    if total_n > 0 && flags.delta_encoding_order == 0 {
      let mut bounds = Vec::new();
      for meta in &metadatas {
        let prefs = match &meta.prefix_metadata {
          PrefixMetadata::Simple { prefixes } => prefixes,
          _ => panic!("expected simple metadata for delta encoding order 0")
        };
        for pref in prefs {
          bounds.push(pref.lower);
          bounds.push(pref.upper);
        }
      }

      let min_num = bounds.iter().min_by_key(|&&x| x.to_unsigned()).cloned().unwrap();
      let max_num = bounds.iter().max_by_key(|&&x| x.to_unsigned()).cloned().unwrap();
      println!("[min, max] numbers: [{}, {}]", min_num, max_num);
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::unknown_inspector::new;

  #[test]
  fn test_dtype_bytes_agree() {
    for header_byte in 0..255 {
      if let Ok(decompressor) = new(header_byte) {
        assert_eq!(decompressor.header_byte(), header_byte);
      }
    }
  }
}
