use std::any::type_name;

use anyhow::{anyhow, Result};

use q_compress::{BitReader, Decompressor, PrefixMetadata};
use q_compress::data_types::{NumberLike, TimestampMicros, TimestampNanos};

use crate::opt::InspectOpt;

fn new_boxed_decompressor<T: NumberLike>() -> Box<dyn UnknownDecompressor> {
  Box::new(Decompressor::<T>::default())
}

pub fn new(header_byte: u8) -> Result<Box<dyn UnknownDecompressor>> {
  Ok(match header_byte {
    bool::HEADER_BYTE => new_boxed_decompressor::<bool>(),
    f32::HEADER_BYTE => new_boxed_decompressor::<f32>(),
    f64::HEADER_BYTE => new_boxed_decompressor::<f64>(),
    i32::HEADER_BYTE => new_boxed_decompressor::<i32>(),
    i64::HEADER_BYTE => new_boxed_decompressor::<i64>(),
    i128::HEADER_BYTE => new_boxed_decompressor::<i128>(),
    TimestampMicros::HEADER_BYTE => new_boxed_decompressor::<TimestampMicros>(),
    TimestampNanos::HEADER_BYTE => new_boxed_decompressor::<TimestampNanos>(),
    u32::HEADER_BYTE => new_boxed_decompressor::<u32>(),
    u64::HEADER_BYTE => new_boxed_decompressor::<u64>(),
    _ => {
      return Err(anyhow!("unknown data type byte {}", header_byte));
    }
  })
}

pub trait UnknownDecompressor {
  fn header_byte(&self) -> u8;
  fn inspect(&self, bytes: &[u8], opt: InspectOpt) -> Result<()>;
}

impl<T: NumberLike> UnknownDecompressor for Decompressor<T> {
  fn header_byte(&self) -> u8 {
    T::HEADER_BYTE
  }

  fn inspect(&self, bytes: &[u8], opt: InspectOpt) -> Result<()> {
    println!("inspecting {:?}", opt.path);
    println!("=================\n");

    println!("data type: {}", std::any::type_name::<T>());

    let mut reader = BitReader::from(bytes);
    let flags = self.header(&mut reader)?;
    println!("flags: {:?}", flags);

    let mut metadatas = Vec::new();
    while let Some(meta) = self.chunk_metadata(&mut reader, &flags)? {
      reader.seek(meta.compressed_body_size * 8);
      metadatas.push(meta);
    }

    println!("number of chunks: {}", metadatas.len());
    let total_n: usize = metadatas.iter()
      .map(|m| m.n)
      .sum();
    println!("total n: {}", total_n);
    let uncompressed_size = T::PHYSICAL_BITS / 8 * total_n;
    println!(
      "uncompressed byte size: {} compressed: {} ratio: {}",
      uncompressed_size,
      bytes.len(),
      uncompressed_size as f64 / bytes.len() as f64,
    );

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
      println!("min: {} max: {}", min_num, max_num);
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::unknown_decompressor::new;

  #[test]
  fn test_dtype_bytes_agree() {
    for header_byte in 0..255 {
      if let Ok(decompressor) = new(header_byte) {
        assert_eq!(decompressor.header_byte(), header_byte);
      }
    }
  }
}
