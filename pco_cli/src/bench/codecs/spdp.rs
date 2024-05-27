use std::cmp::{max, min};
use std::convert::TryInto;
use std::pin::Pin;

use anyhow::{anyhow, Result};
use zstd::zstd_safe::WriteBuf;

use crate::bench::codecs::{utils, CodecInternal};
use crate::dtypes::PcoNumberLike;

#[derive(Clone, Debug)]
pub struct SpdpConfig {
  level: u8,
}

impl Default for SpdpConfig {
  fn default() -> Self {
    Self { level: 5 }
  }
}

impl CodecInternal for SpdpConfig {
  fn name(&self) -> &'static str {
    "spdp"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![("level", self.level.to_string())]
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    match key {
      "level" => {
        let level = value.parse::<u8>().unwrap();
        if level <= 9 {
          self.level = level;
        } else {
          return Err(anyhow!("SPDP max compression level is 9"));
        }
      }
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    println!("C START");
    let mut dst = Vec::new();
    dst.push(self.level);
    dst.extend((nums.len() as u32).to_le_bytes());
    let mut src = unsafe { utils::num_slice_to_bytes(nums) };
    while !src.is_empty() {
      // SPDP modifies the input buffer, so we copy the batch
      let src_batch_length = min(src.len(), 1 << 23);
      let mut src_batch = src[..src_batch_length].to_vec();

      // write uncompressed size and a placeholder for compressed size,
      // to be filled in later
      dst.extend((src_batch_length as u32).to_le_bytes());
      dst.extend(&[0; 4]);

      let dst_buffer_size = 2 * src_batch_length + 9;
      dst.reserve(dst_buffer_size);

      let pos = dst.len();
      let old_src = src_batch.as_mut_ptr();
      let old_dst = (&mut dst[pos..]).as_mut_ptr();
      println!("\nC {:?} {:?}", old_src, old_dst);
      assert!(dst.capacity() >= pos + 2 * src_batch_length + 9);
      unsafe {
        let csize = spdp_sys::spdp_compress_batch(
          self.level,
          src_batch_length,
          src_batch.as_mut_ptr(),
          (&mut dst[pos..]).as_mut_ptr(),
        );
        dst[pos - 4..pos].copy_from_slice(&(csize as u32).to_le_bytes());
        dst.set_len(pos + csize);
      };
      let new_src = src_batch.as_mut_ptr();
      let new_dst = (&mut dst[pos..]).as_mut_ptr();
      if new_src != old_src {
        println!("SRC MOVED!!!!!");
      }
      if new_dst != old_dst {
        println!("DST MOVED!!!!");
      }
      println!("{:?} {:?}", new_src, new_dst);
      src = &src[src_batch_length..];
    }
    println!("C END");
    dst
  }

  fn decompress<T: PcoNumberLike>(&self, mut src: &[u8]) -> Vec<T> {
    println!("D START");
    let level = src[0];
    let total_count = u32::from_le_bytes(src[1..5].try_into().unwrap()) as usize;
    src = &src[5..];
    // Empirically, we need to add some extra buffer room at the end of the
    // decompressed buffer to avoid segfaults. I haven't looked at their source
    // code enough to know exactly how much is necessary.
    let mut dst = Vec::with_capacity(2 * total_count + 9);
    let mut dst_bytes = unsafe {
      dst.set_len(total_count);
      utils::num_slice_to_bytes_mut(&mut dst)
    };

    while !src.is_empty() {
      let dst_batch_length = u32::from_le_bytes(src[..4].try_into().unwrap()) as usize;
      let csize = u32::from_le_bytes(src[4..8].try_into().unwrap()) as usize;
      src = &src[8..];
      // assert!(dst_bytes.capacity() >= dst_batch_length + 4);
      // assert!(dst_bytes.capacity() >= csize + 4);
      // SPDP modifies the input buffer, so we copy the batch
      let mut src_batch = vec![0; max(csize, dst_batch_length)];
      src_batch[..csize].copy_from_slice(&src[..csize]);
      // let mut src_batch = src
      //   .iter()
      //   .take(csize)
      //   .cloned()
      //   .chain([0; 64].into_iter())
      //   .collect::<Vec<_>>();
      // assert!(src_batch.capacity() >= csize + 4);
      // let mut src_batch = src[..csize].to_vec();
      let old_src = src_batch.as_mut_ptr();
      let old_dst = dst_bytes.as_mut_ptr();
      println!(
        "\nD {:?} {:?} {} {}, {} {}",
        old_src,
        old_dst,
        csize,
        dst_batch_length,
        src_batch.len(),
        dst_bytes.len()
      );
      let decompressed = unsafe {
        spdp_sys::spdp_decompress_batch(
          level,
          csize,
          src_batch.as_mut_ptr(),
          dst_bytes.as_mut_ptr(),
        )
      };
      println!("  decompressed {} bytes", decompressed);
      let new_src = src_batch.as_mut_ptr();
      let new_dst = dst_bytes.as_mut_ptr();
      if new_src != old_src {
        println!("SRC MOVED!!!!!");
      }
      if new_dst != old_dst {
        println!("DST MOVED!!!!");
      }
      println!("  {:?} {:?}", new_src, new_dst);
      src = &src[csize..];
      dst_bytes = &mut dst_bytes[dst_batch_length..];
    }
    println!("D END");
    dst
  }
}
