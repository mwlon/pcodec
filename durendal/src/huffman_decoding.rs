use std::cmp::min;

use crate::bin::{Bin, BinDecompressionInfo};
use crate::bit_reader::BitReader;
use crate::constants::{Bitlen, MAX_BIN_TABLE_SIZE_LOG};
use crate::data_types::UnsignedLike;
use crate::errors::{QCompressError, QCompressResult};
use crate::modes::{Mode, ModeBin};

#[derive(Clone, Debug)]
pub enum HuffmanTable<B: ModeBin> {
  Leaf(BinDecompressionInfo<B>),
  NonLeaf {
    table_size_log: Bitlen,
    children: Vec<HuffmanTable<B>>,
  },
}

impl<B: ModeBin> Default for HuffmanTable<B> {
  fn default() -> Self {
    HuffmanTable::Leaf(BinDecompressionInfo::default())
  }
}

impl<B: ModeBin> HuffmanTable<B> {
  pub fn search_with_reader(
    &self,
    reader: &mut BitReader,
  ) -> QCompressResult<&BinDecompressionInfo<B>> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind_bin_overshoot(read_depth - decompression_info.depth);
          return Ok(decompression_info);
        }
        HuffmanTable::NonLeaf {
          table_size_log,
          children,
        } => {
          let (bits_read, idx) = reader.read_bin_table_idx(*table_size_log)?;
          read_depth += bits_read;
          node = &children[idx];
          if bits_read != *table_size_log {
            return match node {
              HuffmanTable::Leaf(decompression_info) if decompression_info.depth == read_depth => {
                Ok(decompression_info)
              }
              HuffmanTable::Leaf(_) => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman bin (reached leaf)",
              )),
              HuffmanTable::NonLeaf {
                table_size_log: _,
                children: _,
              } => Err(QCompressError::insufficient_data(
                "search_with_reader(): ran out of data parsing Huffman bin (reached parent)",
              )),
            };
          }
        }
      }
    }
  }

  pub fn unchecked_search_with_reader(&self, reader: &mut BitReader) -> &BinDecompressionInfo<B> {
    let mut node = self;
    let mut read_depth = 0;
    loop {
      match node {
        HuffmanTable::Leaf(decompression_info) => {
          reader.rewind_bin_overshoot(read_depth - decompression_info.depth);
          return decompression_info;
        }
        HuffmanTable::NonLeaf {
          table_size_log,
          children,
        } => {
          let idx = reader.unchecked_read_bin_table_idx(*table_size_log);
          node = &children[idx];
          read_depth += table_size_log;
        }
      }
    }
  }

  pub fn from_bins<U: UnsignedLike, M: Mode<U, Bin = B>>(bins: &[Bin<U>]) -> Self {
    if bins.is_empty() {
      HuffmanTable::default()
    } else {
      build_from_bins_recursive::<U, M>(bins, 0)
    }
  }
}

fn build_from_bins_recursive<U: UnsignedLike, M: Mode<U>>(
  bins: &[Bin<U>],
  depth: Bitlen,
) -> HuffmanTable<M::Bin> {
  if bins.len() == 1 {
    let bin = &bins[0];
    HuffmanTable::Leaf(M::make_decompression_info(bin))
  } else {
    let max_depth = bins.iter().map(|bin| bin.code_len).max().unwrap();
    let table_size_log = min(MAX_BIN_TABLE_SIZE_LOG, max_depth - depth);
    let final_depth = depth + table_size_log;
    let table_size = 1 << table_size_log;

    // We put each bin into the table, possibly in multiple consecutive locations.
    // e.g. if the table size log is 7 and we have a 4-bit code, we'll put the bin in
    // 2^3=8 table indexes.
    let mut child_infos = vec![Vec::new(); table_size];
    for bin in bins {
      let base_idx = (bin.code >> depth) % table_size;
      let n_idxs = 1 << final_depth.saturating_sub(bin.code_len);
      let idx_stride = 1 << bin.code_len.saturating_sub(depth);
      for i in 0..n_idxs {
        let idx = base_idx + i * idx_stride;
        child_infos[idx].push(*bin);
      }
    }
    let children = child_infos
      .into_iter()
      .map(|bins| build_from_bins_recursive::<U, M>(&bins, final_depth))
      .collect();

    HuffmanTable::NonLeaf {
      table_size_log,
      children,
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::modes::gcd::GcdBin;
  use super::*;

  #[test]
  fn huff_table_size() {
    assert_eq!(
      std::mem::size_of::<HuffmanTable<GcdBin<u64>>>(),
      48
    );
    assert_eq!(
      std::mem::size_of::<HuffmanTable<GcdBin<u32>>>(),
      32
    );
  }
}
