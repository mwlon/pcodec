use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::bin::WeightedPrefix;
use crate::bits;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct HuffmanItem {
  id: usize,
  weight: usize,
  left_id: Option<usize>,
  right_id: Option<usize>,
  leaf_id: Option<usize>,
  bits: Vec<bool>,
}

impl HuffmanItem {
  pub fn new(weight: usize, id: usize) -> HuffmanItem {
    HuffmanItem {
      id,
      weight,
      left_id: None,
      right_id: None,
      leaf_id: Some(id),
      bits: Vec::new(),
    }
  }

  pub fn new_parent_of(tree0: &HuffmanItem, tree1: &HuffmanItem, id: usize) -> HuffmanItem {
    HuffmanItem {
      id,
      weight: tree0.weight + tree1.weight,
      left_id: Some(tree0.id),
      right_id: Some(tree1.id),
      leaf_id: None,
      bits: Vec::new(),
    }
  }

  pub fn create_bits<U: UnsignedLike>(
    &self,
    item_idx: &mut [HuffmanItem],
    leaf_idx: &mut [WeightedPrefix<U>],
  ) {
    self.create_bits_from(Vec::new(), item_idx, leaf_idx);
  }

  fn create_bits_from<U: UnsignedLike>(
    &self,
    bits: Vec<bool>,
    item_idx: &mut [HuffmanItem],
    leaf_idx: &mut [WeightedPrefix<U>],
  ) {
    item_idx[self.id].bits = bits.clone();
    if self.leaf_id.is_some() {
      let leaf_bin = &mut leaf_idx[self.leaf_id.unwrap()].bin;
      leaf_bin.code = bits::bits_to_usize(&bits);
      leaf_bin.code_len = bits.len() as Bitlen;
    } else {
      let mut left_bits = bits.clone();
      left_bits.push(false);
      let mut right_bits = bits;
      right_bits.push(true);
      item_idx[self.left_id.unwrap()]
        .clone()
        .create_bits_from(left_bits, item_idx, leaf_idx);
      item_idx[self.right_id.unwrap()]
        .clone()
        .create_bits_from(right_bits, item_idx, leaf_idx);
    }
  }
}

impl Ord for HuffmanItem {
  fn cmp(&self, other: &Self) -> Ordering {
    other.weight.cmp(&self.weight) // flipped order to make it a min heap
  }
}

impl PartialOrd for HuffmanItem {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

pub fn make_huffman_code<U: UnsignedLike>(bin_sequence: &mut [WeightedPrefix<U>]) {
  let n = bin_sequence.len();
  let mut heap = BinaryHeap::with_capacity(n); // for figuring out huffman tree
  let mut items = Vec::with_capacity(n); // for modifying item codes
  for (i, bin) in bin_sequence.iter().enumerate() {
    let item = HuffmanItem::new(bin.weight, i);
    heap.push(item.clone());
    items.push(item);
  }

  let mut id = bin_sequence.len();
  for _ in 0..(bin_sequence.len() - 1) {
    let small0 = heap.pop().unwrap();
    let small1 = heap.pop().unwrap();
    let new_item = HuffmanItem::new_parent_of(&small0, &small1, id);
    id += 1;
    heap.push(new_item.clone());
    items.push(new_item);
  }

  let head_node = heap.pop().unwrap();
  head_node.create_bits(&mut items, bin_sequence);
}

#[cfg(test)]
mod tests {
  use crate::bin::{BinCompressionInfo, WeightedPrefix};
  use crate::bits;
  use crate::constants::Bitlen;
  use crate::huffman_encoding::make_huffman_code;

  fn coded_bin(weight: usize, code: Vec<bool>) -> WeightedPrefix<u32> {
    WeightedPrefix {
      weight,
      bin: BinCompressionInfo {
        code: bits::bits_to_usize(&code),
        code_len: code.len() as Bitlen,
        ..Default::default()
      },
    }
  }

  fn uncoded_bin(weight: usize) -> WeightedPrefix<u32> {
    coded_bin(weight, Vec::new())
  }

  #[test]
  fn test_make_huffman_code_single() {
    let mut bin_seq = vec![uncoded_bin(100)];
    make_huffman_code(&mut bin_seq);
    assert_eq!(bin_seq, vec![coded_bin(100, vec![]),]);
  }

  #[test]
  fn test_make_huffman_code() {
    let mut bin_seq = vec![
      uncoded_bin(1),
      uncoded_bin(6),
      uncoded_bin(2),
      uncoded_bin(4),
      uncoded_bin(5),
    ];
    make_huffman_code(&mut bin_seq);
    assert_eq!(
      bin_seq,
      vec![
        coded_bin(1, vec![false, false, false]),
        coded_bin(6, vec![true, true]),
        coded_bin(2, vec![false, false, true]),
        coded_bin(4, vec![false, true]),
        coded_bin(5, vec![true, false]),
      ]
    );
  }
}
