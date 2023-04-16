use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::data_types::NumberLike;
use crate::{Prefix, run_len_utils};

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

  pub fn create_bits<T: NumberLike>(
    &self,
    item_idx: &mut [HuffmanItem],
    leaf_idx: &mut [Prefix<T>],
  ) {
    self.create_bits_from(Vec::new(), item_idx, leaf_idx);
  }

  fn create_bits_from<T: NumberLike>(
    &self,
    bits: Vec<bool>,
    item_idx: &mut [HuffmanItem],
    leaf_idx: &mut [Prefix<T>],
  ) {
    item_idx[self.id].bits = bits.clone();
    if self.leaf_id.is_some() {
      leaf_idx[self.leaf_id.unwrap()].code = bits;
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

pub fn make_huffman_code<T: NumberLike>(prefixes: &mut [Prefix<T>], n: usize) {
  let n_pref = prefixes.len();
  let mut heap = BinaryHeap::with_capacity(n_pref); // for figuring out huffman tree
  let mut items = Vec::with_capacity(n_pref); // for modifying item codes
  for (i, prefix) in prefixes.iter().enumerate() {
    let weight = run_len_utils::run_len_weight(prefix.count, n);
    let item = HuffmanItem::new(weight, i);
    heap.push(item.clone());
    items.push(item);
  }

  let mut id = prefixes.len();
  for _ in 0..(prefixes.len() - 1) {
    let small0 = heap.pop().unwrap();
    let small1 = heap.pop().unwrap();
    let new_item = HuffmanItem::new_parent_of(&small0, &small1, id);
    id += 1;
    heap.push(new_item.clone());
    items.push(new_item);
  }

  let head_node = heap.pop().unwrap();
  head_node.create_bits(&mut items, prefixes);
}

#[cfg(test)]
mod tests {
  use crate::huffman_encoding::make_huffman_code;
  use crate::prefix::{Prefix};

  fn coded_prefix(weight: usize, code: Vec<bool>) -> Prefix<i32> {
    Prefix {
      count: weight,
      code,
      lower: 0,
      upper: 0,
      run_len_jumpstart: None,
      gcd: 1,
    }
  }

  fn uncoded_prefix(weight: usize) -> Prefix<i32> {
    coded_prefix(weight, Vec::new())
  }

  #[test]
  fn test_make_huffman_code_single() {
    let mut prefix_seq = vec![uncoded_prefix(100)];
    make_huffman_code(&mut prefix_seq, 100);
    assert_eq!(prefix_seq, vec![coded_prefix(100, vec![]),]);
  }

  #[test]
  fn test_make_huffman_code() {
    let mut prefix_seq = vec![
      uncoded_prefix(1),
      uncoded_prefix(6),
      uncoded_prefix(2),
      uncoded_prefix(4),
      uncoded_prefix(5),
    ];
    make_huffman_code(&mut prefix_seq, 18);
    assert_eq!(
      prefix_seq,
      vec![
        coded_prefix(1, vec![false, false, false]),
        coded_prefix(6, vec![true, true]),
        coded_prefix(2, vec![false, false, true]),
        coded_prefix(4, vec![false, true]),
        coded_prefix(5, vec![true, false]),
      ]
    );
  }
}
