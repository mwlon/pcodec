use crate::bits::bits_to_string;
use crate::prefix::PrefixIntermediate;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct HuffmanItem {
  id: usize,
  weight: u64,
  left_id: Option<usize>,
  right_id: Option<usize>,
  leaf_id: Option<usize>,
  bits: Vec<bool>,
}

impl HuffmanItem {
  pub fn new(weight: u64, id: usize) -> HuffmanItem {
    HuffmanItem {
      id,
      weight,
      left_id: None,
      right_id: None,
      leaf_id: Some(id),
      bits: Vec::new(),
    }
  }

  pub fn parent_of(tree0: &HuffmanItem, tree1: &HuffmanItem, id: usize) -> HuffmanItem {
    HuffmanItem {
      id,
      weight: tree0.weight + tree1.weight,
      left_id: Some(tree0.id),
      right_id: Some(tree1.id),
      leaf_id: None,
      bits: Vec::new(),
    }
  }

  pub fn create_bits<T>(&self, item_index: &mut Vec<HuffmanItem>, leaf_index: &mut Vec<PrefixIntermediate<T>>) {
    self.create_bits_from(Vec::new(), item_index, leaf_index);
  }

  fn create_bits_from<T>(
    &self,
    bits: Vec<bool>,
    item_index: &mut Vec<HuffmanItem>,
    leaf_index: &mut Vec<PrefixIntermediate<T>>,
  ) {
    item_index[self.id].bits = bits.clone();
    if self.leaf_id.is_some() {
      leaf_index[self.leaf_id.unwrap()].val = bits;
    } else {
      let mut left_bits = bits.clone();
      left_bits.push(false);
      let mut right_bits = bits;
      right_bits.push(true);
      item_index[self.left_id.unwrap()].clone().create_bits_from(left_bits, item_index, leaf_index);
      item_index[self.right_id.unwrap()].clone().create_bits_from(right_bits, item_index, leaf_index);
    }
  }

  pub fn to_string(&self, item_index: &[HuffmanItem]) -> String {
    self.to_string_indented(0, item_index)
  }

  fn to_string_indented(&self, indent: usize, item_index: &[HuffmanItem]) -> String {
    let self_string = format!(
      "{}code={} w={}",
      "  ".repeat(indent),
      bits_to_string(&self.bits),
      self.weight,
    );

    if self.leaf_id.is_some() {
      format!("{}**", self_string)
    } else {
      let next_ind = indent + 1;
      format!(
        "{}\n{}\n{}",
        self_string,
        item_index[self.left_id.unwrap()].to_string_indented(next_ind, item_index),
        item_index[self.right_id.unwrap()].to_string_indented(next_ind, item_index),
      )
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

pub fn make_huffman_code<T>(prefix_sequence: &mut Vec<PrefixIntermediate<T>>) {
  let n = prefix_sequence.len();
  let mut heap = BinaryHeap::with_capacity(n); // for figuring out huffman tree
  let mut items = Vec::with_capacity(n); // for modifying item codes
  for (i, prefix) in prefix_sequence.iter().enumerate() {
    let item = HuffmanItem::new(prefix.weight, i);
    heap.push(item.clone());
    items.push(item);
  }

  let mut id = prefix_sequence.len();
  for _ in 0..(prefix_sequence.len() - 1) {
    let small0 = heap.pop().unwrap();
    let small1 = heap.pop().unwrap();
    let new_item = HuffmanItem::parent_of(&small0, &small1, id);
    id += 1;
    heap.push(new_item.clone());
    items.push(new_item);
  }

  let head_node = heap.pop().unwrap();
  head_node.create_bits(&mut items, prefix_sequence);
}

