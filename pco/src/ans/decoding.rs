use crate::ans::spec::Spec;
use crate::ans::{AnsState, Symbol};
use crate::constants::Bitlen;

#[derive(Clone, Debug)]
#[repr(align(16))]
pub struct Node {
  pub symbol: Symbol,
  pub next_state_idx_base: AnsState,
  pub bits_to_read: Bitlen,
}

#[derive(Clone, Debug)]
pub struct Decoder {
  pub nodes: Vec<Node>,
}

impl Decoder {
  pub fn new(spec: &Spec) -> Self {
    let table_size = spec.table_size();
    let mut nodes = Vec::with_capacity(table_size);
    // x_s from Jarek Duda's paper
    let mut symbol_x_s = spec.symbol_weights.clone();
    for &symbol in &spec.state_symbols {
      let mut next_state_base = symbol_x_s[symbol as usize] as AnsState;
      let mut bits_to_read = 0;
      while next_state_base < table_size as AnsState {
        next_state_base *= 2;
        bits_to_read += 1;
      }
      nodes.push(Node {
        symbol,
        next_state_idx_base: next_state_base - table_size as AnsState,
        bits_to_read,
      });
      symbol_x_s[symbol as usize] += 1;
    }

    Self { nodes }
  }
}
