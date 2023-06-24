use crate::ans::spec::{AnsSpec, Token};
use crate::Bin;
use crate::bit_reader::BitReader;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;

#[derive(Clone, Debug)]
struct Node {
  token: Token,
  next_state_base: usize,
  bits_to_read: Bitlen,
}

#[derive(Clone, Debug)]
pub struct AnsDecoder {
  table_size: usize,
  nodes: Vec<Node>,
  pub state: usize,
}

impl AnsDecoder {
  pub fn new(spec: &AnsSpec, final_state: usize) -> Self {
    let table_size = spec.table_size();
    let mut nodes = Vec::with_capacity(table_size);
    // x_s from Jarek Duda's paper
    let mut token_x_s = spec.token_weights.clone();
    for &token in &spec.state_tokens {
      let mut next_state_base = token_x_s[token as usize];
      let mut bits_to_read = 0;
      while next_state_base < table_size {
        next_state_base *= 2;
        bits_to_read += 1;
      }
      nodes.push(Node {
        token,
        next_state_base,
        bits_to_read,
      });
      token_x_s[token as usize] += 1;
    }

    Self {
      table_size,
      nodes,
      state: final_state,
    }
  }

  pub fn from_bins<U: UnsignedLike>(
    size_log: Bitlen,
    bins: &[Bin<U>],
    final_state: usize,
  ) -> QCompressResult<Self> {
    let weights = bins.iter().map(|bin| bin.weight).collect::<Vec<_>>();
    let spec = AnsSpec::from_weights(size_log, weights)?;
    Ok(Self::new(&spec, final_state))
  }

  #[inline]
  pub fn unchecked_decode(&mut self, reader: &mut BitReader) -> Token {
    let node = &self.nodes[self.state - self.table_size];
    let bits_read = reader.unchecked_read_uint::<usize>(node.bits_to_read);
    let next_state = node.next_state_base + bits_read;
    self.state = next_state;
    node.token
  }

  pub fn decode(&mut self, reader: &mut BitReader) -> QCompressResult<Token> {
    let node = &self.nodes[self.state - self.table_size];
    let bits_read = reader.read_usize(node.bits_to_read)?;
    let next_state = node.next_state_base + bits_read;
    self.state = next_state;
    Ok(node.token)
  }

  pub fn state(&self) -> usize {
    self.state
  }

  pub fn recover(&mut self, state: usize) {
    self.state = state;
  }
}
