use crate::ans::spec::Spec;
use crate::ans::{AnsState, Token};
use crate::bit_reader::BitReader;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;

use crate::ChunkLatentMetadata;

#[derive(Clone, Debug)]
struct Node {
  token: Token,
  next_state_idx_base: usize,
  bits_to_read: Bitlen,
}

#[derive(Clone, Debug)]
pub struct Decoder {
  table_size: usize,
  nodes: Vec<Node>,
  state_idx: usize,
}

impl Decoder {
  pub fn new(spec: &Spec, final_state: AnsState) -> Self {
    let table_size = spec.table_size();
    let mut nodes = Vec::with_capacity(table_size);
    // x_s from Jarek Duda's paper
    let mut token_x_s = spec.token_weights.clone();
    for &token in &spec.state_tokens {
      let mut next_state_base = token_x_s[token as usize] as AnsState;
      let mut bits_to_read = 0;
      while next_state_base < table_size as AnsState {
        next_state_base *= 2;
        bits_to_read += 1;
      }
      nodes.push(Node {
        token,
        next_state_idx_base: next_state_base as usize - table_size,
        bits_to_read,
      });
      token_x_s[token as usize] += 1;
    }

    Self {
      table_size,
      nodes,
      state_idx: final_state as usize - table_size,
    }
  }

  pub fn from_latent_meta<U: UnsignedLike>(
    latent_meta: &ChunkLatentMetadata<U>,
    final_state: AnsState,
  ) -> PcoResult<Self> {
    let weights = latent_meta
      .bins
      .iter()
      .map(|bin| bin.weight)
      .collect::<Vec<_>>();
    let spec = Spec::from_weights(latent_meta.ans_size_log, weights)?;
    Ok(Self::new(&spec, final_state))
  }

  #[inline]
  pub fn unchecked_decode(&mut self, reader: &mut BitReader) -> Token {
    let node = &self.nodes[self.state_idx];
    self.state_idx = node.next_state_idx_base + reader.unchecked_read_small(node.bits_to_read);
    node.token
  }

  pub fn decode(&mut self, reader: &mut BitReader) -> PcoResult<Token> {
    let node = &self.nodes[self.state_idx];
    self.state_idx = node.next_state_idx_base + reader.read_small(node.bits_to_read)?;
    Ok(node.token)
  }

  pub fn state(&self) -> AnsState {
    (self.state_idx + self.table_size) as AnsState
  }

  pub fn recover(&mut self, state: AnsState) {
    self.state_idx = state as usize - self.table_size;
  }
}
