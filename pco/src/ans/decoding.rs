use crate::ans::spec::Spec;
use crate::ans::{AnsState, Token};
use crate::bit_reader::BitReader;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;

use crate::ChunkLatentMetadata;

#[derive(Clone, Debug)]
pub struct Node {
  pub token: Token,
  pub next_state_idx_base: usize,
  pub bits_to_read: Bitlen,
}

#[derive(Clone, Debug)]
pub struct Decoder {
  nodes: Vec<Node>,
}

impl Decoder {
  pub fn new(spec: &Spec) -> Self {
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
      nodes,
    }
  }

  pub fn from_latent_meta<U: UnsignedLike>(
    latent_meta: &ChunkLatentMetadata<U>,
  ) -> PcoResult<Self> {
    let weights = latent_meta
      .bins
      .iter()
      .map(|bin| bin.weight)
      .collect::<Vec<_>>();
    let spec = Spec::from_weights(latent_meta.ans_size_log, weights)?;
    Ok(Self::new(&spec))
  }

  #[inline]
  pub fn get_node(&self, state_idx: usize) -> &Node {
    unsafe { self.nodes.get_unchecked(state_idx) }
  }

  // #[inline]
  // pub fn unchecked_decode(&mut self, reader: &mut BitReader) -> Token {
  //   let node = unsafe { self.nodes.get_unchecked(self.state_idx) };
  //   self.state_idx = node.next_state_idx_base + reader.unchecked_read_small(node.bits_to_read);
  //   node.token
  // }

  // pub fn decode(&mut self, reader: &mut BitReader) -> PcoResult<Token> {
  //   let node = &self.nodes[self.state_idx];
  //   self.state_idx = node.next_state_idx_base + reader.read_small(node.bits_to_read)?;
  //   Ok(node.token)
  // }

  // pub fn state_idx(&self) -> AnsState {
  //   self.state_idx as AnsState
  // }
  //
  // pub fn recover(&mut self, state_idx: AnsState) {
  //   self.state_idx = state_idx as usize;
  // }
}
