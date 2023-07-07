use crate::ans::spec::{Spec, Token};
use crate::bit_reader::BitReader;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;

use crate::chunk_metadata::DataPageStreamMetadata;

#[derive(Clone, Debug)]
struct Node {
  token: Token,
  next_state_base: usize,
  bits_to_read: Bitlen,
}

#[derive(Clone, Debug)]
pub struct Decoder {
  table_size: usize,
  nodes: Vec<Node>,
  pub state: usize,
}

impl Decoder {
  pub fn new(spec: &Spec, final_state: usize) -> Self {
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

  pub fn from_stream_meta<U: UnsignedLike>(stream: &DataPageStreamMetadata<U>) -> PcoResult<Self> {
    let weights = stream.bins.iter().map(|bin| bin.weight).collect::<Vec<_>>();
    let spec = Spec::from_weights(stream.ans_size_log, weights)?;
    Ok(Self::new(&spec, stream.ans_final_state))
  }

  #[inline]
  pub fn unchecked_decode(&mut self, reader: &mut BitReader) -> Token {
    let node = &self.nodes[self.state - self.table_size];
    let bits_read = reader.unchecked_read_uint::<usize>(node.bits_to_read);
    let next_state = node.next_state_base + bits_read;
    self.state = next_state;
    node.token
  }

  pub fn decode(&mut self, reader: &mut BitReader) -> PcoResult<Token> {
    let node = &self.nodes[self.state - self.table_size];
    let bits_read = reader.read_small(node.bits_to_read)?;
    let next_state = node.next_state_base + bits_read;
    self.state = next_state;
    Ok(node.token)
  }
}
