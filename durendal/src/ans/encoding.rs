use crate::ans::spec::{AnsSpec, Token};
use crate::bit_writer::BitWriter;
use crate::bits;
use crate::constants::Bitlen;

struct TokenInfo {
  renorm_bit_cutoff: usize,
  min_renorm_bits: Bitlen,
  next_states: Vec<usize>,
}

impl TokenInfo {
  fn next_state_for(&self, x_s: usize) -> usize {
    self.next_states[x_s - self.next_states.len()]
  }
}

pub struct AnsEncoder {
  token_infos: Vec<TokenInfo>,
  state: usize,
}

impl AnsEncoder {
  pub fn new(spec: &AnsSpec) -> Self {
    let table_size = spec.table_size();

    let mut token_infos = spec.token_weights.iter().map(|&weight| {
      // e.g. If the token count is 3 and table size is 16, so the x_s values
      // are in [3, 6).
      // We find the power of 2 in this range (4), then compare its log to 16
      // to find the min renormalization bits (4 - 2 = 2).
      // Finally we choose the cutoff as 2 * 3 * 2 ^ renorm_bits = 24.
      let max_x_s = 2 * weight - 1;
      let min_renorm_bits = spec.size_log - max_x_s.ilog2();
      let renorm_bit_cutoff = 2 * weight * (1 << min_renorm_bits);
      TokenInfo {
        renorm_bit_cutoff,
        min_renorm_bits,
        next_states: Vec::with_capacity(weight),
      }
    })
      .collect::<Vec<_>>();

    for (state_idx, &token) in spec.state_tokens.iter().enumerate() {
      token_infos[token as usize].next_states.push(table_size + state_idx);
    }

    Self {
      // We choose the initial state from [table_size, 2 * table_size)
      // to be the minimum as this tends to require fewer bits to encode
      // the first token.
      state: table_size,
      token_infos,
    }
  }

  // Returns the number of bits to write and the value of those bits.
  // The value of those bits may contain larger significant bits that must be
  // ignored.
  // We don't write to a BitWriter directly because ANS operates in a LIFO
  // manner. We need to write these in reverse order.
  pub fn encode(&mut self, token: Token) -> (usize, Bitlen) {
    let token_info = &self.token_infos[token as usize];
    let renorm_bits = if self.state >= token_info.renorm_bit_cutoff {
      token_info.min_renorm_bits + 1
    } else {
      token_info.min_renorm_bits
    };
    let word = self.state;
    self.state = token_info.next_state_for(self.state >> renorm_bits);
    (word, renorm_bits)
  }

  pub fn state(&self) -> usize {
    self.state
  }
}