use std::cmp::{max, min};
use crate::bits;
use crate::constants::Bitlen;
use crate::errors::{QCompressError, QCompressResult};

// Here and in encoding/decoding, state is between [0, table_size)

pub type Token = u32;

pub struct AnsSpec {
  // log base 2 of the table size
  // e.g. the table states will be in [2^size_log, 2^(size_log + 1))
  pub size_log: Bitlen,
  // the ordered tokens in the table
  pub state_tokens: Vec<Token>,
  // the number of times each token appears in the table
  pub token_weights: Vec<usize>,
}

impl AnsSpec {
  fn choose_state_tokens(size_log: Bitlen, token_weights: &[usize]) -> QCompressResult<Vec<Token>> {
    struct TokenInitInfo {
      token: Token,
      weight: usize,
      current_weight: usize, // mutable
    }

    let table_size = token_weights.iter().sum::<usize>();
    if table_size != (1 << size_log) {
      return Err(QCompressError::corruption(format!(
        "table size log of {} does not agree with total weight of {}",
        size_log,
        table_size,
      )));
    }

    let mut state_tokens = vec![0; table_size];
    if token_weights.len() == 1 {
      return Ok(state_tokens)
    }

    let n_tokens = token_weights.len();
    let mut token_infos = token_weights.iter().enumerate().map(|(token, &weight)| TokenInitInfo {
      token: token as Token,
      weight,
      current_weight: 0,
    })
      .collect::<Vec<_>>();

    let mut state_idx = table_size;
    let mut min_token_left = 0;
    while state_idx > 0 {
      for token in min_token_left..n_tokens {
        let info = &mut token_infos[token];
        let states_finished = table_size - state_idx + 1;
        // reps_short is how many reps of this token we would need to add to
        // exceed this token's overall frequency among the states allocated
        // so far.
        let reps_short = bits::ceil_div(
          (info.weight * states_finished).saturating_sub(info.current_weight * table_size),
          table_size - info.weight,
        );
        let weight_remaining = info.weight - info.current_weight;
        let remaining_states = state_idx;
        let reps_needed_to_interleave = max(
          1,
          weight_remaining / (remaining_states - weight_remaining + 1),
        );
        let reps = min(
          reps_short,
          reps_needed_to_interleave,
        );
        for _ in 0..reps {
          state_idx -= 1;
          state_tokens[state_idx] = token as Token;
        }

        info.current_weight += reps;
        if info.current_weight == info.weight {
          min_token_left = token + 1;
        }

        if state_idx == 0 {
          break;
        }
      }
    }

    Ok(state_tokens)
  }
  // This needs to remain backward compatible.
  // The general idea is to spread the tokens out as much as possible,
  // deterministically, and ensuring each one gets as least one state.
  // Long runs of tokens are generally bad.
  // In the sparse case, it's best to have the very frequent tokens in the low
  // states and rarer tokens somewhat present in the high states, so for best
  // compression, we expect token_weights to be ordered from least frequent to
  // most frequent.
  pub fn from_counts(size_log: Bitlen, token_weights: Vec<usize>) -> QCompressResult<Self> {
    let state_tokens = Self::choose_state_tokens(size_log, &token_weights)?;

    Ok(Self {
      size_log,
      state_tokens,
      token_weights,
    })
  }

  pub fn table_size(&self) -> usize {
    1 << self.size_log
  }

  // number of distinct tokens
  pub fn alphabet_size(&self) -> usize {
    self.token_weights.len()
  }
}

#[cfg(test)]
mod tests {
  use crate::ans::spec::{AnsSpec, Token};
  use crate::errors::QCompressResult;

  fn assert_state_tokens(weights: Vec<usize>, expected: Vec<Token>) -> QCompressResult<()> {
    let table_size_log = weights.iter().sum::<usize>().ilog2();
    let spec = AnsSpec::from_counts(table_size_log, weights)?;
    assert_eq!(spec.state_tokens, expected);
    Ok(())
  }

  #[test]
  fn ans_spec_new() -> QCompressResult<()> {
    assert_state_tokens(
      vec![1, 1, 3, 11],
      vec![3, 3, 3, 3, 2, 3, 3, 3, 3, 2, 3, 3, 3, 2, 1, 0],
    )
  }

  #[test]
  fn ans_spec_new_trivial() -> QCompressResult<()> {
    assert_state_tokens(
      vec![1],
      vec![0],
    )?;
    assert_state_tokens(
      vec![2],
      vec![0, 0],
    )
  }
}
