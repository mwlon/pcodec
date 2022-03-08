use crate::bits;
use std::convert::TryInto;
use crate::constants::{BYTES_PER_WORD, WORD_SIZE};

/// A wrapper around a Vec<usize> with a specific number of bits.
/// 
/// This is used during decompression because doing bit-level operations on a
/// `Vec<usize>` is faster than on a `Vec<u8>`; `usize` represents the
/// true word size of the processor.
#[derive(Clone)]
pub struct BitWords {
  pub words: Vec<usize>,
  pub total_bits: usize,
}

impl<B: AsRef<[u8]>> From<B> for BitWords {
  fn from(bytes_wrapper: B) -> Self {
    let bytes = bytes_wrapper.as_ref();
    let total_bits = 8 * bytes.len();
    let n_words = bits::ceil_div(total_bits, WORD_SIZE);
    let mut words = Vec::with_capacity(n_words);
    words.extend(
      bytes
        .chunks_exact(BYTES_PER_WORD)
        .map(|word_bytes| usize::from_be_bytes(word_bytes.try_into().unwrap()))
    );
    if words.len() < n_words {
      let mut last_bytes = bytes[words.len() * BYTES_PER_WORD..].to_vec();
      while last_bytes.len() < BYTES_PER_WORD {
        last_bytes.push(0);
      }
      words.push(usize::from_be_bytes(last_bytes.try_into().unwrap()));
    }

    BitWords {
      words,
      total_bits,
    }
  }
}
