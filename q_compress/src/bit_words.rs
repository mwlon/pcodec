use std::cmp::min;
use std::convert::TryInto;

use crate::bits;
use crate::constants::{BYTES_PER_WORD, WORD_SIZE};

/// Wrapper around a `Vec<usize>` with a specific number of bits.
///
/// This is used during decompression because doing bit-level operations on a
/// `Vec<usize>` is faster than on a `Vec<u8>`; `usize` represents the
/// true word size of the processor.
#[derive(Clone, Debug, Default)]
pub struct BitWords {
  pub(crate) words: Vec<usize>,
  pub(crate) total_bits: usize,
}

// returns the final number of bits after extending by the bytes
fn extend<B: AsRef<[u8]>>(words: &mut Vec<usize>, initial_bits: usize, bytes_wrapper: B) -> usize {
  let bytes = bytes_wrapper.as_ref();
  let total_bits = initial_bits + 8 * bytes.len();
  let n_words = bits::ceil_div(total_bits, WORD_SIZE);
  words.reserve(n_words - words.len());

  let initial_bytes = initial_bits / 8;
  let alignment = initial_bytes % BYTES_PER_WORD;
  let mut bytes_in_first_word = 0;
  if alignment != 0 {
    bytes_in_first_word = min(BYTES_PER_WORD - alignment, bytes.len());
    for i in 0..bytes_in_first_word {
      *words.last_mut().unwrap() |= (bytes[i] as usize) << (i + alignment);
    }
  }
  let last_aligned_byte =
    bytes_in_first_word + (bytes.len() - bytes_in_first_word) / BYTES_PER_WORD * BYTES_PER_WORD;

  if bytes_in_first_word < bytes.len() {
    words.extend(
      bytes[bytes_in_first_word..last_aligned_byte]
        .chunks_exact(BYTES_PER_WORD)
        .map(|word_bytes| usize::from_le_bytes(word_bytes.try_into().unwrap())),
    );
  }

  if words.len() < n_words {
    let mut last_bytes = bytes[last_aligned_byte..].to_vec();
    while last_bytes.len() < BYTES_PER_WORD {
      last_bytes.push(0);
    }
    words.push(usize::from_le_bytes(
      last_bytes.try_into().unwrap(),
    ));
  }
  total_bits
}

impl<B: AsRef<[u8]>> From<B> for BitWords {
  fn from(bytes_wrapper: B) -> Self {
    let mut words = Vec::new();
    let total_bits = extend(&mut words, 0, bytes_wrapper);

    BitWords { words, total_bits }
  }
}

impl BitWords {
  pub fn extend_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) {
    self.total_bits = extend(&mut self.words, self.total_bits, bytes);
  }

  pub fn truncate_left(&mut self, words_to_free: usize) {
    self.words = self.words[words_to_free..].to_vec();
    self.total_bits -= words_to_free * WORD_SIZE;
  }
}

#[cfg(test)]
mod tests {
  use crate::bit_reader::BitReader;
  use crate::bit_words::BitWords;

  #[test]
  fn test_extend() {
    let mut words = BitWords::default();
    words.extend_bytes(&[0, 1, 2, 3, 4, 5, 6, 7]);
    words.extend_bytes(&[8]);
    words.extend_bytes(&[9, 10]);
    words.extend_bytes(&[11, 12, 13, 14, 15, 16]);

    let mut reader = BitReader::from(&words);
    for i in 0_u32..17 {
      assert_eq!(reader.unchecked_read_uint::<u32>(8), i);
    }
    assert!(reader.read_one().is_err());
  }
}
