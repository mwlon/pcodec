use crate::constants::BYTES_PER_WORD;

/// Wrapper around a `Vec<usize>` with a specific number of bits.
///
/// This is used during decompression because doing bit-level operations on a
/// `Vec<usize>` is faster than on a `Vec<u8>`; `usize` represents the
/// true word size of the processor.
#[derive(Clone, Debug)]
pub struct BitWords {
  pub(crate) bytes: Vec<u8>,
}

impl Default for BitWords {
  fn default() -> Self {
    Self::from(&[])
  }
}

impl<B: AsRef<[u8]>> From<B> for BitWords {
  fn from(bytes_wrapper: B) -> Self {
    let mut bytes = bytes_wrapper.as_ref().to_vec();
    bytes.extend(&vec![0; BYTES_PER_WORD]);
    BitWords { bytes }
  }
}

impl BitWords {
  pub fn total_bits(&self) -> usize {
    (self.bytes.len() - BYTES_PER_WORD) * 8
  }

  pub fn extend_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) {
    self.bytes.truncate(self.bytes.len() - BYTES_PER_WORD);
    self.bytes.extend(bytes.as_ref());
    self.bytes.extend(&vec![0; BYTES_PER_WORD]);
  }

  pub fn truncate_left(&mut self, bytes_to_free: usize) {
    self.bytes = self.bytes[bytes_to_free..].to_vec();
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