use crate::constants::DECOMPRESS_BYTE_PADDING;

// maintains padding at the end of the bytes, even as new ones are added
#[derive(Clone, Debug)]
pub struct PaddedBytes {
  pub(crate) bytes: Vec<u8>,
}

impl Default for PaddedBytes {
  fn default() -> Self {
    Self::from(&[])
  }
}

impl<B: AsRef<[u8]>> From<B> for PaddedBytes {
  fn from(bytes_wrapper: B) -> Self {
    let mut res = PaddedBytes {
      bytes: Vec::with_capacity(bytes_wrapper.as_ref().len() + DECOMPRESS_BYTE_PADDING),
    };
    res.extend_bytes(bytes_wrapper);
    res
  }
}

impl PaddedBytes {
  pub fn total_bits(&self) -> usize {
    (self.bytes.len() - DECOMPRESS_BYTE_PADDING) * 8
  }

  pub fn extend_bytes<B: AsRef<[u8]>>(&mut self, bytes: B) {
    self
      .bytes
      .truncate(self.bytes.len().saturating_sub(DECOMPRESS_BYTE_PADDING));
    self
      .bytes
      .reserve((bytes.as_ref().len() + DECOMPRESS_BYTE_PADDING).saturating_sub(self.bytes.len()));
    self.bytes.extend(bytes.as_ref());
    self.bytes.extend(&vec![0; DECOMPRESS_BYTE_PADDING]);
  }

  pub fn truncate_left(&mut self, bytes_to_free: usize) {
    self.bytes = self.bytes[bytes_to_free..].to_vec();
  }
}

#[cfg(test)]
mod tests {
  use crate::bit_reader::BitReader;
  use crate::bit_words::PaddedBytes;

  #[test]
  fn test_extend() {
    let mut words = PaddedBytes::default();
    words.extend_bytes([0, 1, 2, 3, 4, 5, 6, 7]);
    words.extend_bytes([8]);
    words.extend_bytes([9, 10]);
    words.extend_bytes([11, 12, 13, 14, 15, 16]);

    let mut reader = BitReader::from(&words);
    for i in 0_u32..17 {
      assert_eq!(reader.unchecked_read_uint::<u32>(8), i);
    }
    assert!(reader.read_one().is_err());
  }
}
