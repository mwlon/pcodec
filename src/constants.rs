pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAX_ENTRIES: u64 = (1_u64 << 24) - 1;
pub const BITS_TO_ENCODE_N_ENTRIES: u32 = 24;
pub const MAX_MAX_DEPTH: u32 = 15;
pub const BITS_TO_ENCODE_PREFIX_LEN: u32 = 4;
pub const MAX_JUMPSTART: usize = 31;
pub const BITS_TO_ENCODE_JUMPSTART: u32 = 5;
pub const MAX_CHUNKS: u64 = (1_u64 << 16) - 1;
pub const BITS_TO_ENCODE_N_CHUNKS: u32 = 16;

#[cfg(test)]
mod tests {
  use crate::constants::*;

  fn assert_can_encode(n_bits: u32, max_number: u64) {
    let min_required_bits = ((max_number + 1) as f64).log2().ceil() as u32;
    assert!(n_bits >= min_required_bits)
  }

  #[test]
  fn test_bits_to_encode_n_entries() {
    assert_can_encode(BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES);
  }

  #[test]
  fn test_bits_to_encode_prefix_len() {
    assert_can_encode(BITS_TO_ENCODE_PREFIX_LEN, MAX_MAX_DEPTH as u64);
  }

  #[test]
  fn test_bits_to_encode_jumpstart() {
    assert_can_encode(BITS_TO_ENCODE_JUMPSTART, MAX_JUMPSTART as u64);
  }

  #[test]
  fn test_bits_to_encode_n_chunks() {
    assert_can_encode(BITS_TO_ENCODE_N_CHUNKS, MAX_CHUNKS);
  }
}
