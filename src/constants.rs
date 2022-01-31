pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAGIC_CHUNK_BYTE: u8 = 44; // ,
pub const MAGIC_TERMINATION_BYTE: u8 = 46; // .

pub const MAX_ENTRIES: u64 = (1_u64 << 24) - 1;
pub const BITS_TO_ENCODE_N_ENTRIES: u32 = 24;
pub const MAX_COMPRESSION_LEVEL: u32 = 12;
pub const BITS_TO_ENCODE_PREFIX_LEN: u32 = 5; // was 4 in v0.4
pub const MAX_JUMPSTART: usize = 31;
pub const BITS_TO_ENCODE_JUMPSTART: u32 = 5;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: u32 = 32;

pub const PREFIX_TABLE_SIZE_LOG: usize = 4; // tuned to maximize performance on intel i5
pub const PREFIX_TABLE_SIZE: usize = 1_usize << PREFIX_TABLE_SIZE_LOG;

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
    assert_can_encode(BITS_TO_ENCODE_PREFIX_LEN, MAX_COMPRESSION_LEVEL as u64);
  }

  #[test]
  fn test_bits_to_encode_jumpstart() {
    assert_can_encode(BITS_TO_ENCODE_JUMPSTART, MAX_JUMPSTART as u64);
  }

  #[test]
  fn test_prefix_table_size_fits_in_byte() {
    assert!(PREFIX_TABLE_SIZE_LOG > 0);
    assert!(PREFIX_TABLE_SIZE_LOG <= 8);
  }
}
