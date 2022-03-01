pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAGIC_CHUNK_BYTE: u8 = 44; // ,
pub const MAGIC_TERMINATION_BYTE: u8 = 46; // .

pub const MAX_DELTA_ENCODING_ORDER: usize = 7;
pub const BITS_TO_ENCODE_DELTA_ENCODING_ORDER: usize = 3;
pub const MAX_ENTRIES: usize = (1 << 24) - 1;
pub const BITS_TO_ENCODE_N_ENTRIES: usize = 24;
pub const BITS_TO_ENCODE_N_PREFIXES: usize = 15;
pub const MAX_COMPRESSION_LEVEL: usize = 12;
pub const MAX_JUMPSTART: usize = BITS_TO_ENCODE_N_ENTRIES;
pub const BITS_TO_ENCODE_JUMPSTART: usize = 5;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: usize = 32;

pub const MAX_PREFIX_TABLE_SIZE_LOG: usize = 4; // tuned to maximize performance on intel i5
// pub const MAX_PREFIX_TABLE_SIZE: usize = 1_usize << MAX_PREFIX_TABLE_SIZE_LOG;

pub const WORD_SIZE: usize = usize::BITS as usize;
pub const BYTES_PER_WORD: usize = WORD_SIZE / 8;

#[cfg(test)]
mod tests {
  use crate::constants::*;

  fn assert_can_encode(n_bits: usize, max_number: usize) {
    let min_required_bits = ((max_number + 1) as f64).log2().ceil() as usize;
    assert!(n_bits >= min_required_bits)
  }

  #[test]
  fn test_max_jumpstart_bound() {
    assert!(MAX_JUMPSTART <= BITS_TO_ENCODE_N_ENTRIES);
  }

  #[test]
  fn test_bits_to_encode_delta_encoding_order() {
    assert_can_encode(BITS_TO_ENCODE_DELTA_ENCODING_ORDER, MAX_DELTA_ENCODING_ORDER);
  }

  #[test]
  fn test_bits_to_encode_n_entries() {
    assert_can_encode(BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES);
  }

  #[test]
  fn test_bits_to_encode_jumpstart() {
    assert_can_encode(BITS_TO_ENCODE_JUMPSTART, MAX_JUMPSTART);
  }

  #[test]
  fn test_prefix_table_size_fits_in_byte() {
    assert!(MAX_PREFIX_TABLE_SIZE_LOG > 0);
    assert!(MAX_PREFIX_TABLE_SIZE_LOG <= 8);
  }
}
