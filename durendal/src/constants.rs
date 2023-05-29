// Doing bit reads/writes/shifts with u32 is more performant than u64.
// This type could also be u8 or u16.
pub type Bitlen = u32;

// magic identification bytes
pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAGIC_CHUNK_BYTE: u8 = 44; // ,
pub const MAGIC_TERMINATION_BYTE: u8 = 46; // .

// bit lengths
pub const BITS_TO_ENCODE_ADJ_BITS: Bitlen = 6;
pub const BITS_TO_ENCODE_DELTA_ENCODING_ORDER: Bitlen = 3;
pub const BITS_TO_ENCODE_N_ENTRIES: Bitlen = 24;
pub const BITS_TO_ENCODE_N_BINS: Bitlen = 15;
pub const BITS_TO_ENCODE_JUMPSTART: Bitlen = 5;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: Bitlen = 32;
pub const BITS_TO_ENCODE_CODE_LEN: Bitlen = 5;
pub const MAX_JUMPSTART: Bitlen = BITS_TO_ENCODE_N_ENTRIES;

// performance tuning parameters
pub const MAX_BIN_TABLE_SIZE_LOG: Bitlen = 8;
pub const UNSIGNED_BATCH_SIZE: usize = 512;

// native architecture info
pub const WORD_SIZE: usize = usize::BITS as usize;
pub const WORD_BITLEN: Bitlen = usize::BITS as Bitlen;
pub const BYTES_PER_WORD: usize = WORD_SIZE / 8;

// cutoffs and legal parameter values
pub const AUTO_DELTA_LIMIT: usize = 1100;
pub const MAX_COMPRESSION_LEVEL: usize = 12;
pub const MAX_DELTA_ENCODING_ORDER: usize = 7;
pub const MAX_ENTRIES: usize = (1 << 24) - 1;
pub const MIN_FREQUENCY_TO_USE_RUN_LEN: f64 = 0.8;
pub const MIN_N_TO_USE_RUN_LEN: usize = 1001;
pub const MAX_AUTO_DELTA_COMPRESSION_LEVEL: usize = 6;

// defaults
pub const DEFAULT_COMPRESSION_LEVEL: usize = 8;

#[cfg(test)]
mod tests {
  use crate::bits::bits_to_encode;
  use crate::constants::*;

  fn assert_can_encode(n_bits: Bitlen, max_number: usize) {
    assert!(n_bits >= bits_to_encode(max_number));
  }

  #[test]
  fn test_max_jumpstart_bound() {
    assert!(MAX_JUMPSTART <= BITS_TO_ENCODE_N_ENTRIES);
  }

  #[test]
  fn test_bits_to_encode_delta_encoding_order() {
    assert_can_encode(
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
      MAX_DELTA_ENCODING_ORDER,
    );
  }

  #[test]
  fn test_bits_to_encode_n_entries() {
    assert_can_encode(BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES);
  }

  #[test]
  fn test_bits_to_encode_jumpstart() {
    assert_can_encode(
      BITS_TO_ENCODE_JUMPSTART,
      MAX_JUMPSTART as usize,
    );
  }

  #[test]
  fn test_bin_table_size_fits_in_word() {
    assert!(MAX_BIN_TABLE_SIZE_LOG > 0);
    assert!(MAX_BIN_TABLE_SIZE_LOG <= WORD_BITLEN);
  }

  #[test]
  fn test_auto_detects_sparsity() {
    assert!(AUTO_DELTA_LIMIT > MIN_N_TO_USE_RUN_LEN + MAX_DELTA_ENCODING_ORDER);
  }
}
