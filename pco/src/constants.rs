// Doing bit reads/writes/shifts with u32 is more performant than u64.
// This type could also be u8 or u16.
pub type Bitlen = u32;

// magic identification bytes
pub const MAGIC_HEADER: [u8; 4] = [112, 99, 111, 33]; // ascii for pco!
pub const MAGIC_CHUNK_BYTE: u8 = 44; // ,
pub const MAGIC_TERMINATION_BYTE: u8 = 46; // .

// bit lengths
pub const BITS_TO_ENCODE_ANS_SIZE_LOG: Bitlen = 4;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: Bitlen = 32;
pub const BITS_TO_ENCODE_DELTA_ENCODING_ORDER: Bitlen = 3;
pub const BITS_TO_ENCODE_LOOKBACK: Bitlen = 10;
pub const BITS_TO_ENCODE_MODE: Bitlen = 4;
pub const BITS_TO_ENCODE_N_ENTRIES: Bitlen = 24;
pub const BITS_TO_ENCODE_N_BINS: Bitlen = 15;
pub const BITS_TO_ENCODE_N_LOOKBACKS: Bitlen = 6;

// performance tuning parameters
pub const DECOMPRESS_UNCHECKED_THRESHOLD: usize = 32;
pub const DECOMPRESS_BYTE_PADDING: usize = BYTES_PER_WORD + DECOMPRESS_UNCHECKED_THRESHOLD * 8;
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
pub const MAX_AUTO_DELTA_COMPRESSION_LEVEL: usize = 6;

// defaults
pub const DEFAULT_COMPRESSION_LEVEL: usize = 8;

// other
pub const MAX_N_STREAMS: usize = 2;

#[cfg(test)]
mod tests {
  use crate::constants::*;

  fn bits_to_encode(max_value: usize) -> Bitlen {
    usize::BITS - max_value.leading_zeros()
  }

  fn assert_can_encode(n_bits: Bitlen, max_number: usize) {
    assert!(n_bits >= bits_to_encode(max_number));
  }

  #[test]
  fn test_padding_sufficient() {
    // We need at least 8 bytes of padding to ensure reading a word doesn't
    // go out of bounds on any architecture.
    assert!(DECOMPRESS_BYTE_PADDING >= 8);
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
}
