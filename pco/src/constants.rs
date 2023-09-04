// must be u8 or larger
// u64+ seems less performant
// exposed in public API
pub(crate) type Bitlen = u32;
// must be u32 or larger
// exposed in public API
pub(crate) type Weight = u32;

// magic identification bytes
pub const MAGIC_HEADER: [u8; 4] = [112, 99, 111, 33]; // ascii for pco!
pub const MAGIC_CHUNK_BYTE: u8 = 44; // ,
pub const MAGIC_TERMINATION_BYTE: u8 = 46; // .

// bit lengths
pub const BITS_TO_ENCODE_ANS_SIZE_LOG: Bitlen = 4;
pub const BITS_TO_ENCODE_DELTA_ENCODING_ORDER: Bitlen = 3;
pub const BITS_TO_ENCODE_MODE: Bitlen = 4;
pub const BITS_TO_ENCODE_N_ENTRIES: Bitlen = 24;
pub const BITS_TO_ENCODE_N_BINS: Bitlen = 15;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: Bitlen = 32;

// performance tuning parameters
const WORDS_PER_OFFSET_UPPER_BOUND: usize = 5;
pub const DECOMPRESS_BYTE_PADDING: usize = BYTES_PER_WORD * WORDS_PER_OFFSET_UPPER_BOUND;
// pub const UNSIGNED_BATCH_SIZE: usize = 512;

// native architecture info
pub const WORD_SIZE: usize = usize::BITS as usize;
pub const WORD_BITLEN: Bitlen = usize::BITS as Bitlen;
pub const BYTES_PER_WORD: usize = WORD_SIZE / 8;

// cutoffs and legal parameter values
pub const AUTO_DELTA_LIMIT: usize = 1100;
pub const MAX_COMPRESSION_LEVEL: usize = 12;
pub const MAX_DELTA_ENCODING_ORDER: usize = 7;
pub const MAX_ENTRIES: usize = (1 << BITS_TO_ENCODE_N_ENTRIES) - 1;
pub const MAX_AUTO_DELTA_COMPRESSION_LEVEL: usize = 6;

// defaults
pub const DEFAULT_COMPRESSION_LEVEL: usize = 8;

// other
pub const FULL_BATCH_SIZE: usize = 256;
pub const ANS_INTERLEAVING: usize = 4;

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
