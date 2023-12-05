use crate::constants::{Bitlen, OVERSHOOT_PADDING};

// ascii for pco!
pub const MAGIC_HEADER: [u8; 4] = [112, 99, 111, 33];
pub const MAGIC_TERMINATION_BYTE: u8 = 0;
pub const BITS_TO_ENCODE_N_ENTRIES: Bitlen = 24;
pub const CURRENT_STANDALONE_VERSION: usize = 2;
pub const BITS_TO_ENCODE_STANDALONE_VERSION: Bitlen = 8;

// padding
pub const STANDALONE_CHUNK_PREAMBLE_PADDING: usize =
  1 + BITS_TO_ENCODE_N_ENTRIES as usize + OVERSHOOT_PADDING;
pub const STANDALONE_HEADER_PADDING: usize = 30;

#[cfg(test)]
mod tests {
  use crate::constants::MAX_ENTRIES;
  use crate::standalone::constants::*;

  #[test]
  fn test_enough_bits() {
    assert!(1 << BITS_TO_ENCODE_N_ENTRIES >= MAX_ENTRIES);
  }
}
