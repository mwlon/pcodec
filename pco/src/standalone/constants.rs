use crate::constants::Bitlen;

// ascii for pco!
pub const MAGIC_HEADER: [u8; 4] = [112, 99, 111, 33];
pub const MAGIC_TERMINATION_BYTE: u8 = 0;
pub const BITS_TO_ENCODE_N_ENTRIES: Bitlen = 24;
pub const BITS_TO_ENCODE_COMPRESSED_BODY_SIZE: Bitlen = 32;

#[cfg(test)]
mod tests {
  use crate::constants::MAX_ENTRIES;
  use crate::standalone::constants::*;

  #[test]
  fn test_enough_bits() {
    assert!(1 << BITS_TO_ENCODE_N_ENTRIES >= MAX_ENTRIES);
  }
}