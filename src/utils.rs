use std::fmt;

use crate::bits::bits_to_string;
use crate::prefix::Prefix;
use crate::types::NumberLike;

pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAX_ENTRIES: u64 = (1_u64 << 32) - 1;
pub const BITS_TO_ENCODE_N_ENTRIES: u32 = 32; // should be (MAX_ENTRIES + 1).log2().ceil()
pub const MAX_MAX_DEPTH: u32 = 15;
pub const BITS_TO_ENCODE_PREFIX_LEN: u32 = 4; // should be (MAX_MAX_DEPTH + 1).log2().ceil()

pub fn display_prefixes<T: NumberLike>(prefixes: &Vec<Prefix<T>>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
  let s = prefixes
    .iter()
    .map(|p| format!(
      "\t{}: {} to {}",
      bits_to_string(&p.val),
      p.lower,
      p.upper,
    ))
    .collect::<Vec<String>>()
    .join("\n");
  write!(f, "{}", s)
}
