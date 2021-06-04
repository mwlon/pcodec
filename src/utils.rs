use std::fmt;

use crate::bits::bits_to_string;
use crate::prefix::Prefix;

pub const MAGIC_HEADER: [u8; 4] = [113, 99, 111, 33]; // ascii for qco!
pub const MAX_ENTRIES: u64 = (1_u64 << 32) - 1;
pub const BITS_TO_ENCODE_N_ENTRIES: u32 = 32; // should be (MAX_ENTRIES + 1).log2().ceil()
pub const MAX_MAX_DEPTH: u32 = 15;
pub const BITS_TO_ENCODE_PREFIX_LEN: u32 = 4; // should be (MAX_MAX_DEPTH + 1).log2().ceil()

#[inline(always)]
pub fn u64_diff(upper: i64, lower: i64) -> u64 {
  if lower >= 0 {
    return (upper - lower) as u64;
  }
  if lower == upper {
    return 0;
  }
  let pos_lower_p1 = (lower + 1).abs() as u64;
  if upper >= 0 {
    return (upper as u64) + pos_lower_p1 + 1;
  }
  return (pos_lower_p1 + 1) - (upper.abs() as u64);
}

#[inline(always)]
pub fn i64_plus_u64(i: i64, u: u64) -> i64 {
  if i >= 0 {
    return (i as u64 + u) as i64;
  }
  if u == 0 {
    return i;
  }
  let negi = (-i) as u64;
  if negi <= u {
    (u - negi) as i64
  } else {
    -((negi - u) as i64)
  }
}

pub fn display_prefixes(prefixes: &Vec<Prefix>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
  let s = prefixes
    .iter()
    .map(|p| format!(
      "\t{}: {} to {} (density {})",
      bits_to_string(&p.val),
      p.lower,
      p.upper,
      2.0_f64.powf(-(p.val.len() as f64)) / (p.upper as f64 - p.lower as f64 + 1.0),
    ))
    .collect::<Vec<String>>()
    .join("\n");
  write!(f, "{}", s)
}
