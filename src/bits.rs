use crate::data_types::UnsignedLike;

const BIT_MASKS: [u8; 8] = [
  0x80,
  0x40,
  0x20,
  0x10,
  0x08,
  0x04,
  0x02,
  0x01,
];
pub const LEFT_MASKS: [u8; 9] = [
  0xff,
  0x7f,
  0x3f,
  0x1f,
  0x0f,
  0x07,
  0x03,
  0x01,
  0x00,
];
pub const RIGHT_MASKS: [u8; 9] = [
  0x00,
  0x80,
  0xc0,
  0xe0,
  0xf0,
  0xf8,
  0xfc,
  0xfe,
  0xff,
];

pub fn bit_from_byte(byte: u8, j: usize) -> bool {
  (byte & BIT_MASKS[j]) > 0
}

pub fn bits_to_bytes(bits: Vec<bool>) -> Vec<u8> {
  let mut res = Vec::new();
  let mut i = 0;
  while i < bits.len() {
    let mut byte = 0_u8;
    for _ in 0..8 {
      byte <<= 1;
      if i < bits.len() {
        if bits[i] {
          byte |= 1;
        }
        i += 1;
      }
    }
    res.push(byte);
  }
  res
}

pub fn bytes_to_bits(bytes: Vec<u8>) -> Vec<bool> {
  let mut res = Vec::with_capacity(bytes.len() * 8);
  for b in bytes {
    for &mask in &BIT_MASKS {
      res.push(b & mask > 0);
    }
  }
  res
}

pub fn bits_to_usize(bits: &[bool]) -> usize {
  bits_to_usize_truncated(bits, bits.len())
}

pub fn bits_to_usize_truncated(bits: &[bool], max_depth: usize) -> usize {
  if max_depth < 1 {
    return 0;
  }

  let pow = 1_usize << (max_depth - 1);
  let mut res = 0;
  for (i, bit) in bits.iter().enumerate() {
    if *bit {
      res |= pow >> i;
    }
  }
  res
}

pub fn usize_truncated_to_bits(x: usize, max_depth: usize) -> Vec<bool> {
  if max_depth < 1 {
    return Vec::new();
  }

  let mut res = Vec::with_capacity(max_depth);
  for i in 0..max_depth {
    res.push((x >> (max_depth - i - 1)) & 1 > 0);
  }
  res
}

pub fn bits_to_string(bits: &[bool]) -> String {
  return bits
    .iter()
    .map(|b| if *b {"1"} else {"0"})
    .collect::<Vec<&str>>()
    .join("");
}

fn bumpy_log(x: f64) -> f64 {
  let k = x.log2().floor();
  let two_to_k = (2.0_f64).powf(k);
  let overshoot = x - two_to_k;
  k + (2.0 * overshoot) / x
}

pub fn avg_offset_bits<Diff: UnsignedLike>(lower: Diff, upper: Diff) -> f64 {
  bumpy_log((upper - lower).to_f64() + 1.0)
}

pub fn avg_depth_bits(weight: usize, total_weight: usize) -> f64 {
  bumpy_log(total_weight as f64 / weight as f64)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_bits_to_string() {
    assert_eq!(bits_to_string(&[]), "".to_string());
    assert_eq!(bits_to_string(&[true, false, false]), "100".to_string());
  }

  #[test]
  fn test_depth_bits() {
    assert_eq!(avg_depth_bits(2, 2), 0.0);
    assert_eq!(avg_depth_bits(2, 4), 1.0);
    assert_eq!(avg_depth_bits(2, 8), 2.0);
    assert_eq!(avg_depth_bits(4, 8), 1.0);
  }

  #[test]
  fn test_avg_base2_bits() {
    assert_eq!(avg_offset_bits(0_u32, 0_u32), 0.0);
    assert_eq!(avg_offset_bits(4_u32, 5_u32), 1.0);
    assert!((avg_offset_bits(2_u32, 4_u32) - 5.0 / 3.0).abs() < 1E-8);
    assert_eq!(avg_offset_bits(10_u32, 13_u32), 2.0);
    assert!((avg_offset_bits(0_u64, 4_u64) - 12.0 / 5.0).abs() < 1E-8);
  }

  #[test]
  fn test_bits_to_usize_truncated() {
    assert_eq!(bits_to_usize_truncated(&[], 0), 0);
    assert_eq!(bits_to_usize_truncated(&[true], 4), 8);
    assert_eq!(bits_to_usize_truncated(&[true], 3), 4);
    assert_eq!(bits_to_usize_truncated(&[true, false, true], 4), 10);
    assert_eq!(bits_to_usize_truncated(&[true, false, true, true], 4), 11);
  }

  #[test]
  fn test_bits_to_bytes_to_bits() {
    let bits_28 = vec![false, false, false, true, true, true, false, false];
    let byte_28 = bits_to_bytes(bits_28.clone());
    assert_eq!(
      byte_28,
      vec![28]
    );

    let bits_28_128 = vec![false, false, false, true, true, true, false, false, true];
    let byte_28_128 = bits_to_bytes(bits_28_128.clone());
    assert_eq!(
      byte_28_128,
      vec![28, 128]
    );
  }
}
