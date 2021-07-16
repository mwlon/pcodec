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

pub fn bit_from_byte(byte: u8, j: usize) -> bool {
  (byte & BIT_MASKS[j]) > 0
}

pub fn byte_to_bits(byte: u8) -> [bool; 8] {
  let mut res: [bool; 8];
  unsafe {
    res = std::mem::MaybeUninit::uninit().assume_init();
  }
  for (j, entry) in res.iter_mut().enumerate() {
    *entry = bit_from_byte(byte, j)
  }
  res
}

pub fn bytes_to_bits(bytes: Vec<u8>) -> Vec<bool> {
  let mut res = Vec::with_capacity(8 * bytes.len());
  for b in &bytes {
    res.extend(&byte_to_bits(*b));
  }
  res
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

pub fn bits_to_usize_truncated(bits: &[bool], max_depth: u32) -> usize {
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

pub fn usize_to_varint_bits(mut x: usize, jumpstart: usize) -> Vec<bool> {
  let mut res = Vec::with_capacity(jumpstart + 5);
  res.extend(usize_to_bits(x, jumpstart as u32));
  x >>= jumpstart;
  while x > 0 {
    res.push(true);
    res.push(x & 1 > 0);
    x >>= 1;
  }
  res.push(false);
  res
}

pub fn usize_to_bits(x: usize, n_bits: u32) -> Vec<bool> {
  u64_to_bits(x as u64, n_bits)
}

pub fn u64_to_bits(x: u64, n_bits: u32) -> Vec<bool> {
  let mut res = Vec::with_capacity(n_bits as usize);
  extend_with_u64_bits(x, n_bits, &mut res);
  res
}

pub fn extend_with_u64_bits(x: u64, n_bits: u32, v: &mut Vec<bool>) {
  // the least significant bits, but still in bigendian order
  for i in 1..n_bits + 1 {
    let shift = n_bits - i;
    v.push(x & (1 << shift) > 0);
  }
}

pub fn bits_to_string(bits: &[bool]) -> String {
  return bits
    .iter()
    .map(|b| if *b {"1"} else {"0"})
    .collect::<Vec<&str>>()
    .join("");
}

pub fn avg_base2_bits(upper_lower_diff: u64) -> f64 {
  let n = upper_lower_diff as f64 + 1.0;
  let k = n.log2().floor();
  let two_to_k = (2.0_f64).powf(k);
  let overshoot = n - two_to_k;
  k + (2.0 * overshoot) / n
}

pub fn depth_bits(weight: u64, total_weight: usize) -> f64 {
  -(weight as f64 / total_weight as f64).log2()
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
    assert_eq!(depth_bits(2, 2), 0.0);
    assert_eq!(depth_bits(2, 4), 1.0);
    assert_eq!(depth_bits(2, 8), 2.0);
    assert_eq!(depth_bits(4, 8), 1.0);
  }

  #[test]
  fn test_avg_base2_bits() {
    assert_eq!(avg_base2_bits(0), 0.0);
    assert_eq!(avg_base2_bits(1), 1.0);
    assert!((avg_base2_bits(2) - 5.0 / 3.0).abs() < 1E-8);
    assert_eq!(avg_base2_bits(3), 2.0);
    println!("{}", avg_base2_bits(4));
    assert!((avg_base2_bits(4) - 12.0 / 5.0).abs() < 1E-8);
  }

  #[test]
  fn test_u64_to_bits() {
    assert_eq!(u64_to_bits(7, 0), vec![]);
    assert_eq!(u64_to_bits(7, 4), vec![false, true, true, true]);
  }

  #[test]
  fn test_usize_to_bits() {
    assert_eq!(usize_to_bits(7, 5), vec![false, false, true, true, true]);
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
    assert_eq!(
      bytes_to_bits(byte_28),
      bits_28
    );

    let bits_28_128 = vec![false, false, false, true, true, true, false, false, true];
    let byte_28_128 = bits_to_bytes(bits_28_128.clone());
    assert_eq!(
      byte_28_128,
      vec![28, 128]
    );
    assert_eq!(
      bytes_to_bits(byte_28_128)[0..9],
      bits_28_128[..],
    )
  }
}
