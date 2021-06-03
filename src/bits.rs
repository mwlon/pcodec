use std::convert::TryInto;
use crate::utils::u64_diff;

#[inline(always)]
pub fn byte_to_bits(byte: u8) -> [bool; 8] {
  let mut res: [bool; 8];
  unsafe {
    res = std::mem::MaybeUninit::uninit().assume_init();
  }
  // not sure why rust doesn't unroll this when I do it as a loop with mask constants
  res[0] = (byte & 0x80) > 0;
  res[1] = (byte & 0x40) > 0;
  res[2] = (byte & 0x20) > 0;
  res[3] = (byte & 0x10) > 0;
  res[4] = (byte & 0x08) > 0;
  res[5] = (byte & 0x04) > 0;
  res[6] = (byte & 0x02) > 0;
  res[7] = (byte & 0x01) > 0;
  res
}

pub fn bits_to_int64(bits: Vec<bool>) -> i64 {
  let bytes = bits_to_bytes(bits);
  i64::from_be_bytes(bytes.try_into().unwrap())
}

pub fn bits_to_usize(bits: Vec<bool>) -> usize {
  let mut res = 0;
  for i in 0..bits.len() {
    res *= 2;
    res += bits[i] as usize;
  }
  res
}

pub fn bits_to_usize_truncated(bits: &Vec<bool>, max_depth: u32) -> usize {
  let mut pow = 1_usize << max_depth;
  let mut res = 0;
  for i in 0..bits.len() {
    pow /= 2;
    res += pow * bits[i] as usize;
  }
  res
}

pub fn u32_to_bits(mut x: usize, n_bits: u32) -> Vec<bool> {
  let mut res = Vec::with_capacity(n_bits as usize);
  let mut m = 1_usize << (n_bits - 1);
  for _ in 0..n_bits {
    if x >= m {
      x -= m;
      res.push(true);
    } else {
      res.push(false);
    }
    m /= 2;
  }
  res
}

pub fn bits_to_bytes(bits: Vec<bool>) -> Vec<u8> {
  let mut res = Vec::new();
  let mut i = 0;
  while i < bits.len() {
    let mut byte = 0 as u8;
    for _ in 0..8 {
      byte *= 2;
      if i < bits.len() {
        byte += bits[i] as u8;
        i += 1;
      }
    }
    res.push(byte);
  }
  return res;
}

pub fn bits_to_string(bits: &Vec<bool>) -> String {
  return bits
    .iter()
    .map(|b| if *b {"1"} else {"0"})
    .collect::<Vec<&str>>()
    .join("");
}

pub fn bytes_to_bits(bytes: [u8; 8]) -> Vec<bool> {
  let mut res = Vec::with_capacity(64);
  for b in &bytes {
    let mut x = b.clone();
    let mut m = 128;
    for _ in 0..8 {
      res.push(x >= m);
      x %= m;
      m /= 2;
    }
  }
  res
}

pub fn u64_to_least_significant_bits(x: u64, n: u32) -> Vec<bool> {
  let mut res = Vec::with_capacity(n as usize);
  for i in 1..n + 1 {
    let shift = n - i;
    res.push((x >> shift) & 1 == 1);
  }
  res
}

pub fn avg_base2_bits(upper: i64, lower: i64) -> f64 {
  let n = u64_diff(upper, lower) as f64 + 1.0;
  let k = n.log2().floor();
  let two_to_k = (2.0 as f64).powf(k);
  let overshoot = n - two_to_k;
  return k + (2.0 * overshoot) / n;
}

pub fn depth_bits(weight: u64, n: usize) -> f64 {
  return -(weight as f64 / n as f64).log2();
}
