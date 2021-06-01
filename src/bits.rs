use std::convert::TryInto;

pub fn byte_to_bits(mut byte: u8) -> [bool; 8] {
  let mut res = [false; 8];
  let mut m = 128 as u8;
  for i in 0..8 {
    res[i] = byte >= m;
    byte %= m;
    m /= 2;
  }
  return res;
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
  let mut pow = (2 as usize).pow(max_depth);
  let mut res = 0;
  for i in 0..bits.len() {
    pow /= 2;
    res += pow * bits[i] as usize;
  }
  res
}

pub fn usize_to_bits(mut x: usize, n_bits: usize) -> Vec<bool> {
  let mut res = Vec::with_capacity(n_bits);
  let mut m = (2 as usize).pow(n_bits as u32 - 1);
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

pub fn u64_diff(upper: i64, lower: i64) -> u64 {
  if lower > upper {
    panic!(format!("bad! {} {}", upper, lower));
  }
  if lower >= 0 {
    return (upper - lower) as u64;
  }
  let pos_lower = lower.abs() as u64;
  if upper >= 0 {
    return (upper as u64) + (pos_lower as u64);
  }
  return pos_lower - (upper.abs() as u64);
}

pub fn i64_bytes_to_bits(bytes: [u8; 8]) -> Vec<bool> {
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

pub fn base2_bits(upper: i64, lower: i64) -> f64 {
  let n = (u64_diff(upper, lower) + 1) as f64;
  let k = n.log2().floor();
  let two_to_k = (2.0 as f64).powf(k);
  let overshoot = n - two_to_k;
  return k + (2.0 * overshoot) / n;
}

pub fn depth_bits(weight: u64, n: usize) -> f64 {
  return -(weight as f64 / n as f64).log2();
}
