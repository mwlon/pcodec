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

#[inline(always)]
pub fn byte_to_bits(byte: u8) -> [bool; 8] {
  let mut res: [bool; 8];
  unsafe {
    res = std::mem::MaybeUninit::uninit().assume_init();
  }
  for i in 0..8 {
    res[i] = (byte & BIT_MASKS[i]) > 0
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
    let mut byte = 0 as u8;
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
  return res;
}

pub fn bits_to_usize_truncated(bits: &Vec<bool>, max_depth: u32) -> usize {
  let pow = 1_usize << (max_depth - 1);
  let mut res = 0;
  for i in 0..bits.len() {
    if bits[i] {
      res |= pow >> i;
    }
  }
  res
}

pub fn usize_to_bits(x: usize, n_bits: u32) -> Vec<bool> {
  u64_to_bits(x as u64, n_bits)
}

pub fn u64_to_bits(x: u64, n_bits: u32) -> Vec<bool> {
  // the least significant bits, but still in bigendian order
  let mut res = Vec::with_capacity(n_bits as usize);
  for i in 1..n_bits + 1 {
    let shift = n_bits - i;
    res.push(((x >> shift) & 1) > 0);
  }
  res
}

pub fn bits_to_string(bits: &Vec<bool>) -> String {
  return bits
    .iter()
    .map(|b| if *b {"1"} else {"0"})
    .collect::<Vec<&str>>()
    .join("");
}

pub fn avg_base2_bits(upper_lower_diff: u64) -> f64 {
  let n = upper_lower_diff as f64 + 1.0;
  let k = n.log2().floor();
  let two_to_k = (2.0 as f64).powf(k);
  let overshoot = n - two_to_k;
  return k + (2.0 * overshoot) / n;
}

pub fn depth_bits(weight: u64, total_weight: usize) -> f64 {
  return -(weight as f64 / total_weight as f64).log2();
}
