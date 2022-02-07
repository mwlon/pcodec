use crate::bits::LEFT_MASKS;
use crate::errors::{QCompressError, QCompressResult};
use crate::types::UnsignedLike;
use crate::constants::{BITS_TO_ENCODE_N_ENTRIES, MAX_ENTRIES};

#[derive(Clone)]
pub struct BitWriter {
  bytes: Vec<u8>,
  j: usize,
}

impl Default for BitWriter {
  fn default() -> Self {
    BitWriter {
      bytes: Vec::new(),
      j: 8,
    }
  }
}

impl BitWriter {
  pub fn write_aligned_byte(&mut self, byte: u8) -> QCompressResult<()> {
    self.write_aligned_bytes(&[byte])
  }

  pub fn write_aligned_bytes(&mut self, bytes: &[u8]) -> QCompressResult<()> {
    if self.j == 8 {
      self.bytes.extend(bytes);
      Ok(())
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot write aligned bytes to unaligned bit reader at byte {} bit {}",
        self.bytes.len(),
        self.j,
      )))
    }
  }

  pub fn write_bytes(&mut self, bytes: &[u8]) {
    if self.j == 8 {
      self.bytes.extend(bytes);
    } else {
      for byte in bytes {
        *self.bytes.last_mut().unwrap() |= byte >> self.j;
        self.bytes.push(byte << (8 - self.j));
      }
    }
  }

  pub fn refresh_if_needed(&mut self) {
    if self.j == 8 {
      self.bytes.push(0);
      self.j = 0;
    }
  }

  pub fn write_one(&mut self, b: bool) {
    self.refresh_if_needed();

    if b {
      *self.bytes.last_mut().unwrap() |= 1_u8 << (7 - self.j);
    }

    self.j += 1;
  }

  pub fn write_bits(&mut self, bs: &[bool]) {
    for &b in bs {
      self.write_one(b);
    }
  }

  pub fn write_usize(&mut self, x: usize, n: usize) {
    self.write_diff(x as u64, n);
  }

  pub fn write_diff<Diff: UnsignedLike>(&mut self, x: Diff, n: usize) {
    if n == 0 {
      return;
    }

    self.refresh_if_needed();

    let mut remaining = n;
    let n_plus_j = n + self.j;
    if n_plus_j <= 8 {
      let lshift = 8 - n_plus_j;
      *self.bytes.last_mut().unwrap() |= (x << lshift).last_u8() & LEFT_MASKS[self.j];
      self.j = n_plus_j;
      return;
    } else {
      let rshift = n_plus_j - 8;
      *self.bytes.last_mut().unwrap() |= (x >> rshift).last_u8() & LEFT_MASKS[self.j];
      remaining -= 8 - self.j;
    }

    while remaining > 8 {
      let rshift = remaining - 8;
      self.bytes.push((x >> rshift).last_u8());
      remaining -= 8;
    }

    // now remaining bits <= 8
    let lshift = 8 - remaining;
    self.bytes.push((x << lshift).last_u8());
    self.j = remaining;
  }

  pub fn write_varint(&mut self, mut x: usize, jumpstart: usize) {
    if x > MAX_ENTRIES {
      panic!("unable to encode varint greater than max number of entries");
    }

    self.write_usize(x, jumpstart);
    x >>= jumpstart;
    for _ in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if x > 0 {
        self.write_one(true);
        self.write_one(x & 1 > 0);
        x >>= 1;
      } else {
        break;
      }
    }
    self.write_one(false);
  }

  pub fn finish_byte(&mut self) {
    self.j = 8;
  }

  pub fn assign_usize(&mut self, mut i: usize, mut j: usize, x: usize, n: usize) {
    // not the most efficient implementation but it's ok because we
    // only rarely use this now
    for k in 0..n {
      let b = (x >> (n - k - 1)) & 1 > 0;
      if j == 8 {
        i += 1;
        j = 0;
      }
      let shift = 7 - j;
      let mask = 1_u8 << shift;
      let shifted_bit = (b as u8) << shift;
      if self.bytes[i] & mask != shifted_bit {
        self.bytes[i] ^= shifted_bit;
      }
      j += 1;
    }
  }

  pub fn pop(self) -> Vec<u8> {
    self.bytes
  }

  pub fn size(&self) -> usize {
    self.bytes.len()
  }
}

#[cfg(test)]
mod tests {
  use super::BitWriter;

  #[test]
  fn test_write_bigger_num() {
    let mut writer = BitWriter::default();
    writer.write_bits(&vec![true, true, true, true]);
    writer.write_usize(187, 4);
    let bytes = writer.pop();
    assert_eq!(
      bytes,
      vec![251],
    )
  }

  #[test]
  fn test_long_write() {
    let mut writer = BitWriter::default();
    // 10100000 00001000 00000010 00000001 1
    writer.write_one(true);
    writer.write_usize((1 << 30) + (1 << 20) + (1 << 10) + 3, 32);
    let bytes = writer.pop();
    assert_eq!(
      bytes,
      vec![160, 8, 2, 1, 128]
    );
  }

  #[test]
  fn test_various_writes() {
    let mut writer = BitWriter::default();
    // 10001000 01000000 01111011 10010101 11100101 0101
    writer.write_one(true);
    writer.write_one(false);
    writer.write_usize(33, 8);
    writer.finish_byte();
    writer.write_aligned_byte(123).expect("misaligned");
    writer.write_varint(100, 3);
    writer.write_usize(5, 4);
    writer.write_usize(5, 4);

    let bytes = writer.pop();
    assert_eq!(
      bytes,
      vec![136, 64, 123, 149, 229, 80],
    );
  }

  #[test]
  fn test_assign_usize() {
    let mut writer = BitWriter::default();
    writer.write_usize(0, 24);
    writer.assign_usize(1, 1, 129, 9);
    let bytes = writer.pop();
    assert_eq!(
      bytes,
      vec![0, 32, 64],
    );
  }
}
