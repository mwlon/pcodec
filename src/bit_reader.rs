use std::cmp::min;
use crate::bits::byte_to_bits;

const LEFT_MASKS: [u8; 8] = [
  0xff,
  0x7f,
  0x3f,
  0x1f,
  0x0f,
  0x07,
  0x03,
  0x01,
];
const RIGHT_MASKS: [u8; 8] = [
  0x00,
  0x80,
  0xc0,
  0xe0,
  0xf0,
  0xf8,
  0xfc,
  0xfe,
];

pub struct BitReader {
  bytes: Vec<u8>,
  current_bits: [bool; 8],
  i: usize,
  j: usize,
}

impl BitReader {
  pub fn new(bytes: Vec<u8>) -> BitReader {
    let current_bits = byte_to_bits(bytes[0]);
    return BitReader {
      bytes,
      current_bits,
      i: 0,
      j: 0,
    }
  }

  #[inline(always)]
  fn refresh_if_needed(&mut self) {
    if self.j == 8 {
      self.i += 1;
      self.current_bits = byte_to_bits(self.bytes[self.i]);
      self.j = 0;
    }
  }

  pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], String> {
    self.refresh_if_needed();

    if self.j == 0 {
      let res = &self.bytes[self.i..self.i + n];
      self.i += n - 1;
      self.j = 8;
      Ok(res)
    } else {
      Err("cannot read_bytes on misaligned bit reader".to_string())
    }
  }

  pub fn read(&mut self, n: usize) -> Vec<bool> {
    let mut res = Vec::with_capacity(n);

    //finish current byte
    let mut m = min(8 - self.j, n);
    if self.j < 8 {
      res.extend(self.current_bits[self.j..self.j + m].iter());
      self.j += m;
    }

    while m < n {
      self.i += 1;
      self.current_bits = byte_to_bits(self.bytes[self.i]);
      let additional = min(8, n - m);
      res.extend(self.current_bits[0..additional].iter());
      m += additional;
      self.j = additional;
    }
    return res;
  }

  pub fn read_u64(&mut self, n: usize) -> u64 {
    if n == 0 {
      return 0;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j < 8 {
      let shift = 8 - n_plus_j;
      let res = ((self.bytes[self.i] & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift) as u64;
      self.j = n_plus_j;
      res
    } else {
      let mut res = 0;
      let mut remaining = n; // how many bits we still need to read
      // let mut s = 0;  // number of bits read into the u64 so far
      remaining -= 8 - self.j;
      res |= ((self.bytes[self.i] & LEFT_MASKS[self.j]) as u64) << remaining;
      while remaining >= 8 {
        self.i += 1;
        remaining -= 8;
        res |= (self.bytes[self.i] as u64) << remaining;
      }
      if remaining > 0 {
        self.i += 1;
        let shift = 8 - remaining;
        res |= ((self.bytes[self.i] & RIGHT_MASKS[remaining]) >> shift) as u64;
        self.j = remaining;
      } else {
        self.j = 8;
      }
      self.current_bits = byte_to_bits(self.bytes[self.i]);
      res
    }
  }

  pub fn read_one(&mut self) -> bool {
    self.refresh_if_needed();

    let res = self.current_bits[self.j];
    self.j += 1;
    res
  }
}
