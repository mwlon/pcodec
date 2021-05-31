use std::cmp::min;
use crate::bits::byte_to_bits;

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

  pub fn read(&mut self, n: usize) -> Vec<bool> {
    let mut res = Vec::with_capacity(n);
    let mut m = 0;
    while m < n {
      let additional = min(8 - self.j, n - m);
      res.extend(self.current_bits[self.j..self.j + additional].iter());
      m += additional;
      self.j += additional;
      if self.j == 8 {
        self.i += 1;
        self.current_bits = byte_to_bits(self.bytes[self.i]);
        self.j = 0;
      }
    }
    return res;
  }

  pub fn read_rest(&mut self) -> Vec<bool> {
    let mut result = self.current_bits[self.j..].to_vec();
    for b in &self.bytes[self.i + 1..] {
      result.extend(&byte_to_bits(*b));
    }
    self.i = self.bytes.len() - 1;
    self.j = 8;
    result
  }
}
