use std::fmt::{Debug, Formatter};
use std::fmt;

use crate::bits;
use crate::errors::QCompressError;

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

#[derive(Clone)]
pub struct BitReader {
  bytes: Vec<u8>,
  i: usize,
  j: usize,
}

impl Debug for BitReader {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    let current_info = if self.i < self.bytes.len() {
      format!(
        "current byte {}",
        self.bytes[self.i],
      )
    } else {
      "OOB".to_string()
    };

    write!(
      f,
      "BitReader(\n\tbyte {}/{} bit {}\n\t{}\n\t)",
      self.i,
      self.bytes.len(),
      self.j,
      current_info,
    )
  }
}

impl From<Vec<u8>> for BitReader {
  fn from(bytes: Vec<u8>) -> BitReader {
    BitReader {
      bytes,
      i: 0,
      j: 0,
    }
  }
}

impl BitReader {
  fn refresh_if_needed(&mut self) {
    if self.j == 8 {
      self.i += 1;
      self.j = 0;
    }
  }

  pub fn read_bytes(&mut self, n: usize) -> Result<&[u8], QCompressError> {
    self.refresh_if_needed();

    if self.j == 0 {
      let res = &self.bytes[self.i..self.i + n];
      self.i += n - 1;
      self.j = 8;
      Ok(res)
    } else {
      Err(QCompressError::MisalignedError {})
    }
  }

  pub fn read(&mut self, n: usize) -> Vec<bool> {
    let mut res = Vec::with_capacity(n);

    // implementation not well optimized because this is only used in reading header
    let mut byte = self.bytes[self.i];
    for _ in 0..n {
      if self.j == 8 {
        self.i += 1;
        self.j = 0;
        byte = self.bytes[self.i];
      }
      res.push(bits::bit_from_byte(byte, self.j));
      self.j += 1;
    }
    res
  }

  pub fn read_u64(&mut self, n: usize) -> u64 {
    if n == 0 {
      return 0;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j < 8 {
      // it's all in the current byte
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
      res
    }
  }

  pub fn read_varint(&mut self, jumpstart: usize) -> usize {
    let mut res = self.read_u64(jumpstart) as usize;
    let mut mask = 1 << jumpstart;
    while self.read_one() {
      if self.read_one() {
        res |= mask;
      }
      mask <<= 1;
    }
    res
  }

  pub fn read_one(&mut self) -> bool {
    self.refresh_if_needed();

    let res = bits::bit_from_byte(self.bytes[self.i], self.j);
    self.j += 1;
    res
  }

  pub fn drain_bytes(&mut self) -> Result<&[u8], QCompressError> {
    if self.j != 0 {
      self.i += 1;
      self.j = 0;
    }
    let n = self.bytes.len() - self.i;
    self.read_bytes(n)
  }
}

#[cfg(test)]
mod tests {
  use crate::BitReader;
  use crate::errors::QCompressError;

  #[test]
  fn test_bit_reader() -> Result<(), QCompressError>{
    // bits: 1001 1010  0110 1011  0010 1101
    let bytes = vec![0x9a, 0x6b, 0x2d];
    let mut bit_reader = BitReader::from(bytes);
    assert_eq!(
      bit_reader.read_bytes(1)?,
      vec![0x9a],
    );
    assert!(!bit_reader.read_one());
    assert!(bit_reader.read_one());
    assert_eq!(
      bit_reader.read(3),
      vec![true, false, true],
    );
    assert_eq!(
      bit_reader.read_u64(2),
      1
    );
    assert_eq!(
      bit_reader.read_u64(3),
      4
    );
    assert_eq!(
      bit_reader.read_varint(2),
      6
    );
    //leaves 1 bit left over
    Ok(())
  }
}
