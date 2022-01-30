use std::fmt::{Debug, Formatter};
use std::fmt;

use crate::bits;
use crate::bits::{LEFT_MASKS, RIGHT_MASKS};
use crate::errors::{QCompressError, QCompressResult};
use crate::types::UnsignedLike;
use crate::constants::PREFIX_TABLE_SIZE_LOG;

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

  pub fn read_aligned_bytes(&mut self, n: usize) -> QCompressResult<&[u8]> {
    self.refresh_if_needed();

    if self.j == 0 {
      let res = &self.bytes[self.i..self.i + n];
      self.i += n - 1;
      self.j = 8;
      Ok(res)
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot read aligned bytes on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
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

  // returns (bits read, idx)
  pub fn read_prefix_table_idx(&mut self) -> (usize, usize) {
    let n_plus_j = PREFIX_TABLE_SIZE_LOG + self.j;
    if n_plus_j <= 8 {
      let shift = 8 - n_plus_j;
      let res = (self.bytes[self.i] & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift;
      self.j = n_plus_j;
      (PREFIX_TABLE_SIZE_LOG, res as usize)
    } else {
      let remaining = n_plus_j - 8;
      let mut res = ((self.bytes[self.i] & LEFT_MASKS[self.j]) as usize) << remaining;
      self.i += 1;
      if self.i < self.bytes.len() {
        let shift = 8 - remaining;
        res |= ((self.bytes[self.i] & RIGHT_MASKS[remaining]) >> shift) as usize;
        self.j = remaining;
        (PREFIX_TABLE_SIZE_LOG, res)
      } else {
        self.j = 0;
        (PREFIX_TABLE_SIZE_LOG - remaining, res)
      }
    }
  }

  pub fn read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> Diff {
    if n == 0 {
      return Diff::ZERO;
    }

    self.refresh_if_needed();

    let n_plus_j = n + self.j;
    if n_plus_j <= 8 {
      // it's all in the current byte
      let shift = 8 - n_plus_j;
      let res = Diff::from((self.bytes[self.i] & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift);
      self.j = n_plus_j;
      res
    } else {
      let mut res = Diff::ZERO;
      let mut remaining = n; // how many bits we still need to read
      // let mut s = 0;  // number of bits read into the u64 so far
      remaining -= 8 - self.j;
      res |= Diff::from(self.bytes[self.i] & LEFT_MASKS[self.j]) << remaining;
      while remaining >= 8 {
        self.i += 1;
        remaining -= 8;
        res |= Diff::from(self.bytes[self.i]) << remaining;
      }
      if remaining > 0 {
        self.i += 1;
        let shift = 8 - remaining;
        res |= Diff::from((self.bytes[self.i] & RIGHT_MASKS[remaining]) >> shift);
        self.j = remaining;
      } else {
        self.j = 8;
      }
      res
    }
  }

  pub fn read_usize(&mut self, n: usize) -> usize {
    self.read_diff::<u64>(n) as usize
  }

  pub fn read_varint(&mut self, jumpstart: usize) -> usize {
    let mut res = self.read_diff::<u64>(jumpstart) as usize;
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

  pub fn drain_bytes(&mut self) -> &[u8] {
    if self.j != 0 {
      self.i += 1;
      self.j = 0;
    }
    let n = self.bytes.len() - self.i;
    self.read_aligned_bytes(n).unwrap() // this cannot fail because we just did byte alignment
  }

  // Seek to the end of the byte.
  // Used to skip to the next metadata or body section of the file, since they
  // always start byte-aligned.
  pub fn drain_byte(&mut self) {
    if self.j > 0 {
      self.j = 8;
    }
  }

  pub fn seek_bytes(&mut self, n_bytes: usize) {
    self.j = 8;
    self.i += n_bytes;
  }

  pub fn rewind(&mut self, n: usize) {
    if n > self.j {
      self.i -= 1 + (n - self.j - 1).div_euclid(8);
      self.j = 7 - (n - self.j - 1).rem_euclid(8);
    } else {
      self.j -= n;
    }
  }

  pub fn inds(&self) -> (usize, usize) {
    (self.i, self.j)
  }

  pub fn aligned_byte_ind(&self) -> QCompressResult<usize> {
    if self.j == 0 {
      Ok(self.i)
    } else if self.j == 8 {
      Ok(self.i + 1)
    } else {
      Err(QCompressError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
    }
  }

  pub fn size(&self) -> usize {
    self.bytes.len()
  }
}

#[cfg(test)]
mod tests {
  use crate::BitReader;
  use crate::errors::QCompressResult;

  #[test]
  fn test_bit_reader() -> QCompressResult<()>{
    // bits: 1001 1010  0110 1011  0010 1101
    let bytes = vec![0x9a, 0x6b, 0x2d];
    let mut bit_reader = BitReader::from(bytes);
    assert_eq!(
      bit_reader.read_aligned_bytes(1)?,
      vec![0x9a],
    );
    assert!(!bit_reader.read_one());
    assert!(bit_reader.read_one());
    assert_eq!(
      bit_reader.read(3),
      vec![true, false, true],
    );
    assert_eq!(
      bit_reader.read_diff::<u64>(2),
      1_u64
    );
    assert_eq!(
      bit_reader.read_diff::<u32>(3),
      4_u32
    );
    assert_eq!(
      bit_reader.read_varint(2),
      6
    );
    //leaves 1 bit left over
    Ok(())
  }

  #[test]
  fn test_rewind() {
    let mut reader = BitReader {
      bytes: vec![], // irrelevant
      i: 5,
      j: 3,
    };

    reader.rewind(2);
    assert_eq!(reader.inds(), (5, 1));
    reader.rewind(2);
    assert_eq!(reader.inds(), (4, 7));
    reader.rewind(7);
    assert_eq!(reader.inds(), (4, 0));
    reader.rewind(8);
    assert_eq!(reader.inds(), (3, 0));
    reader.rewind(17);
    assert_eq!(reader.inds(), (0, 7));
  }
}
