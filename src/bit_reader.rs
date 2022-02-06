use std::fmt::{Debug, Formatter};
use std::fmt;

use crate::bits;
use crate::bits::{LEFT_MASKS, RIGHT_MASKS};
use crate::constants::{PREFIX_TABLE_SIZE_LOG, BITS_TO_ENCODE_N_ENTRIES};
use crate::errors::{QCompressError, QCompressResult};
use crate::types::UnsignedLike;

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

  fn byte(&self) -> QCompressResult<u8> {
    if self.i < self.bytes.len() {
      Ok(self.bytes[self.i])
    } else {
      Err(QCompressError::insufficient_data(
        "byte(): reached end of data available to BitReader"
      ))
    }
  }

  fn unchecked_byte(&self) -> u8 {
    self.bytes[self.i]
  }

  pub fn read_aligned_bytes(&mut self, n: usize) -> QCompressResult<&[u8]> {
    self.refresh_if_needed();

    if self.j != 0 {
      Err(QCompressError::invalid_argument(format!(
        "cannot read aligned bytes on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
    } else if self.i + n > self.size() {
      Err(QCompressError::insufficient_data(format!(
        "cannot read {} aligned bytes at byte {} out of {}",
        n,
        self.i,
        self.size(),
      )))
    } else {
      let res = &self.bytes[self.i..self.i + n];
      self.i += n - 1;
      self.j = 8;
      Ok(res)
    }
  }

  pub fn read(&mut self, n: usize) -> QCompressResult<Vec<bool>> {
    let mut res = Vec::with_capacity(n);

    // implementation not well optimized because this is only used in reading header
    let mut byte = self.byte()?;
    for _ in 0..n {
      if self.j == 8 {
        self.i += 1;
        self.j = 0;
        byte = self.byte()?;
      }
      res.push(bits::bit_from_byte(byte, self.j));
      self.j += 1;
    }
    Ok(res)
  }

  // returns (bits read, idx)
  pub fn unchecked_read_prefix_table_idx(&mut self) -> usize {
    self.refresh_if_needed();

    let n_plus_j = PREFIX_TABLE_SIZE_LOG + self.j;
    if n_plus_j <= 8 {
      let shift = 8 - n_plus_j;
      let res = (self.unchecked_byte() & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift;
      self.j = n_plus_j;
      res as usize
    } else {
      let remaining = n_plus_j - 8;
      let mut res = ((self.unchecked_byte() & LEFT_MASKS[self.j]) as usize) << remaining;
      self.i += 1;
      let shift = 8 - remaining;
      res |= ((self.unchecked_byte() & RIGHT_MASKS[remaining]) >> shift) as usize;
      self.j = remaining;
      res
    }
  }

  // returns (bits read, idx)
  pub fn read_prefix_table_idx(&mut self) -> QCompressResult<(usize, usize)> {
    self.refresh_if_needed();

    let n_plus_j = PREFIX_TABLE_SIZE_LOG + self.j;
    if n_plus_j <= 8 {
      let shift = 8 - n_plus_j;
      let res = (self.byte()? & LEFT_MASKS[self.j] & RIGHT_MASKS[n_plus_j]) >> shift;
      self.j = n_plus_j;
      Ok((PREFIX_TABLE_SIZE_LOG, res as usize))
    } else {
      let remaining = n_plus_j - 8;
      let mut res = ((self.byte()? & LEFT_MASKS[self.j]) as usize) << remaining;
      self.i += 1;
      if self.i < self.bytes.len() {
        let shift = 8 - remaining;
        res |= ((self.unchecked_byte() & RIGHT_MASKS[remaining]) >> shift) as usize;
        self.j = remaining;
        Ok((PREFIX_TABLE_SIZE_LOG, res))
      } else {
        self.j = 0;
        Ok((PREFIX_TABLE_SIZE_LOG - remaining, res))
      }
    }
  }

  pub fn unchecked_read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> Diff {
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

  pub fn read_diff<Diff: UnsignedLike>(&mut self, n: usize) -> QCompressResult<Diff> {
    if self.i * 8 + self.j + n > self.bytes.len() * 8 {
      return Err(QCompressError::insufficient_data(
        "read_diff(): reached end of data available to BitReader"
      ))
    }

    Ok(self.unchecked_read_diff::<Diff>(n))
  }

  pub fn read_usize(&mut self, n: usize) -> QCompressResult<usize> {
    Ok(self.read_diff::<u64>(n)? as usize)
  }

  pub fn unchecked_read_varint(&mut self, jumpstart: usize) -> usize {
    let mut res = self.unchecked_read_diff::<u64>(jumpstart) as usize;
    for i in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if self.unchecked_read_one() {
        if self.unchecked_read_one() {
          res |= 1 << i
        }
      } else {
        break;
      }
    }
    res
  }

  pub fn read_varint(&mut self, jumpstart: usize) -> QCompressResult<usize> {
    let mut res = self.read_usize(jumpstart)?;
    for i in jumpstart..BITS_TO_ENCODE_N_ENTRIES {
      if self.read_one()? {
        if self.read_one()? {
          res |= 1 << i
        }
      } else {
        break;
      }
    }
    Ok(res)
  }

  pub fn unchecked_read_one(&mut self) -> bool {
    self.refresh_if_needed();

    let res = bits::bit_from_byte(self.unchecked_byte(), self.j);
    self.j += 1;
    res
  }

  pub fn read_one(&mut self) -> QCompressResult<bool> {
    self.refresh_if_needed();

    let res = bits::bit_from_byte(self.byte()?, self.j);
    self.j += 1;
    Ok(res)
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
  pub fn drain_empty_byte<F>(&mut self, f: F) -> QCompressResult<()>
  where F: FnOnce() -> QCompressError {
    if self.j > 0 {
      if self.byte()? & LEFT_MASKS[self.j] > 0 {
        return Err(f());
      }
      self.j = 8;
    }
    Ok(())
  }

  pub fn seek_aligned_bytes(&mut self, n_bytes: usize) -> QCompressResult<()> {
    self.refresh_if_needed();

    if self.j != 0 {
      Err(QCompressError::invalid_argument(format!(
        "cannot seek aligned bytes on misaligned bit reader at byte {} bit {}",
        self.i,
        self.j,
      )))
    } else if self.i + n_bytes >= self.bytes.len() {
      Err(QCompressError::insufficient_data(
        "seek_aligned_bytes(): reached end of data available to BitReader"
      ))
    } else {
      self.i += n_bytes;
      Ok(())
    }
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
    assert!(!bit_reader.unchecked_read_one());
    assert!(bit_reader.read_one()?);
    assert_eq!(
      bit_reader.read(3)?,
      vec![true, false, true],
    );
    assert_eq!(
      bit_reader.unchecked_read_diff::<u64>(2),
      1_u64
    );
    assert_eq!(
      bit_reader.unchecked_read_diff::<u32>(3),
      4_u32
    );
    assert_eq!(
      bit_reader.unchecked_read_varint(2),
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
