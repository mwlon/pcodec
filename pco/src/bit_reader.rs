use std::cmp::{max, min};

use std::mem;

use crate::bits;
use crate::constants::{Bitlen, BYTES_PER_WORD, WORD_BITLEN};
use crate::errors::{PcoError, PcoResult};
use crate::read_write_uint::ReadWriteUint;

pub fn make_extension_for(slice: &[u8], padding: usize) -> Vec<u8> {
  let shared = min(slice.len(), padding);
  let len = shared + padding;
  let mut res = vec![0; len];
  // This copy isn't necessary for BitWriter, which also uses this.
  // Not sure this will ever be a performance issue though.
  res[..shared].copy_from_slice(&slice[slice.len() - shared..]);
  res
}

#[inline]
pub fn word_at(src: &[u8], byte_idx: usize) -> usize {
  let raw_bytes = unsafe { *(src.as_ptr().add(byte_idx) as *const [u8; BYTES_PER_WORD]) };
  usize::from_le_bytes(raw_bytes)
}

#[inline]
pub fn read_uint_at<U: ReadWriteUint, const MAX_EXTRA_WORDS: usize>(
  src: &[u8],
  mut byte_idx: usize,
  bits_past_byte: Bitlen,
  n: Bitlen,
) -> U {
  let mut res = U::from_word(word_at(src, byte_idx) >> bits_past_byte);
  // TODO can I read up the end of the word instead of end - 8?
  let mut processed = min(n, WORD_BITLEN - 8 - bits_past_byte);
  byte_idx += BYTES_PER_WORD - 1;

  for _ in 0..MAX_EXTRA_WORDS {
    res |= U::from_word(word_at(src, byte_idx)) << processed;
    processed = min(n, processed + WORD_BITLEN);
    byte_idx += BYTES_PER_WORD;
  }

  bits::lowest_bits(res, n)
}

pub struct BitReader<'a> {
  pub current_stream: &'a [u8], // either src or extension
  other_stream: &'a [u8],
  current_is_src: bool,       // as opposed to extension
  skipped: usize,             // in extension
  pub stale_byte_idx: usize,  // in current stream
  pub bits_past_byte: Bitlen, // in current stream
}

impl<'a> BitReader<'a> {
  pub fn new(src: &'a [u8], extension: &'a [u8]) -> Self {
    // we assume extension has len min(src.len(), padding) + padding
    // where the first min(src.len(), padding) overlap with src
    let padding = max(
      extension.len() / 2,
      extension.len().saturating_sub(src.len()),
    );
    let skipped = src.len().saturating_sub(padding);
    Self {
      current_stream: src,
      other_stream: extension,
      skipped,
      stale_byte_idx: 0,
      bits_past_byte: 0,
      current_is_src: true,
    }
  }

  pub fn bit_idx(&self) -> usize {
    self.stale_byte_idx * 8 + self.bits_past_byte as usize
  }

  fn byte_idx(&self) -> usize {
    self.bit_idx() / 8
  }

  fn src_bit_idx(&self) -> usize {
    let bit_idx = self.bit_idx();
    if self.current_is_src {
      bit_idx
    } else {
      bit_idx + self.skipped * 8
    }
  }

  fn src_bit_size(&self) -> usize {
    let byte_size = if self.current_is_src {
      self.current_stream.len()
    } else {
      self.other_stream.len()
    };
    byte_size * 8
  }

  fn switch_to_extension(&mut self) {
    assert!(self.current_is_src);
    self.stale_byte_idx -= self.skipped;
    self.current_is_src = false;
    mem::swap(
      &mut self.current_stream,
      &mut self.other_stream,
    );
  }

  pub fn ensure_padded(&mut self, required_padding: usize) -> PcoResult<()> {
    self.check_in_bounds()?;

    let byte_idx = self.byte_idx();
    if byte_idx + required_padding < self.current_stream.len() {
      return Ok(());
    }

    // see if we can switch to the other stream
    if self.current_is_src && byte_idx + required_padding < self.other_stream.len() + self.skipped {
      self.switch_to_extension();
      return Ok(());
    }

    Err(PcoError::insufficient_data(
      "[BitReader] insufficient padding; this is likely either a bug in pco or \
      a result of using too large a custom data type",
    ))
  }

  // Returns the reader's current byte index. Will return an error if the
  // reader is at a misaligned position.
  fn aligned_byte_idx(&self) -> PcoResult<usize> {
    if self.bits_past_byte % 8 == 0 {
      Ok(self.byte_idx())
    } else {
      Err(PcoError::invalid_argument(format!(
        "cannot get aligned byte index on misaligned bit reader (byte {} + {} bits)",
        self.stale_byte_idx, self.bits_past_byte,
      )))
    }
  }

  #[inline]
  fn refill(&mut self) {
    self.stale_byte_idx += (self.bits_past_byte / 8) as usize;
    self.bits_past_byte %= 8;
  }

  #[inline]
  fn consume(&mut self, n: Bitlen) {
    self.bits_past_byte += n;
  }

  pub fn read_aligned_bytes(&mut self, n: usize) -> PcoResult<&'a [u8]> {
    let byte_idx = self.aligned_byte_idx()?;
    let new_byte_idx = byte_idx + n;
    self.stale_byte_idx = new_byte_idx;
    Ok(&self.current_stream[byte_idx..new_byte_idx])
  }

  pub fn read_usize(&mut self, n: Bitlen) -> usize {
    self.read_uint(n)
  }

  pub fn read_bitlen(&mut self, n: Bitlen) -> Bitlen {
    self.read_uint(n)
  }

  pub fn read_uint<U: ReadWriteUint>(&mut self, n: Bitlen) -> U {
    self.refill();
    let res = match U::MAX_EXTRA_WORDS {
      0 => read_uint_at::<U, 0>(
        self.current_stream,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
      ),
      1 => read_uint_at::<U, 1>(
        self.current_stream,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
      ),
      2 => read_uint_at::<U, 2>(
        self.current_stream,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
      ),
      _ => panic!(
        "[BitReader] data type too large (extra words {} > 2)",
        U::MAX_EXTRA_WORDS
      ),
    };
    self.consume(n);
    res
  }

  // TODO should this be used?
  // #[inline]
  // pub fn read_small(&mut self, n: Bitlen) -> AnsState {
  //   self.refill();
  //   let res = read_uint::<AnsState, 0>(
  //     self.current_stream,
  //     self.stale_byte_idx,
  //     self.bits_past_byte,
  //     n,
  //   );
  //   self.consume(n);
  //   res
  // }

  pub fn check_in_bounds(&self) -> PcoResult<()> {
    let src_bit_idx = self.src_bit_idx();
    let src_size = self.src_bit_size();
    if src_bit_idx > src_size {
      return Err(PcoError::insufficient_data(format!(
        "[BitReader] out of bounds at bit {} / {}",
        src_bit_idx, src_size,
      )));
    }
    Ok(())
  }

  // Seek to the end of the byte.
  // Used to skip to the next metadata or body section of the file, since they
  // always start byte-aligned.
  pub fn drain_empty_byte(&mut self, message: &str) -> PcoResult<()> {
    self.check_in_bounds()?;
    self.refill();
    if self.bits_past_byte != 0 {
      if (self.current_stream[self.stale_byte_idx] >> self.bits_past_byte) > 0 {
        return Err(PcoError::corruption(message));
      }
      self.consume(8 - self.bits_past_byte);
    }
    Ok(())
  }

  pub fn bits_consumed(self) -> PcoResult<usize> {
    self.check_in_bounds()?;

    Ok(self.src_bit_idx())
  }

  pub fn bytes_consumed(self) -> PcoResult<usize> {
    if self.bits_past_byte % 8 != 0 {
      panic!("dangling bits remain; this is likely a bug in pco");
    }

    Ok(self.bits_consumed()? / 8)
  }
}

#[cfg(test)]
mod tests {
  use crate::errors::PcoResult;

  use super::*;

  // I find little endian confusing, hence all the comments.
  // All the bytes in comments are written backwards,
  // e.g. 00000001 = 2^7

  #[test]
  fn test_bit_reader() -> PcoResult<()> {
    // 10010001 01100100 00000000 11111111 10000010
    let src = vec![137, 38, 0, 255, 65];
    let ext = make_extension_for(&src, 4);
    assert_eq!(ext, vec![38, 0, 255, 65, 0, 0, 0, 0]);
    let mut reader = BitReader::new(&src, &ext);

    assert_eq!(reader.read_bitlen(4), 9);
    assert!(reader.read_aligned_bytes(1).is_err());
    assert_eq!(reader.read_bitlen(4), 8);
    assert_eq!(reader.read_aligned_bytes(1)?, vec![38]);
    reader.ensure_padded(4)?;
    assert_eq!(reader.read_usize(15), 255 + 65 * 256);
    reader.drain_empty_byte("should be empty")?;
    let consumed = reader.bytes_consumed()?;
    assert_eq!(consumed, 5);
    Ok(())
  }
}
