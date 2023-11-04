use std::cmp::min;
use std::mem;

use crate::bits;
use crate::constants::Bitlen;
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

// Q: Why u64?
// A: It's the largest data type most instruction sets have support for (and
//    can do few-cycle/SIMD ops on). e.g. even 32-bit wasm has 64-bit ints and
//    opcodes.
#[inline]
pub fn u64_at(src: &[u8], byte_idx: usize) -> u64 {
  let raw_bytes = unsafe { *(src.as_ptr().add(byte_idx) as *const [u8; 8]) };
  u64::from_le_bytes(raw_bytes)
}

#[inline]
pub fn read_uint_at<U: ReadWriteUint, const MAX_EXTRA_U64S: usize>(
  src: &[u8],
  mut byte_idx: usize,
  bits_past_byte: Bitlen,
  n: Bitlen,
) -> U {
  // Q: Why is this fast?
  // A: The 0..MAX_EXTRA_U64S can be unrolled at compile time and interact
  //    freely with an outer loop, allowing really fast SIMD stuff.
  //
  // Q: Why does this work?
  // A: We set MAX_EXTRA_U64S so that,
  //    0  to 57  bit reads -> 0 extra u64's
  //    58 to 113 bit reads -> 1 extra u64's
  //    113 to 128 bit reads -> 2 extra u64's
  //    During the 1st u64 (prior to the loop), we read all bytes from the
  //    current u64. Due to our bit packing, up to the first 7 of these may
  //    be useless, so we can read up to (64 - 7) = 57 bits safely from a
  //    single u64. We right shift by only up to 7 bits, which is safe.
  //
  //    For the 2nd u64, we skip only 7 bytes forward. This will overlap with
  //    the 1st u64 by 1 byte, which seems useless, but allows us to avoid one
  //    nasty case: left shifting by U::BITS (a panic). This could happen e.g.
  //    with 64-bit reads when we start out byte-aligned (bits_past_byte=0).
  //
  //    For the 3rd u64 and onward, we skip 8 bytes forward. Due to how we
  //    handled the 2nd u64, the most we'll ever need to shift by is
  //    U::BITS - 8, which is safe.
  let mut res = U::from_u64(u64_at(src, byte_idx) >> bits_past_byte);
  let mut processed = min(n, 56 - bits_past_byte);
  byte_idx += 7;

  for _ in 0..MAX_EXTRA_U64S {
    res |= U::from_u64(u64_at(src, byte_idx)) << processed;
    processed += 64;
    byte_idx += 8;
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

    if extension.len() > 2 * src.len() {
      // src doesn't have enough padding even at the start
      Self {
        current_stream: extension,
        other_stream: src,
        skipped: 0,
        stale_byte_idx: 0,
        bits_past_byte: 0,
        current_is_src: false,
      }
    } else {
      let padding = extension.len() / 2;
      let skipped = src.len() - padding;

      Self {
        current_stream: src,
        other_stream: extension,
        skipped,
        stale_byte_idx: 0,
        bits_past_byte: 0,
        current_is_src: true,
      }
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
    self.refill();

    let byte_idx = self.stale_byte_idx;
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
    let res = match U::MAX_EXTRA_U64S {
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
        "[BitReader] data type too large (extra u64's {} > 2)",
        U::MAX_EXTRA_U64S
      ),
    };
    self.consume(n);
    res
  }

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
    Ok(self.bits_consumed()? / 8)
  }

  pub fn aligned_bytes_consumed(self) -> PcoResult<usize> {
    if self.bits_past_byte % 8 != 0 {
      panic!("dangling bits remain; this is likely a bug in pco");
    }

    self.bytes_consumed()
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
    let consumed = reader.aligned_bytes_consumed()?;
    assert_eq!(consumed, 5);
    Ok(())
  }
}
