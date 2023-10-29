use std::io::Write;

use crate::bit_reader::u64_at;
use crate::bits;
use crate::constants::Bitlen;
use crate::errors::{PcoError, PcoResult};
use crate::read_write_uint::ReadWriteUint;

// TODO One day the naming should probably be more consistent with Read/Write traits.
// Right now "write_*" functions just stage; and "flush" writes to dst.

// TODO this could be split into BitBuffer (no generics) and
// BitWriter (wrapping BitBuffer, generic to W) to reduce binary size

#[inline]
pub fn write_u64_to(x: u64, byte_idx: usize, dst: &mut [u8]) {
  unsafe {
    let target = dst.as_mut_ptr().add(byte_idx) as *mut [u8; 8];
    *target = x.to_le_bytes();
  };
}

#[inline]
pub fn write_uint_to<U: ReadWriteUint, const MAX_EXTRA_U64S: Bitlen>(
  x: U,
  mut byte_idx: usize,
  bits_past_byte: Bitlen,
  n: Bitlen,
  dst: &mut [u8],
) {
  // See bit_reader for an explanation of why this is fast and how it works.
  let x = bits::lowest_bits(x, n);
  write_u64_to(
    u64_at(dst, byte_idx) | (x.to_u64() << bits_past_byte),
    byte_idx,
    dst,
  );
  let mut processed = 56 - bits_past_byte;
  byte_idx += 7;

  for _ in 0..MAX_EXTRA_U64S {
    write_u64_to((x >> processed).to_u64(), byte_idx, dst);
    processed += 64;
    byte_idx += 8;
  }
}

pub struct BitWriter<W: Write> {
  pub buf: Vec<u8>,
  dst: W,
  // pub current_stream: &'a mut [u8], // either dst or extension
  // other_stream: &'a mut [u8],
  // current_is_dst: bool,       // as opposed to extension
  // skipped: usize,             // in extension
  pub stale_byte_idx: usize,
  pub bits_past_byte: Bitlen,
}

impl<W: Write> BitWriter<W> {
  pub fn new(dst: W, size: usize) -> Self {
    Self {
      buf: vec![0; size],
      dst,
      stale_byte_idx: 0,
      bits_past_byte: 0,
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

  fn check_aligned(&self) -> PcoResult<()> {
    if self.bits_past_byte % 8 != 0 {
      return Err(PcoError::invalid_argument(format!(
        "cannot write aligned bytes to unaligned writer ({} bits past byte)",
        self.bits_past_byte,
      )));
    }

    Ok(())
  }

  pub fn write_aligned_bytes(&mut self, bytes: &[u8]) -> PcoResult<()> {
    self.check_aligned()?;
    self.refill();

    let end = bytes.len() + self.stale_byte_idx;
    self.buf[self.stale_byte_idx..end].clone_from_slice(bytes);
    self.stale_byte_idx = end;

    Ok(())
  }

  pub fn write_uint<U: ReadWriteUint>(&mut self, x: U, n: Bitlen) {
    self.refill();
    match U::MAX_EXTRA_U64S {
      0 => write_uint_to::<U, 0>(
        x,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
        &mut self.buf,
      ),
      1 => write_uint_to::<U, 1>(
        x,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
        &mut self.buf,
      ),
      2 => write_uint_to::<U, 2>(
        x,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
        &mut self.buf,
      ),
      _ => panic!(
        "[BitWriter] data type too large (extra u64's {} > 2)",
        U::MAX_EXTRA_U64S
      ),
    }
    self.consume(n);
  }

  pub fn write_usize(&mut self, x: usize, n: Bitlen) {
    self.write_uint(x, n)
  }

  pub fn write_bitlen(&mut self, x: Bitlen, n: Bitlen) {
    self.write_uint(x, n)
  }

  pub fn finish_byte(&mut self) {
    self.stale_byte_idx += bits::ceil_div(self.bits_past_byte as usize, 8);
    self.bits_past_byte = 0;
  }

  pub fn flush(&mut self) -> PcoResult<()> {
    self.refill();
    let n_bytes = self.stale_byte_idx;

    self.dst.write_all(&self.buf[..n_bytes])?;
    self.buf[..n_bytes].fill(0);
    if n_bytes > 0 && self.bits_past_byte > 0 {
      // We need to keep track of the partially initialized byte.
      self.buf[0] = self.buf[n_bytes];
      self.buf[n_bytes] = 0;
    }

    self.stale_byte_idx = 0;
    Ok(())
  }

  pub fn finish(self) -> W {
    self.dst
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  // I find little endian confusing, hence all the comments.
  // All the bytes in comments are written backwards,
  // e.g. 00000001 = 2^7

  #[test]
  fn test_long_uint_writes() -> PcoResult<()> {
    let mut dst = Vec::new();
    let mut writer = BitWriter::new(&mut dst, 30);
    writer.write_uint::<u32>((1 << 9) + (1 << 8) + 1, 9);
    // 10000000 1
    writer.write_uint::<u32>((1 << 16) + (1 << 5), 17);
    // 10000000 10000010 00000000 01
    writer.write_uint::<u32>(1 << 1, 17);
    // 10000000 10000010 00000000 01010000 00000000
    // 000
    writer.flush()?;
    writer.write_uint::<u32>(1 << 1, 13);
    // 10000000 10000010 00000000 01010000 00000000
    // 00001000 00000000
    writer.flush()?;
    writer.write_uint::<u32>((1 << 23) + (1 << 15), 24);
    // 10000000 10000010 00000000 01010000 00000000
    // 00001000 00000000 00000000 00000001 00000001
    writer.flush()?;

    assert_eq!(
      dst,
      vec![1, 65, 0, 10, 0, 16, 0, 0, 128, 128],
    );
    Ok(())
  }
}
