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
pub fn read_uint<U: ReadWriteUint, const MAX_EXTRA_WORDS: usize>(
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
  padding: usize,             // in extension
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
      padding,
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
    if self.current_is_src && byte_idx + required_padding > self.other_stream.len() + self.padding {
      self.switch_to_extension();
      return Ok(());
    }

    Err(PcoError::insufficient_data(
      "insufficient padding; this is likely either a bug in pco or a result of\
      using too large a custom data type",
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
      0 => read_uint::<U, 0>(
        self.current_stream,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
      ),
      1 => read_uint::<U, 1>(
        self.current_stream,
        self.stale_byte_idx,
        self.bits_past_byte,
        n,
      ),
      2 => read_uint::<U, 2>(
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
        "out of bounds at bit {} / {}",
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

// #[cfg(test)]
// mod tests {
//   use crate::bit_writer::BitWriter;
//   use crate::constants::WORD_BITLEN;
//   use crate::errors::PcoResult;
//
//   use super::BitReader;
//
//   #[test]
//   fn test_bit_reader() -> PcoResult<()> {
//     // bits: 1001 1010  1101 0110  1011 0100
//     let bytes = vec![0x9a, 0xd6, 0xb4];
//     let words = PaddedBytes::from(&bytes);
//     let mut bit_reader = BitReader::from(&words);
//     assert_eq!(bit_reader.read_aligned_bytes(1)?, vec![0x9a],);
//     assert_eq!(bit_reader.bit_idx(), 8);
//     bit_reader.seek_to(10);
//     assert_eq!(bit_reader.bit_idx(), 10);
//     bit_reader.read_uint::<u64>(3); // skip 3 bits
//     assert_eq!(bit_reader.bit_idx(), 13);
//     assert_eq!(
//       bit_reader.read_uint::<u64>(2),
//       2_u64
//     );
//     assert_eq!(bit_reader.bit_idx(), 15);
//     assert_eq!(bit_reader.read_small(3), 1_u32);
//     assert_eq!(bit_reader.bit_idx(), 18);
//     //leaves 1 bit left over
//     Ok(())
//   }
//
//   #[test]
//   fn test_writer_reader() -> PcoResult<()> {
//     let mut writer = BitWriter::default();
//     for i in 1..WORD_BITLEN + 1 {
//       writer.write_usize(i as usize, i)?;
//     }
//     let bytes = writer.drain_bytes();
//     let words = PaddedBytes::from(&bytes);
//     let mut usize_reader = BitReader::from(&words);
//     for i in 1..WORD_BITLEN + 1 {
//       assert_eq!(
//         usize_reader.read_uint::<usize>(i),
//         i as usize
//       );
//     }
//     Ok(())
//   }
// }
