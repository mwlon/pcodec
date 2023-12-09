use std::io::Result;

/// A better trait for buffered reading from a source.
///
/// Supports all of these:
/// * zero-copy reads from data that is already in memory (`&[u8]`)
/// * resizable capacity so `BetterBufRead` can be passed between programs that
///   require reading different lengths of data at once
/// * ensuring that the buffer contains at least the requested length of data
///   (unless we've reached the end of the file)
///
/// In contrast, programs that use a [`BufRead`][std::io::BufRead] often copy
/// to yet another internal buffer in order to guarantee an appropriate size.
/// This reduces performance and inevitably makes implementations more
/// complicated.
pub trait BetterBufRead {
  /// Fills the internal buffer with at least `n_bytes` if possible, or as many
  /// as possible if the end of the file is reached.
  ///
  /// Depending on the implementation, this may return an IO error for either
  /// of these reasons:
  /// * `n_bytes` exceeds the current capacity
  /// * errors crop up when reading from the source
  fn fill_or_eof(&mut self, n_bytes: usize) -> Result<()>;
  /// Returns all data available in memory.
  ///
  /// This may be smaller than the last `n_bytes` read during `fill_or_eof`,
  /// but only if EOF was reached; and it may be larger than that depending on
  /// the implementation.
  fn buffer(&self) -> &[u8];
  /// Advances by `n_bytes`, reducing the size of the available data to read.
  ///
  /// Panics if `n_bytes` is greater than the buffer's length.
  fn consume(&mut self, n_bytes: usize);
  /// Returns the capacity of the internal buffer, if one exists.
  ///
  /// Implementations like `&[u8]` will return None, since they have no
  /// real internal buffer.
  fn capacity(&self) -> Option<usize>;
  /// Modifies capacity of the internal buffer, if one exists.
  ///
  /// It is advisable to set capacity long enough to support any
  /// optimistic reads you need to do and avoid excessively frequent reads,
  /// but short enough that it fits into a cache or doesn't take too much
  /// memory.
  fn resize_capacity(&mut self, desired: usize);
}

impl BetterBufRead for &[u8] {
  #[inline]
  fn fill_or_eof(&mut self, _bytes_requested: usize) -> Result<()> {
    Ok(())
  }

  #[inline]
  fn buffer(&self) -> &[u8] {
    self
  }

  #[inline]
  fn consume(&mut self, n_bytes: usize) {
    *self = &self[n_bytes..];
  }

  #[inline]
  fn capacity(&self) -> Option<usize> {
    None
  }

  #[inline]
  fn resize_capacity(&mut self, _desired: usize) {}
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_slice_reads() {
    let data = vec![0_u8, 1, 2, 3, 4, 5, 6, 7];
    let mut slice = data.as_slice();
    assert_eq!(slice.buffer(), &data);
    slice.consume(1);
    assert_eq!(slice.buffer(), &data[1..]);
    slice.fill_or_eof(33).unwrap();
    slice.resize_capacity(0);
    assert_eq!(slice.buffer(), &data[1..]);
    slice.consume(2);
    assert_eq!(slice.buffer(), &data[3..]);
  }
}
