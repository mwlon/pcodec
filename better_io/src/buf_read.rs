use std::io::Result;

pub trait BetterBufRead {
  fn fill_or_eof(&mut self, n_bytes: usize) -> Result<()>;
  fn buffer(&self) -> &[u8];
  fn consume(&mut self, n_bytes: usize);
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

  fn consume(&mut self, n_bytes: usize) {
    *self = &self[n_bytes..];
  }

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
