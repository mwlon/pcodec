use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

#[derive(Clone, Debug)]
pub struct DecomposedUnsigned<U: UnsignedLike> {
  pub ans_word: usize,
  pub ans_bits: Bitlen,
  pub offset: U,
  pub offset_bits: Bitlen,
}

// persists for a whole chunk
#[derive(Clone, Debug)]
pub struct UnsignedSrc<U: UnsignedLike> {
  // immutable
  unsigneds: Vec<U>,
  adjustments: Vec<U>,
  // mutable
  decomposed: Vec<DecomposedUnsigned<U>>,
  i: usize,
}

impl<U: UnsignedLike> UnsignedSrc<U> {
  pub fn new(unsigneds: Vec<U>, adjustments: Vec<U>) -> Self {
    Self {
      unsigneds,
      adjustments,
      decomposed: unsafe {
        let mut res = Vec::with_capacity(unsigneds.len());
        res.set_len(unsigneds.len());
        res
      },
      i: 0,
    }
  }

  pub fn set_decomposed(&mut self, idx: usize, decomposed: DecomposedUnsigned<U>) {
    self.decomposed[idx] = decomposed;
  }

  pub fn decomposed(&self) -> &DecomposedUnsigned<U> {
    &self.decomposed[self.i]
  }

  pub fn unsigned(&self) -> U {
    self.unsigneds[self.i]
  }

  pub fn adjustment(&self) -> U {
    self.adjustments[self.i]
  }

  pub fn idx(&self) -> usize {
    self.i
  }

  pub fn incr(&mut self) {
    self.i += 1;
  }

  pub fn finished_unsigneds(&self) -> bool {
    self.i >= self.unsigneds.len()
  }

  pub fn unsigneds(&self) -> &[U] {
    &self.unsigneds
  }

  pub fn unsigneds_mut(&mut self) -> &mut Vec<U> {
    &mut self.unsigneds
  }
}

// mutable destination for unsigneds and associated information to be written
pub struct UnsignedDst<'a, U: UnsignedLike> {
  // immutable
  unsigneds: &'a mut [U],
  adjustments: &'a mut [U],
  len: usize,
  // mutable
  i: usize,
}

impl<'a, U: UnsignedLike> UnsignedDst<'a, U> {
  pub fn new(unsigneds: &'a mut [U], adjustments: &'a mut [U]) -> Self {
    let len = unsigneds.len();
    assert!(adjustments.len() >= len);
    Self {
      unsigneds,
      adjustments,
      len,
      i: 0,
    }
  }

  pub fn write_unsigned(&mut self, u: U) {
    self.unsigneds[self.i] = u;
  }

  pub fn write_adj(&mut self, u: U) {
    self.adjustments[self.i] = u;
  }

  pub fn n_processed(&self) -> usize {
    self.i
  }

  pub fn incr(&mut self) {
    self.i += 1;
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn remaining(&self) -> usize {
    self.len - self.i
  }

  pub fn decompose(self) -> (&'a mut [U], &'a mut [U]) {
    (self.unsigneds, self.adjustments)
  }
}
