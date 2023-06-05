use crate::data_types::UnsignedLike;

// persists for a whole chunk
#[derive(Clone)]
pub struct UnsignedSrc<U: UnsignedLike> {
  // immutable
  unsigneds: Vec<U>,
  adjustments: Vec<U>,
  len: usize,
  // mutable
  i: usize,
}

impl<U: UnsignedLike> UnsignedSrc<U> {
  pub fn new(unsigneds: Vec<U>, adjustments: Vec<U>) -> Self {
    let len = unsigneds.len();
    Self {
      unsigneds,
      adjustments,
      len,
      i: 0,
    }
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

  pub fn complete(&self) -> bool {
    self.i == self.len
  }

  pub fn unsigneds(&self) -> &[U] {
    &self.unsigneds
  }

  pub fn unsigneds_mut(&mut self) -> &mut Vec<U> {
    &mut self.unsigneds
  }
}


// mutable destination for unsigneds and associated information to be written
#[derive(Clone)]
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

  pub fn unsigneds_mut(&self) -> &'a mut [U] {
    self.unsigneds
  }

  pub fn adjustments(&self) -> &'a [U] {
    self.adjustments
  }
}
