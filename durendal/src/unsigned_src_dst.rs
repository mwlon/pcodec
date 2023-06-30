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
  unsigneds: Vec<U>,
  adjustments: Vec<U>,
  decomposeds: Vec<DecomposedUnsigned<U>>,
  i: usize,
}

impl<U: UnsignedLike> UnsignedSrc<U> {
  pub fn new(unsigneds: Vec<U>, adjustments: Vec<U>) -> Self {
    Self {
      unsigneds,
      adjustments,
      decomposeds: Vec::new(),
      i: 0,
    }
  }

  pub fn set_decomposeds(&mut self, decomposeds: Vec<DecomposedUnsigned<U>>) {
    self.decomposeds = decomposeds;
  }

  pub fn decomposed(&self) -> &DecomposedUnsigned<U> {
    &self.decomposeds[self.i]
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
// Each stream interleaved in the data writes to a corresponding stream here.
// It would be nicer to have a single data member for all the streams, but
// that's not possible because the primary stream may be provided by the user
// for performance reasons (and is therefore in a different memory location).
pub struct UnsignedDst<'a, U: UnsignedLike> {
  primary_stream: &'a mut [U],
  stream1: &'a mut [U],
  len: usize,
  i: usize,
}

impl<'a, U: UnsignedLike> UnsignedDst<'a, U> {
  pub fn new(primary_stream: &'a mut [U], stream1: &'a mut [U]) -> Self {
    let len = primary_stream.len();
    assert!(stream1.len() >= len);
    Self {
      primary_stream,
      stream1,
      len,
      i: 0,
    }
  }

  #[inline]
  pub fn write(&mut self, stream_idx: usize, u: U) {
    match stream_idx {
      0 => self.primary_stream[self.i] = u,
      1 => self.stream1[self.i] = u,
      _ => panic!("invalid stream; should be unreachable"),
    }
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
    (self.primary_stream, self.stream1)
  }
}
