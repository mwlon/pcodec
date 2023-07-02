use crate::bit_writer::BitWriter;
use crate::constants::{Bitlen, MAX_N_STREAMS};
use crate::data_types::UnsignedLike;

#[derive(Clone, Debug)]
pub struct Decomposed<U: UnsignedLike> {
  pub ans_word: usize,
  pub ans_bits: Bitlen,
  pub offset: U,
  pub offset_bits: Bitlen,
}

impl<U: UnsignedLike> Decomposed<U> {
  pub fn write_to(&self, writer: &mut BitWriter) {
    writer.write_usize(self.ans_word, self.ans_bits);
    writer.write_diff(self.offset, self.offset_bits);
  }
}

#[derive(Clone, Debug)]
pub struct StreamSrc<U: UnsignedLike> {
  streams: [Vec<U>; MAX_N_STREAMS],
}

impl<U: UnsignedLike> StreamSrc<U> {
  pub fn new(streams: [Vec<U>; MAX_N_STREAMS]) -> Self {
    Self { streams }
  }

  pub fn stream(&self, stream_idx: usize) -> &[U] {
    &self.streams[stream_idx]
  }

  pub fn stream_mut(&mut self, stream_idx: usize) -> &mut Vec<U> {
    &mut self.streams[stream_idx]
  }
}

#[derive(Clone, Debug)]
pub struct DecomposedSrc<U: UnsignedLike> {
  decomposeds: [Vec<Decomposed<U>>; MAX_N_STREAMS],
  ans_final_states: [usize; MAX_N_STREAMS],
  i: usize,
}

impl<U: UnsignedLike> DecomposedSrc<U> {
  pub fn new(
    decomposeds: [Vec<Decomposed<U>>; MAX_N_STREAMS],
    ans_final_states: [usize; MAX_N_STREAMS],
  ) -> Self {
    Self {
      decomposeds,
      ans_final_states,
      i: 0,
    }
  }

  #[inline]
  pub fn decomposed(&self, stream_idx: usize) -> &Decomposed<U> {
    &self.decomposeds[stream_idx][self.i]
  }

  pub fn n_processed(&self) -> usize {
    self.i
  }

  pub fn incr(&mut self) {
    self.i += 1;
  }

  pub fn ans_final_state(&self, stream_idx: usize) -> usize {
    self.ans_final_states[stream_idx]
  }

  pub fn stream_len(&self, stream_idx: usize) -> usize {
    self.decomposeds[stream_idx].len()
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
