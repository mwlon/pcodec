use std::io::Write;

use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::PcoResult;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaMoments<U: UnsignedLike> {
  // length = delta encoding order
  pub moments: Vec<U>,
}

impl<U: UnsignedLike> DeltaMoments<U> {
  fn new(moments: Vec<U>) -> Self {
    Self { moments }
  }

  pub fn parse_from(reader: &mut BitReader, order: usize) -> PcoResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(reader.read_uint::<U>(U::BITS));
    }
    Ok(DeltaMoments { moments })
  }

  pub fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) {
    for &moment in &self.moments {
      writer.write_uint(moment, U::BITS);
    }
  }

  pub fn order(&self) -> usize {
    self.moments.len()
  }
}

// Without this, deltas in, say, [-5, 5] would be split out of order into
// [U::MAX - 4, U::MAX] and [0, 5].
// This can be used to convert from
// * unsigned deltas -> (effectively) signed deltas; encoding
// * signed deltas -> unsigned deltas; decoding
#[inline(never)]
pub fn toggle_center_in_place<U: UnsignedLike>(unsigneds: &mut [U]) {
  for u in unsigneds.iter_mut() {
    *u = u.wrapping_add(U::MID);
  }
}

fn first_order_encode_in_place<U: UnsignedLike>(unsigneds: &mut [U]) {
  if unsigneds.is_empty() {
    return;
  }

  for i in 0..unsigneds.len() - 1 {
    unsigneds[i] = unsigneds[i + 1].wrapping_sub(unsigneds[i]);
  }
}

// used for a single page, so we return the delta moments
#[inline(never)]
pub fn encode_in_place<U: UnsignedLike>(mut latents: &mut [U], order: usize) -> DeltaMoments<U> {
  // TODO this function could be made faster by doing all steps on mini batches
  // of ~512 at a time
  if order == 0 {
    // exit early so we don't toggle to signed values
    return DeltaMoments::default();
  }

  let mut page_moments = Vec::with_capacity(order);
  for _ in 0..order {
    page_moments.push(latents.first().copied().unwrap_or(U::ZERO));

    first_order_encode_in_place(latents);
    let truncated_len = latents.len().saturating_sub(1);
    latents = &mut latents[0..truncated_len];
  }
  toggle_center_in_place(latents);

  DeltaMoments::new(page_moments)
}

fn first_order_decode_in_place<U: UnsignedLike>(moment: &mut U, unsigneds: &mut [U]) {
  for delta in unsigneds.iter_mut() {
    let tmp = *delta;
    *delta = *moment;
    *moment = moment.wrapping_add(tmp);
  }
}

// used for a single batch, so we mutate the delta moments
#[inline(never)]
pub fn decode_in_place<U: UnsignedLike>(delta_moments: &mut DeltaMoments<U>, unsigneds: &mut [U]) {
  if delta_moments.order() == 0 {
    // exit early so we don't toggle to signed values
    return;
  }

  toggle_center_in_place(unsigneds);
  for moment in delta_moments.moments.iter_mut().rev() {
    first_order_decode_in_place(moment, unsigneds);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_delta_encode_decode() {
    let orig_unsigneds: Vec<u32> = vec![2, 2, 1, u32::MAX, 0];
    let mut deltas = orig_unsigneds.to_vec();
    let order = 2;
    let zero_delta = u32::MID;
    let mut moments = encode_in_place(&mut deltas, order);

    // add back some padding we lose during compression
    for _ in 0..order {
      deltas.push(zero_delta);
    }

    decode_in_place::<u32>(&mut moments, &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_unsigneds[..3]);

    decode_in_place::<u32>(&mut moments, &mut deltas[3..]);
    assert_eq!(&deltas[3..5], &orig_unsigneds[3..5]);
  }
}
