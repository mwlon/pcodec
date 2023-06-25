use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::UnsignedLike;
use crate::errors::QCompressResult;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaMoments<U: UnsignedLike> {
  pub moments: Vec<U>,
}

impl<U: UnsignedLike> DeltaMoments<U> {
  fn new(moments: Vec<U>) -> Self {
    Self { moments }
  }

  pub fn parse_from(reader: &mut BitReader, order: usize) -> QCompressResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(reader.read_uint::<U>(U::BITS)?);
    }
    Ok(DeltaMoments { moments })
  }

  pub fn write_to(&self, writer: &mut BitWriter) {
    for &moment in &self.moments {
      writer.write_diff(moment, U::BITS);
    }
  }

  pub fn order(&self) -> usize {
    self.moments.len()
  }
}

// Without this, deltas in (say) [-5, 5] would be split out of order into
// [U::MAX - 4, U::MAX] and [0, 5].
#[inline(never)]
fn toggle_center_deltas_in_place<U: UnsignedLike>(dest: &mut [U]) {
  for u in dest.iter_mut() {
    *u = u.wrapping_add(U::MID);
  }
}

#[inline(never)]
fn first_order_deltas_in_place<U: UnsignedLike>(dest: &mut Vec<U>) {
  if dest.is_empty() {
    return;
  }

  for i in 0..dest.len() - 1 {
    dest[i] = dest[i + 1].wrapping_sub(dest[i]);
  }
  dest.truncate(dest.len() - 1);
}

pub fn nth_order_deltas<U: UnsignedLike>(
  unsigneds: &mut Vec<U>,
  order: usize,
  data_page_idxs: &[usize],
) -> Vec<DeltaMoments<U>> {
  if order == 0 {
    return data_page_idxs
      .iter()
      .map(|_| DeltaMoments::default())
      .collect();
  }

  let mut data_page_moments = vec![Vec::with_capacity(order); data_page_idxs.len()];
  for _ in 0..order {
    for (page_idx, &i) in data_page_idxs.iter().enumerate() {
      data_page_moments[page_idx].push(unsigneds.get(i).copied().unwrap_or(U::ZERO));
    }
    first_order_deltas_in_place(unsigneds);
  }
  let moments = data_page_moments
    .into_iter()
    .map(DeltaMoments::new)
    .collect::<Vec<_>>();
  toggle_center_deltas_in_place(unsigneds);
  moments
}

fn first_order_reconstruct_in_place<U: UnsignedLike>(moment: &mut U, dest: &mut [U]) {
  for i in 0..dest.len() {
    let tmp = dest[i];
    dest[i] = *moment;
    *moment = moment.wrapping_add(tmp);
  }
}

pub fn reconstruct_in_place<U: UnsignedLike>(delta_moments: &mut DeltaMoments<U>, dest: &mut [U]) {
  if delta_moments.order() == 0 {
    return;
  }

  toggle_center_deltas_in_place(dest);
  for moment in delta_moments.moments.iter_mut().rev() {
    first_order_reconstruct_in_place(moment, dest);
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
    let mut momentss = nth_order_deltas(&mut deltas, order, &[0, 3]);

    // add back some padding we lose during compression
    assert_eq!(deltas.len(), orig_unsigneds.len() - order);
    for _ in 0..order {
      deltas.push(zero_delta);
    }

    reconstruct_in_place::<u32>(&mut momentss[0], &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_unsigneds[..3]);
    assert_eq!(momentss[0], momentss[1]);

    reconstruct_in_place::<u32>(&mut momentss[1], &mut deltas[3..]);
    assert_eq!(&deltas[3..], &orig_unsigneds[3..]);
  }
}
