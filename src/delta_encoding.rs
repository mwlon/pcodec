use std::marker::PhantomData;

use crate::{BitReader, BitWriter};
use crate::data_types::{NumberLike, SignedLike};
use crate::errors::QCompressResult;

#[derive(Clone, Debug, PartialEq)]
pub struct DeltaMoments<T: NumberLike> {
  pub moments: Vec<T::Signed>,
  pub phantom: PhantomData<T>,
}

impl<T: NumberLike> DeltaMoments<T> {
  pub fn from(nums: &[T], order: usize) -> Self {
    let moments = nth_order_moments(nums, order);
    DeltaMoments {
      moments,
      phantom: PhantomData,
    }
  }

  pub fn parse_from(reader: &mut BitReader, order: usize) -> QCompressResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(T::Signed::read_from(reader)?);
    }
    Ok(DeltaMoments {
      moments,
      phantom: PhantomData,
    })
  }

  pub fn write_to(&self, writer: &mut BitWriter) {
    for moment in &self.moments {
      moment.write_to(writer);
    }
  }

  pub fn order(&self) -> usize {
    self.moments.len()
  }
}

fn first_order_deltas_in_place<T: NumberLike<Signed=T> + SignedLike>(nums: &mut Vec<T>) {
  if nums.is_empty() {
    return;
  }

  for i in 0..nums.len() - 1 {
    nums[i] = nums[i + 1].wrapping_sub(nums[i]);
  }
  unsafe {
    nums.set_len(nums.len() - 1);
  }
}

// only valid for order >= 1
pub fn nth_order_deltas<T: NumberLike>(
  nums: &[T],
  order: usize,
) -> Vec<T::Signed> {
  let mut res = nums
    .iter()
    .map(|x| x.to_signed())
    .collect::<Vec<_>>();
  for _ in 0..order {
    first_order_deltas_in_place(&mut res);
  }
  res
}

// this could probably be made faster by instead doing a single pass with
// a short vector of moments
fn nth_order_moments<T: NumberLike>(
  nums: &[T],
  order: usize,
) -> Vec<T::Signed> {
  let limited_nums = if nums.len() <= order {
    nums
  } else {
    &nums[0..order]
  };
  let mut deltas = limited_nums
    .iter()
    .map(|x| x.to_signed())
    .collect::<Vec<_>>();

  let mut res = Vec::new();
  for _ in 0..order {
    if deltas.is_empty() {
      res.push(T::Signed::ZERO);
    } else {
      res.push(deltas[0]);
      first_order_deltas_in_place(&mut deltas);
    }
  }
  res
}

pub fn reconstruct_nums<T: NumberLike>(
  delta_moments: &mut DeltaMoments<T>,
  deltas: &[T::Signed],
  n: usize,
) -> Vec<T> {
  let mut res = Vec::with_capacity(n);
  let order = delta_moments.order();
  let moments = &mut delta_moments.moments;
  for i in 0..n {
    res.push(T::from_signed(moments[0]));
    for o in 0..order - 1 {
      moments[o] = moments[o].wrapping_add(moments[o + 1]);
    }
    if i < deltas.len() {
      moments[order - 1] = moments[order - 1].wrapping_add(deltas[i]);
    }
  }
  res
}
