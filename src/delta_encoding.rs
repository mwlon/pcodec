use crate::{BitReader, BitWriter};
use crate::types::{NumberLike, SignedLike};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
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

  pub fn parse_from(reader: &mut BitReader, order: usize) -> Self {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(T::Signed::read_from(reader));
    }
    DeltaMoments {
      moments,
      phantom: PhantomData,
    }
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

fn first_order_deltas<T: NumberLike<Signed=T> + SignedLike>(nums: &[T]) -> Vec<T> {
  let new_n = nums.len() - 1;
  let mut res = Vec::with_capacity(new_n);
  for i in 0..new_n {
    res.push(nums[i + 1].wrapping_sub(nums[i]))
  }
  res
}

// only valid for order >= 1
pub fn nth_order_deltas<T: NumberLike>(
  nums: &[T],
  order: usize,
) -> Vec<T::Signed> {
  let signeds = nums
    .iter()
    .map(|x| x.to_signed())
    .collect::<Vec<_>>();
  let mut res = first_order_deltas(&signeds);
  for _ in 0..order - 1 {
    res = first_order_deltas(&res);
  }
  res
}

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
      deltas = first_order_deltas(&deltas);
    }
  }
  res
}

fn apply_first_order_deltas<T: NumberLike + SignedLike>(
  moment: T,
  deltas: &[T],
  n: usize,
) -> Vec<T> {
  let mut res = Vec::with_capacity(n);
  let mut elem = moment;
  res.push(moment);
  for &delta in deltas {
    elem = elem.wrapping_add(delta);
    res.push(elem);
  }
  res
}

pub fn reconstruct_nums<T: NumberLike>(
  delta_moments: &DeltaMoments<T>,
  deltas: &[T::Signed],
  n: usize,
) -> Vec<T> {
  let order = delta_moments.order();
  let mut signeds = deltas.to_vec();
  for i in 0..order {
    let idx = order - i - 1;
    let moment = delta_moments.moments[idx];
    signeds = apply_first_order_deltas(moment, &signeds, n - idx);
  }

  signeds.iter()
    .map(|&s| T::from_signed(s))
    .collect()
}
