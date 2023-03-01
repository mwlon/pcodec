use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::{NumberLike, SignedLike, UnsignedLike};
use crate::errors::QCompressResult;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeltaMoments<S: SignedLike> {
  pub moments: Vec<S>,
}

impl<S: SignedLike> DeltaMoments<S> {
  fn new(moments: Vec<S>) -> Self {
    Self { moments }
  }

  pub fn parse_from(reader: &mut BitReader, order: usize) -> QCompressResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(S::read_from(reader)?);
    }
    Ok(DeltaMoments { moments })
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

#[inline(never)]
fn nums_to_signeds<T: NumberLike>(nums: &[T]) -> Vec<T::Signed> {
  nums.iter().map(|x| x.to_signed()).collect::<Vec<_>>()
}

#[inline(never)]
fn first_order_deltas_in_place<S: SignedLike>(nums: &mut Vec<S>) {
  if nums.is_empty() {
    return;
  }

  for i in 0..nums.len() - 1 {
    nums[i] = nums[i + 1].wrapping_sub(nums[i]);
  }
  nums.truncate(nums.len() - 1);
}

// only valid for order >= 1
pub fn nth_order_deltas<T: NumberLike>(
  nums: &[T],
  order: usize,
  data_page_idxs: &[usize],
) -> (Vec<T::Signed>, Vec<DeltaMoments<T::Signed>>) {
  let mut data_page_moments = vec![Vec::with_capacity(order); data_page_idxs.len()];
  let mut res = nums_to_signeds(nums);
  for _ in 0..order {
    for (page_idx, &i) in data_page_idxs.iter().enumerate() {
      data_page_moments[page_idx].push(res.get(i).copied().unwrap_or(T::Signed::ZERO));
    }
    first_order_deltas_in_place(&mut res);
  }
  let moments = data_page_moments
    .into_iter()
    .map(DeltaMoments::new)
    .collect::<Vec<DeltaMoments<T::Signed>>>();
  (res, moments)
}

fn reconstruct_nums_w_order<T: NumberLike, const ORDER: usize>(
  delta_moments: &mut DeltaMoments<T::Signed>,
  mut u_deltas: Vec<T::Unsigned>,
  n: usize,
) -> Vec<T> {
  let mut res = Vec::with_capacity(n);
  for _ in 0..ORDER {
    u_deltas.push(T::Unsigned::ZERO);
  }

  let moments = &mut delta_moments.moments;
  for i in 0..n {
    let delta = T::Signed::from_unsigned(u_deltas[i]);
    res.push(T::from_signed(moments[0]));

    for o in 0..ORDER - 1 {
      moments[o] = moments[o].wrapping_add(moments[o + 1]);
    }
    moments[ORDER - 1] = moments[ORDER - 1].wrapping_add(delta);
  }
  res
}

pub fn reconstruct_nums<T: NumberLike>(
  delta_moments: &mut DeltaMoments<T::Signed>,
  u_deltas: Vec<T::Unsigned>,
  n: usize,
) -> Vec<T> {
  match delta_moments.order() {
    1 => reconstruct_nums_w_order::<T, 1>(delta_moments, u_deltas, n),
    2 => reconstruct_nums_w_order::<T, 2>(delta_moments, u_deltas, n),
    3 => reconstruct_nums_w_order::<T, 3>(delta_moments, u_deltas, n),
    4 => reconstruct_nums_w_order::<T, 4>(delta_moments, u_deltas, n),
    5 => reconstruct_nums_w_order::<T, 5>(delta_moments, u_deltas, n),
    6 => reconstruct_nums_w_order::<T, 6>(delta_moments, u_deltas, n),
    7 => reconstruct_nums_w_order::<T, 7>(delta_moments, u_deltas, n),
    _ => panic!("this order should be unreachable"),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_nth_order_deltas() {
    let nums: Vec<u32> = vec![2, 2, 1, u32::MAX, 0, 1];
    let (deltas, moments) = nth_order_deltas(&nums, 2, &vec![0, 3]);
    assert_eq!(deltas, vec![-1, -1, 3, 0]);
    assert_eq!(
      moments,
      vec![
        DeltaMoments::new(vec![i32::MIN + 2, 0]),
        DeltaMoments::new(vec![i32::MAX, 1]),
      ]
    );
  }

  #[test]
  fn test_reconstruct_nums_full() {
    let u_deltas = vec![1_i32, 2, -3]
      .into_iter()
      .map(u32::from_signed)
      .collect::<Vec<u32>>();
    let mut moments: DeltaMoments<i32> = DeltaMoments::new(vec![77, 1]);

    // full
    let mut new_moments = moments.clone();
    let nums = reconstruct_nums::<i32>(&mut new_moments, u_deltas.clone(), 5);
    assert_eq!(nums, vec![77, 78, 80, 84, 85]);

    //partial
    let nums = reconstruct_nums::<i32>(&mut moments, u_deltas.clone(), 3);
    assert_eq!(nums, vec![77, 78, 80]);
    assert_eq!(moments, DeltaMoments::new(vec![84, 1]));

    let nums = reconstruct_nums::<i32>(&mut moments, u_deltas[3..].to_vec(), 2);
    assert_eq!(nums, vec![84, 85]);
  }
}
