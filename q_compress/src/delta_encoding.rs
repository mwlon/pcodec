use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::data_types::{NumberLike, SignedLike};
use crate::errors::QCompressResult;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeltaMoments<S: SignedLike> {
  pub moments: Vec<S>,
}

impl<S: SignedLike> DeltaMoments<S> {
  fn new(moments: Vec<S>) -> Self {
    Self {
      moments,
    }
  }

  pub fn parse_from(reader: &mut BitReader, order: usize) -> QCompressResult<Self> {
    let mut moments = Vec::new();
    for _ in 0..order {
      moments.push(S::read_from(reader)?);
    }
    Ok(DeltaMoments {
      moments,
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
  println!("ORDER IS {}", order);
  let mut data_page_moments = vec![Vec::with_capacity(order); data_page_idxs.len()];
  let mut res = nums
    .iter()
    .map(|x| x.to_signed())
    .collect::<Vec<_>>();
  for _ in 0..order {
    for (page_idx, &i) in data_page_idxs.iter().enumerate() {
      data_page_moments[page_idx].push(res.get(i).copied().unwrap_or(T::Signed::ZERO));
    }
    first_order_deltas_in_place(&mut res);
  }
  let moments = data_page_moments.into_iter()
    .map(DeltaMoments::new)
    .collect::<Vec<DeltaMoments<T::Signed>>>();
  (res, moments)
}

pub fn sum_deltas_in_place<S: SignedLike>(
  moment: S,
  deltas: &mut [S],
) {
  deltas[0] = moment;
  for i in 1..deltas.len() {
    deltas[i] = deltas[i].wrapping_add(deltas[i - 1]);
  }
}

pub fn reconstruct_nums<T: NumberLike>(
  delta_moments: &DeltaMoments<T::Signed>,
  u_deltas: &[T::Unsigned],
  n: usize,
) -> (Vec<T>, DeltaMoments<T::Signed>) {
  let order = delta_moments.order();
  let mut signeds = vec![T::Signed::ZERO; u_deltas.len() + order];
  for i in 0..u_deltas.len() {
    signeds[i + order] = T::Signed::from_unsigned(u_deltas[i]);
  }

  let mut new_moments = vec![T::Signed::ZERO; order];
  for o in (0..order).rev() {
    let slice = &mut signeds[o..];
    sum_deltas_in_place(delta_moments.moments[o], slice);
    new_moments[o] = slice.get(n).copied().unwrap_or(T::Signed::ZERO);
  }
  let res = signeds.into_iter().take(n).map(T::from_signed).collect::<Vec<T>>();
  (res, DeltaMoments::new(new_moments))
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_nth_order_deltas() {
    let nums: Vec<u16> = vec![2, 2, 1, u16::MAX, 0, 1];
    let (deltas, moments) = nth_order_deltas(&nums, 2, &vec![0, 3]);
    assert_eq!(deltas, vec![-1, -1, 3, 0]);
    assert_eq!(moments, vec![
      DeltaMoments::new(vec![i16::MIN + 2, 0]),
      DeltaMoments::new(vec![i16::MAX, 1]),
    ]);
  }

  #[test]
  fn test_reconstruct_nums_full() {
    let u_deltas = vec![1_i16, 2, -3].into_iter().map(u16::from_signed).collect::<Vec<u16>>();
    let moments: DeltaMoments<i16> = DeltaMoments::new(vec![77, 1]);

    // full
    let (nums, new_moments) = reconstruct_nums::<i16>(&moments, &u_deltas, 5);
    assert_eq!(nums, vec![77, 78, 80, 84, 85]);
    assert_eq!(new_moments, DeltaMoments::new(vec![0, 0]));

    //partial
    let (nums, new_moments) = reconstruct_nums::<i16>(&moments, &u_deltas, 3);
    assert_eq!(nums, vec![77, 78, 80]);
    assert_eq!(new_moments, DeltaMoments::new(vec![84, 1]));
  }
}
