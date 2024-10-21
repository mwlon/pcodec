use crate::dtypes::PcoNumber;
use pco::data_types::{Number, NumberType};
use pco::{define_number_enum, match_number_enum};

fn check_equal<T: PcoNumber>(recovered: &[T], original: &[T]) {
  assert_eq!(recovered.len(), original.len());
  for (i, (x, y)) in recovered.iter().zip(original.iter()).enumerate() {
    assert_eq!(
      x.to_latent_ordered(),
      y.to_latent_ordered(),
      "{} != {} at {}",
      x,
      y,
      i
    );
  }
}

define_number_enum!(
  #[derive()]
  pub NumVec(Vec)
);

impl NumVec {
  pub fn n(&self) -> usize {
    match_number_enum!(
      self,
      NumVec<T>(nums) => { nums.len() }
    )
  }

  pub fn dtype(&self) -> NumberType {
    match_number_enum!(
      self,
      NumVec<T>(_inner) => { NumberType::new::<T>().unwrap() }
    )
  }

  pub fn truncated(&self, limit: usize) -> Self {
    match_number_enum!(
      self,
      NumVec<T>(nums) => { NumVec::new(nums[..limit].to_vec()).unwrap() }
    )
  }

  pub fn check_equal(&self, other: &NumVec) {
    match_number_enum!(
      self,
      NumVec<T>(nums) => {
        let other_nums = other.downcast_ref::<T>();
        assert!(other_nums.is_some(), "NumVecs had mismatched types");
        let other_nums = other_nums.unwrap();
        check_equal(nums, other_nums);
      }
    )
  }
}
