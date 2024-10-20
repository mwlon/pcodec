use crate::dtypes::PcoNumberLike;
use pco::data_types::{CoreDataType, NumberLike};
use pco::{define_number_like_enum, match_number_like_enum};

fn check_equal<T: PcoNumberLike>(recovered: &[T], original: &[T]) {
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

define_number_like_enum!(
  #[derive()]
  pub NumVec(Vec)
);

impl NumVec {
  pub fn n(&self) -> usize {
    match_number_like_enum!(
      self,
      NumVec<T>(nums) => { nums.len() }
    )
  }

  pub fn dtype(&self) -> CoreDataType {
    match_number_like_enum!(
      self,
      NumVec<T>(_inner) => { CoreDataType::new::<T>().unwrap() }
    )
  }

  pub fn truncated(&self, limit: usize) -> Self {
    match_number_like_enum!(
      self,
      NumVec<T>(nums) => { NumVec::new(nums[..limit].to_vec()).unwrap() }
    )
  }

  pub fn check_equal(&self, other: &NumVec) {
    match_number_like_enum!(
      self,
      NumVec<T>(nums) => {
        let other_nums = other.downcast_ref::<T>();
        assert!(other_nums.is_some(), "NumVecs had mismatched dtypes");
        let other_nums = other_nums.unwrap();
        check_equal(nums, other_nums);
      }
    )
  }
}
