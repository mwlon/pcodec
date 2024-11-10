use crate::metadata::DynLatents;

#[derive(Clone, Debug)]
pub struct SplitLatents {
  pub primary: DynLatents,
  pub secondary: Option<DynLatents>,
}
