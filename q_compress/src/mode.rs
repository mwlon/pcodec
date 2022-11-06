use std::fmt::Debug;

pub trait Mode: Copy + Debug + Default {
  const IS_WRAPPED: bool;
  const NAME: &'static str;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Standalone;

impl Mode for Standalone {
  const IS_WRAPPED: bool = false;
  const NAME: &'static str = "standalone";
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Wrapped;

impl Mode for Wrapped {
  const IS_WRAPPED: bool = true;
  const NAME: &'static str = "wrapped";
}
