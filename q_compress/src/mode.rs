pub trait Mode: Default {
  const IS_WRAPPED: bool;
  const NAME: &'static str;
}

#[derive(Default)]
pub struct Standalone;

impl Mode for Standalone {
  const IS_WRAPPED: bool = false;
  const NAME: &'static str = "standalone";
}

#[derive(Default)]
pub struct Wrapped;

impl Mode for Wrapped {
  const IS_WRAPPED: bool = true;
  const NAME: &'static str = "wrapped";
}
