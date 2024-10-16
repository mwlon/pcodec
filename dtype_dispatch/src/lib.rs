#![doc = include_str!("../README.md")]

/// Produces two macros: an enum definer and an enum matcher.
///
/// See the crate-level documentation for more info.
#[macro_export]
macro_rules! build_dtype_macros {
  (
    $(#[$definer_attrs: meta])*
    $definer: ident,
    $(#[$matcher_attrs: meta])*
    $matcher: ident,
    $constraint: path,
    {$($variant: ident => $t: ty,)+}$(,)?
  ) => {
    $(#[$definer_attrs])*
    macro_rules! $definer {
      (#[$enum_attrs: meta] $vis: vis $name: ident, $container: ident) => {
        $vis trait Downcast {
          fn downcast<S: $constraint>(self) -> $container<S>;
          fn downcast_ref<S: $constraint>(&self) -> &$container<S>;
          fn downcast_mut<S: $constraint>(&mut self) -> &mut $container<S>;
        }

        #[$enum_attrs]
        $vis enum $name {
          $($variant($container<$t>),)+
        }

        impl<T: $constraint> Downcast for $container<T> {
          fn downcast<S: $constraint>(self) -> $container<S> {
            if std::any::TypeId::of::<S>() == std::any::TypeId::of::<T>() {
              unsafe {
                std::mem::transmute::<_, $container<S>>(self)
              }
            } else {
              panic!(
                "unsafe downcast conversion from {} to {}",
                std::any::type_name::<T>(),
                std::any::type_name::<S>(),
              )
            }
          }

          fn downcast_ref<S: $constraint>(&self) -> &$container<S> {
            if std::any::TypeId::of::<S>() == std::any::TypeId::of::<T>() {
              unsafe {
                std::mem::transmute::<_, &$container<S>>(self)
              }
            } else {
              panic!(
                "unsafe downcast conversion from {} to {}",
                std::any::type_name::<T>(),
                std::any::type_name::<S>(),
              )
            }
          }

          fn downcast_mut<S: $constraint>(&mut self) -> &mut $container<S> {
            if std::any::TypeId::of::<S>() == std::any::TypeId::of::<T>() {
              unsafe {
                std::mem::transmute::<_, &mut $container<S>>(self)
              }
            } else {
              panic!(
                "unsafe downcast conversion from {} to {}",
                std::any::type_name::<T>(),
                std::any::type_name::<S>(),
              )
            }
          }
        }

        impl $name {
          pub fn downcast<S: $constraint>(self) -> $container<S> {
            match self {
              $(
                Self::$variant(inner) => inner.downcast::<S>(),
              )+
            }
          }

          pub fn downcast_ref<S: $constraint>(&self) -> &$container<S> {
            match self {
              $(
                Self::$variant(inner) => inner.downcast_ref::<S>(),
              )+
            }
          }

          pub fn downcast_mut<S: $constraint>(&mut self) -> &mut $container<S> {
            match self {
              $(
                Self::$variant(inner) => inner.downcast_mut::<S>(),
              )+
            }
          }
        }

        impl<S: $constraint> From<$container<S>> for $name {
          fn from(value: $container<S>) -> Self {
            let type_id = std::any::TypeId::of::<S>();
            $(
              if type_id == std::any::TypeId::of::<$t>() {
                return $name::$variant(value.downcast());
              }
            )+
            panic!("unsafe conversion from {}", std::any::type_name::<S>());
          }
        }
      };
    }

    $(#[$matcher_attrs])*
    macro_rules! $matcher {
      ($value: expr, $enum_: ident<$generic: ident>($inner: ident) => $block: block) => {
        match $value {
          $($enum_::$variant($inner) => {
            type $generic = $t;
            $block
          })+
        }
      };
    }
  };
}

#[allow(dead_code)]
#[cfg(test)]
mod tests {
  trait Constraint: 'static {}

  impl Constraint for u16 {}
  impl Constraint for u32 {}
  impl Constraint for u64 {}

  build_dtype_macros!(
    define_enum,
    match_enum,
    crate::tests::Constraint,
    {
      U16 => u16,
      U32 => u32,
      U64 => u64,
    }
  );

  define_enum!(
    #[derive(Clone, Debug)]
    MyEnum,
    Vec
  );

  // we use this helper just to prove that we can handle generic types, not
  // just concrete types
  fn generic_new<T: Constraint>(inner: Vec<T>) -> MyEnum {
    MyEnum::from(inner)
  }

  #[test]
  fn test_end_to_end() {
    let x = generic_new(vec![1_u16, 1, 2, 3, 5]);
    let bit_size = match_enum!(&x, MyEnum<L>(inner) => { inner.len() * L::BITS as usize });
    assert_eq!(bit_size, 80);
    let x = x.downcast::<u16>();
    assert_eq!(x[0], 1);
  }
}
