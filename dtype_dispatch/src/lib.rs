#![doc = include_str!("../README.md")]

/// Produces two macros: an enum definer and an enum matcher.
///
/// See the crate-level documentation for more info.
#[macro_export]
macro_rules! build_dtype_macros {
    (
        $definer: ident,
        $matcher: ident,
        $constraint: ident,
        {$($variant: ident => $t: ty,)+}$(,)?
    ) => {
        macro_rules! $definer {
            (#[$attrs: meta] $vis: vis $name: ident, $container: ident) => {
                $vis trait Downcast {
                    fn downcast<S: $constraint>(self) -> $container<S>;
                    fn downcast_ref<S: $constraint>(&self) -> &$container<S>;
                    fn downcast_mut<S: $constraint>(&mut self) -> &mut $container<S>;
                }

                #[$attrs]
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
