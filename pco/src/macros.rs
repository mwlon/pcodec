#![allow(clippy::single_component_path_imports)]

dtype_dispatch::build_dtype_macros!(
  #[doc = "\
    Defines enums where each variant holds a `$container<$t>` for each
    `Latent` type $t.
  "]
  #[doc = "\
    Equips each enum with `From<L: Latent>` and `.downcast*::<L: Latent>()`
    functionality.
  "]
  #[macro_export]
  define_latent_enum,

  #[doc = "\
    Matches enums from `define_latent_enum!` and puts their `Latent` type into
    scope.
  "]
  #[macro_export]
  match_latent_enum,

  Latent,
  {
    U16 => u16,
    U32 => u32,
    U64 => u64,
  },
);

pub(crate) use define_latent_enum;
pub(crate) use match_latent_enum;
