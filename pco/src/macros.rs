#![allow(clippy::single_component_path_imports)]

dtype_dispatch::build_dtype_macros!(
  define_latent_enum,
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
