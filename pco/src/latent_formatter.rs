use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent, NumberLike};
use crate::{ChunkMeta, Mode};
use std::marker::PhantomData;

pub trait FormatsLatent<L: Latent> {
  fn var_description(&self) -> String;
  fn var_units(&self) -> String;
  fn format(&self, latent: L) -> String;
}

pub type LatentFormatter<L> = Box<dyn FormatsLatent<L>>;

pub(crate) fn match_classic_mode<T: NumberLike>(
  meta: &ChunkMeta<T::L>,
  delta_units: &'static str,
) -> Option<Vec<LatentFormatter<T::L>>> {
  match (meta.mode, meta.delta_encoding_order) {
    (Mode::Classic, 0) => {
      let formatter = Box::new(ClassicFormatter::<T>::default());
      Some(vec![formatter])
    }
    (Mode::Classic, _) => {
      let formatter = centered_delta_formatter("delta".to_string(), delta_units.to_string());
      Some(vec![formatter])
    }
    _ => None,
  }
}

pub fn match_int_modes<L: Latent>(
  meta: &ChunkMeta<L>,
  is_signed: bool,
) -> Option<Vec<LatentFormatter<L>>> {
  match meta.mode {
    Mode::IntMult(base) => {
      let dtype_center = if is_signed { L::MID } else { L::ZERO };
      let mult_center = dtype_center / base;
      let adj_center = dtype_center % base;
      let primary = if meta.delta_encoding_order == 0 {
        // TODO
        Box::new(IntFormatter {
          description: format!("multiplier [x{}]", base),
          units: "x".to_string(),
          center: mult_center,
          is_signed,
        })
      } else {
        centered_delta_formatter(
          format!("multiplier delta [x{}]", base),
          "x".to_string(),
        )
      };
      let secondary = Box::new(IntFormatter {
        description: "adjustment".to_string(),
        units: "".to_string(),
        center: adj_center,
        is_signed: false,
      });
      Some(vec![primary, secondary])
    }
    _ => None,
  }
}

pub fn match_float_modes<F: FloatLike>(
  meta: &ChunkMeta<F::L>,
) -> Option<Vec<LatentFormatter<F::L>>> {
  match meta.mode {
    Mode::FloatMult(base) => {
      let base_string = F::from_latent_ordered(base).to_string();
      let primary: LatentFormatter<F::L> = if meta.delta_encoding_order == 0 {
        Box::new(FloatMultFormatter {
          base_string,
          phantom: PhantomData::<F>,
        })
      } else {
        Box::new(IntFormatter {
          description: format!("multiplier delta [x{}]", base_string),
          units: "x".to_string(),
          center: F::L::MID,
          is_signed: true,
        })
      };
      let secondary = Box::new(IntFormatter {
        description: "adjustment".to_string(),
        units: " ULPs".to_string(),
        center: F::L::MID,
        is_signed: true,
      });
      Some(vec![primary, secondary])
    }
    Mode::FloatQuant(k) => {
      let primary = if meta.delta_encoding_order == 0 {
        Box::new(FloatQuantFormatter {
          k,
          phantom: PhantomData::<F>,
        })
      } else {
        centered_delta_formatter(
          format!("quantums delta [<<{}]", k),
          "q".to_string(),
        )
      };
      let secondary = Box::new(IntFormatter {
        description: "magnitude adjustment".to_string(),
        units: " ULPs".to_string(),
        center: F::L::ZERO,
        is_signed: false,
      });

      Some(vec![primary, secondary])
    }
    _ => None,
  }
}

#[derive(Default)]
struct ClassicFormatter<T: NumberLike>(PhantomData<T>);

impl<T: NumberLike> FormatsLatent<T::L> for ClassicFormatter<T> {
  fn var_description(&self) -> String {
    "primary".to_string()
  }

  fn var_units(&self) -> String {
    "".to_string()
  }

  fn format(&self, latent: T::L) -> String {
    T::from_latent_ordered(latent).to_string()
  }
}

struct IntFormatter<L: Latent> {
  description: String,
  units: String,
  center: L,
  is_signed: bool,
}

impl<L: Latent> FormatsLatent<L> for IntFormatter<L> {
  fn var_description(&self) -> String {
    self.description.to_string()
  }

  fn var_units(&self) -> String {
    self.units.to_string()
  }

  fn format(&self, latent: L) -> String {
    let centered = latent.wrapping_sub(self.center);
    if centered < L::MID || !self.is_signed {
      centered.to_string()
    } else {
      format!("-{}", L::MAX - (centered - L::ONE),)
    }
  }
}

fn centered_delta_formatter<L: Latent>(description: String, units: String) -> LatentFormatter<L> {
  Box::new(IntFormatter {
    description,
    units,
    center: L::MID,
    is_signed: true,
  })
}

struct FloatMultFormatter<F: FloatLike> {
  base_string: String,
  phantom: PhantomData<F>,
}

impl<F: FloatLike> FormatsLatent<F::L> for FloatMultFormatter<F> {
  fn var_description(&self) -> String {
    format!("multiplier [x{}]", self.base_string)
  }

  fn var_units(&self) -> String {
    "x".to_string()
  }

  fn format(&self, latent: F::L) -> String {
    F::int_float_from_latent(latent).to_string()
  }
}

struct FloatQuantFormatter<F: FloatLike> {
  k: Bitlen,
  phantom: PhantomData<F>,
}

impl<F: FloatLike> FormatsLatent<F::L> for FloatQuantFormatter<F> {
  fn var_description(&self) -> String {
    "quantized".to_string()
  }

  fn var_units(&self) -> String {
    "".to_string()
  }

  fn format(&self, latent: F::L) -> String {
    let shifted = latent << self.k;
    if shifted >= F::L::MID {
      F::from_latent_ordered(shifted).to_string()
    } else {
      (-F::from_latent_ordered(F::L::MAX - shifted)).to_string()
    }
  }
}
