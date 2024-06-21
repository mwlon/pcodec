use crate::constants::Bitlen;
use crate::data_types::{FloatLike, Latent, NumberLike};
use crate::{ChunkMeta, Mode};
use std::marker::PhantomData;

pub trait DescribeLatent<L: Latent> {
  fn latent_var(&self) -> String;
  fn latent_units(&self) -> String;
  fn latent(&self, latent: L) -> String;
}

pub type LatentDescriber<L> = Box<dyn DescribeLatent<L>>;

pub(crate) fn match_classic_mode<T: NumberLike>(
  meta: &ChunkMeta<T::L>,
  delta_units: &'static str,
) -> Option<Vec<LatentDescriber<T::L>>> {
  match (meta.mode, meta.delta_encoding_order) {
    (Mode::Classic, 0) => {
      let describer = Box::new(ClassicDescriber::<T>::default());
      Some(vec![describer])
    }
    (Mode::Classic, _) => {
      let describer = centered_delta_describer("delta".to_string(), delta_units.to_string());
      Some(vec![describer])
    }
    _ => None,
  }
}

pub(crate) fn match_int_modes<L: Latent>(
  meta: &ChunkMeta<L>,
  is_signed: bool,
) -> Option<Vec<LatentDescriber<L>>> {
  match meta.mode {
    Mode::IntMult(base) => {
      let dtype_center = if is_signed { L::MID } else { L::ZERO };
      let mult_center = dtype_center / base;
      let adj_center = dtype_center % base;
      let primary = if meta.delta_encoding_order == 0 {
        // TODO
        Box::new(IntDescriber {
          description: format!("multiplier [x{}]", base),
          units: "x".to_string(),
          center: mult_center,
          is_signed,
        })
      } else {
        centered_delta_describer(
          format!("multiplier delta [x{}]", base),
          "x".to_string(),
        )
      };
      let secondary = Box::new(IntDescriber {
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

pub(crate) fn match_float_modes<F: FloatLike>(
  meta: &ChunkMeta<F::L>,
) -> Option<Vec<LatentDescriber<F::L>>> {
  match meta.mode {
    Mode::FloatMult(base) => {
      let base_string = F::from_latent_ordered(base).to_string();
      let primary: LatentDescriber<F::L> = if meta.delta_encoding_order == 0 {
        Box::new(FloatMultDescriber {
          base_string,
          phantom: PhantomData::<F>,
        })
      } else {
        Box::new(IntDescriber {
          description: format!("multiplier delta [x{}]", base_string),
          units: "x".to_string(),
          center: F::L::MID,
          is_signed: true,
        })
      };
      let secondary = Box::new(IntDescriber {
        description: "adjustment".to_string(),
        units: " ULPs".to_string(),
        center: F::L::MID,
        is_signed: true,
      });
      Some(vec![primary, secondary])
    }
    Mode::FloatQuant(k) => {
      let primary = if meta.delta_encoding_order == 0 {
        Box::new(FloatQuantDescriber {
          k,
          phantom: PhantomData::<F>,
        })
      } else {
        centered_delta_describer(
          format!("quantums delta [<<{}]", k),
          "q".to_string(),
        )
      };
      let secondary = Box::new(IntDescriber {
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
struct ClassicDescriber<T: NumberLike>(PhantomData<T>);

impl<T: NumberLike> DescribeLatent<T::L> for ClassicDescriber<T> {
  fn latent_var(&self) -> String {
    "primary".to_string()
  }

  fn latent_units(&self) -> String {
    "".to_string()
  }

  fn latent(&self, latent: T::L) -> String {
    T::from_latent_ordered(latent).to_string()
  }
}

struct IntDescriber<L: Latent> {
  description: String,
  units: String,
  center: L,
  is_signed: bool,
}

impl<L: Latent> DescribeLatent<L> for IntDescriber<L> {
  fn latent_var(&self) -> String {
    self.description.to_string()
  }

  fn latent_units(&self) -> String {
    self.units.to_string()
  }

  fn latent(&self, latent: L) -> String {
    let centered = latent.wrapping_sub(self.center);
    if centered < L::MID || !self.is_signed {
      centered.to_string()
    } else {
      format!("-{}", L::MAX - (centered - L::ONE),)
    }
  }
}

fn centered_delta_describer<L: Latent>(description: String, units: String) -> LatentDescriber<L> {
  Box::new(IntDescriber {
    description,
    units,
    center: L::MID,
    is_signed: true,
  })
}

struct FloatMultDescriber<F: FloatLike> {
  base_string: String,
  phantom: PhantomData<F>,
}

impl<F: FloatLike> DescribeLatent<F::L> for FloatMultDescriber<F> {
  fn latent_var(&self) -> String {
    format!("multiplier [x{}]", self.base_string)
  }

  fn latent_units(&self) -> String {
    "x".to_string()
  }

  fn latent(&self, latent: F::L) -> String {
    F::int_float_from_latent(latent).to_string()
  }
}

struct FloatQuantDescriber<F: FloatLike> {
  k: Bitlen,
  phantom: PhantomData<F>,
}

impl<F: FloatLike> DescribeLatent<F::L> for FloatQuantDescriber<F> {
  fn latent_var(&self) -> String {
    "quantized".to_string()
  }

  fn latent_units(&self) -> String {
    "".to_string()
  }

  fn latent(&self, latent: F::L) -> String {
    let shifted = latent << self.k;
    if shifted >= F::L::MID {
      F::from_latent_ordered(shifted).to_string()
    } else {
      (-F::from_latent_ordered(F::L::MAX - shifted)).to_string()
    }
  }
}
