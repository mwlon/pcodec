use crate::constants::{Lookback, MAX_LZ_DELTA_LOOKBACK, MAX_LZ_DELTA_LOOKBACK_LOG};
use crate::data_types::Latent;
use crate::metadata::delta_encoding::{DeltaEncoding, DeltaMoments};
use std::cmp::min;
use std::io::Write;
use std::mem::{transmute, MaybeUninit};

#[derive(Clone, Copy, Debug)]
pub enum DeltaStrategy {
  None,
  Consecutive(usize),
  Lz,
}

impl DeltaStrategy {
  pub fn n_moments(&self) -> usize {
    match self {
      Self::None => 0,
      Self::Consecutive(order) => *order,
      Self::Lz => 0,
    }
  }
}

// Without this, deltas in, say, [-5, 5] would be split out of order into
// [U::MAX - 4, U::MAX] and [0, 5].
// This can be used to convert from
// * unsigned deltas -> (effectively) signed deltas; encoding
// * signed deltas -> unsigned deltas; decoding
#[inline(never)]
pub fn toggle_center_in_place<L: Latent>(latents: &mut [L]) {
  for l in latents.iter_mut() {
    *l = l.toggle_center();
  }
}

// CONSECUTIVE
// ===========
fn consecutive_first_order_encode_in_place<L: Latent>(latents: &mut [L]) {
  if latents.is_empty() {
    return;
  }

  for i in 0..latents.len() - 1 {
    latents[i] = latents[i + 1].wrapping_sub(latents[i]);
  }
}

// used for a single page, so we return the delta moments
#[inline(never)]
pub fn consecutive_encode_in_place<L: Latent>(
  mut latents: &mut [L],
  order: usize,
) -> DeltaMoments<L> {
  let mut page_moments = Vec::with_capacity(order);
  // TODO this could be made faster by doing all steps on mini batches
  // of ~512 at a time
  for _ in 0..order {
    page_moments.push(latents.first().copied().unwrap_or(L::ZERO));

    consecutive_first_order_encode_in_place(latents);
    let truncated_len = latents.len().saturating_sub(1);
    latents = &mut latents[0..truncated_len];
  }
  toggle_center_in_place(latents);

  DeltaMoments::new(page_moments)
}

fn consecutive_first_order_decode_in_place<L: Latent>(moment: &mut L, latents: &mut [L]) {
  for delta in latents.iter_mut() {
    let tmp = *delta;
    *delta = *moment;
    *moment = moment.wrapping_add(tmp);
  }
}

// used for a single batch, so we mutate the delta moments
#[inline(never)]
pub fn consecutive_decode_in_place<L: Latent>(
  delta_moments: &mut DeltaMoments<L>,
  latents: &mut [L],
) {
  if delta_moments.order() == 0 {
    // exit early so we don't toggle to signed values
    return;
  }

  toggle_center_in_place(latents);
  for moment in delta_moments.moments.iter_mut().rev() {
    consecutive_first_order_decode_in_place(moment, latents);
  }
}

// LZ
// ==

pub fn get_default_lz_window<L: Latent>() -> [L; MAX_LZ_DELTA_LOOKBACK] {
  core::array::from_fn(|i| {
    L::from_u64(i as u64) * (L::ONE << (L::BITS - MAX_LZ_DELTA_LOOKBACK_LOG))
  })
}

pub fn lz_encode_in_place<L: Latent>(latents: &mut [L]) -> Vec<Lookback> {
  let default_window = get_default_lz_window::<L>();
  let mut lookbacks = vec![MaybeUninit::uninit(); latents.len()];

  for (i, &this) in latents.iter().enumerate() {
    let mut best_lookback = 1;
    let mut best_dist = L::MAX;
    for lookback in 1..=MAX_LZ_DELTA_LOOKBACK {
      let other = if lookback > i {
        default_window[MAX_LZ_DELTA_LOOKBACK + i - lookback]
      } else {
        latents[i - lookback]
      };
      let delta = this.wrapping_sub(other);
      let dist = min(delta, L::ZERO.wrapping_sub(delta));
      best_lookback = if dist < best_dist {
        lookback
      } else {
        best_lookback
      };

      best_dist = min(dist, best_dist);
    }

    latents[i] = this.wrapping_sub(if best_lookback > i {
      default_window[MAX_LZ_DELTA_LOOKBACK + i - best_lookback]
    } else {
      latents[i - best_lookback]
    });
    lookbacks[i] = MaybeUninit::new(best_lookback as Lookback);
  }

  unsafe { transmute(lookbacks) }
}

pub fn lz_decode_in_place<L: Latent>(
  window: &[L; MAX_LZ_DELTA_LOOKBACK],
  lookbacks: &[Lookback],
  deltas: &mut [L],
) {
  for (i, &lookback) in lookbacks.iter().enumerate() {
    let lookback = lookback as usize;
    let other = if lookback > i {
      window[MAX_LZ_DELTA_LOOKBACK + i - lookback]
    } else {
      deltas[i - lookback]
    };
    deltas[i] = deltas[i].wrapping_add(other);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_consecutive_delta_encode_decode() {
    let orig_latents: Vec<u32> = vec![2, 2, 1, u32::MAX, 0];
    let mut deltas = orig_latents.to_vec();
    let order = 2;
    let zero_delta = u32::MID;
    let mut moments = consecutive_encode_in_place(&mut deltas, order);

    // add back some padding we lose during compression
    for _ in 0..order {
      deltas.push(zero_delta);
    }

    consecutive_decode_in_place::<u32>(&mut moments, &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_latents[..3]);

    consecutive_decode_in_place::<u32>(&mut moments, &mut deltas[3..]);
    assert_eq!(&deltas[3..5], &orig_latents[3..5]);
  }

  #[test]
  fn test_lz_encode_decode() {
    let orig_latents = vec![0_u32, 3, 110, 2, 112, 2, 30, 117];
    let mut latents = orig_latents.to_vec();
    let lookbacks = lz_encode_in_place(&mut latents);
    assert_eq!(
      lookbacks,
      vec![MAX_LZ_DELTA_LOOKBACK as Lookback, 1, 1, 2, 2, 2, 1, 3]
    );

    lz_decode_in_place(
      &get_default_lz_window(),
      &lookbacks,
      &mut latents,
    );
    assert_eq!(latents, orig_latents);
  }
}
