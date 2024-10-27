use crate::constants::DeltaLookback;
use crate::data_types::Latent;
use crate::macros::match_latent_enum;
use crate::metadata::delta_encoding::DeltaLz77Config;
use crate::metadata::dyn_latents::DynLatents;
use crate::metadata::DeltaEncoding;
use std::cmp;
use std::ops::Range;

pub type DeltaState = DynLatents;

#[derive(Clone, Debug, Default)]
pub(crate) struct DeltaMoments<L: Latent>(pub(crate) Vec<L>);

impl<L: Latent> DeltaMoments<L> {
  pub fn order(&self) -> usize {
    self.0.len()
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

fn first_order_encode_consecutive_in_place<L: Latent>(latents: &mut [L]) {
  if latents.is_empty() {
    return;
  }

  for i in (1..latents.len()).rev() {
    latents[i] = latents[i].wrapping_sub(latents[i - 1]);
  }
}

// used for a single page, so we return the delta moments
#[inline(never)]
fn encode_consecutive_in_place<L: Latent>(order: usize, mut latents: &mut [L]) -> Vec<L> {
  // TODO this function could be made faster by doing all steps on mini batches
  // of ~512 at a time
  let mut page_moments = Vec::with_capacity(order);
  for _ in 0..order {
    page_moments.push(latents.first().copied().unwrap_or(L::ZERO));

    first_order_encode_consecutive_in_place(latents);
    let truncated_start = cmp::min(latents.len(), 1);
    latents = &mut latents[truncated_start..];
  }
  toggle_center_in_place(latents);

  page_moments
}

fn first_order_decode_consecutive_in_place<L: Latent>(moment: &mut L, latents: &mut [L]) {
  for delta in latents.iter_mut() {
    let tmp = *delta;
    *delta = *moment;
    *moment = moment.wrapping_add(tmp);
  }
}

// used for a single batch, so we mutate the delta moments
#[inline(never)]
pub(crate) fn decode_consecutive_in_place<L: Latent>(
  delta_moments: &mut DeltaMoments<L>,
  latents: &mut [L],
) {
  toggle_center_in_place(latents);
  for moment in delta_moments.0.iter_mut().rev() {
    first_order_decode_consecutive_in_place(moment, latents);
  }
}

fn choose_lz77_lookbacks<L: Latent>(config: DeltaLz77Config, latents: &[L]) -> Vec<DeltaLookback> {
  let state_n = config.state_n();
  let window_n = config.window_n();
  let mut res = Vec::with_capacity(latents.len() - state_n);
  // TODO make this fast
  for i in state_n..latents.len() {
    // TODO default window
    let l = latents[i];
    let mut best_j = i;
    let mut best_delta = L::MAX;
    for j in (i.saturating_sub(window_n)..i) {
      let other = latents[j];
      let delta = L::min(l.wrapping_sub(other), other.wrapping_sub(l));
      if delta < best_delta {
        best_j = j;
        best_delta = delta;
      }
    }

    best_j = cmp::min(best_j, i - 1);
    res[i - state_n] = (i - best_j) as DeltaLookback;
  }
  res
}

fn encode_lz77_in_place<L: Latent>(
  config: DeltaLz77Config,
  lookbacks: &[L],
  latents: &mut [L],
) -> Vec<L> {
  toggle_center_in_place(latents);
  unimplemented!();
}

pub fn compute_delta_latent_var(
  delta_encoding: DeltaEncoding,
  primary_latents: &mut DynLatents,
  range: Range<usize>,
) -> Option<DynLatents> {
  match delta_encoding {
    DeltaEncoding::None | DeltaEncoding::Consecutive(_) => None,
    DeltaEncoding::Lz77(config) => {
      let res = match_latent_enum!(
        primary_latents,
        DynLatents<L>(inner) => {
          let latents = &mut inner[range];
          DynLatents::new(choose_lz77_lookbacks(config, latents)).unwrap()
        }
      );
      Some(res)
    }
  }
}

pub fn encode_in_place(
  delta_encoding: DeltaEncoding,
  delta_latents: Option<&DynLatents>,
  latents: &mut DynLatents,
) -> DeltaState {
  match_latent_enum!(
    latents,
    DynLatents<L>(inner) => {
      let delta_state = match delta_encoding {
        DeltaEncoding::None => Vec::<L>::new(),
        DeltaEncoding::Consecutive(order) => {
          encode_consecutive_in_place(order, inner)
        }
        DeltaEncoding::Lz77(config) => {
          let lookbacks = delta_latents.unwrap().downcast_ref::<L>().unwrap();
          encode_lz77_in_place(config, lookbacks, inner)
        }
      };
      DynLatents::new(delta_state).unwrap()
    }
  )
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_delta_encode_decode() {
    let orig_latents: Vec<u32> = vec![2, 2, 1, u32::MAX, 0];
    let mut deltas = orig_latents.to_vec();
    let order = 2;
    let zero_delta = u32::MID;
    let mut moments = encode_consecutive_in_place(&mut deltas, order);

    // add back some padding we lose during compression
    for _ in 0..order {
      deltas.push(zero_delta);
    }

    decode_consecutive_in_place::<u32>(&mut moments, &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_latents[..3]);

    decode_consecutive_in_place::<u32>(&mut moments, &mut deltas[3..]);
    assert_eq!(&deltas[3..5], &orig_latents[3..5]);
  }
}
