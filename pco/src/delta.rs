use crate::constants::{Bitlen, DeltaLookback};
use crate::data_types::Latent;
use crate::macros::match_latent_enum;
use crate::metadata::delta_encoding::DeltaLz77Config;
use crate::metadata::dyn_latents::DynLatents;
use crate::metadata::DeltaEncoding;
use crate::FULL_BATCH_N;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::{array, cmp, mem};

pub type DeltaState = DynLatents;

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

// Used for a single page, so we return the delta moments.
// All encode in place functions leave junk data (`order`
// latents in this case) at the front of the latents.
// Using the front instead of the back is preferable because it makes the lz77
// encode function simpler and faster.
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
pub(crate) fn decode_consecutive_in_place<L: Latent>(delta_moments: &mut [L], latents: &mut [L]) {
  toggle_center_in_place(latents);
  for moment in delta_moments.iter_mut().rev() {
    first_order_decode_consecutive_in_place(moment, latents);
  }
}

// there are 3 types of proposed lookbacks:
// * brute force: just try the most recent few latents
// * repeating: try the most recent lookbacks we actually used
// * hash: look up similar values by hash
const PROPOSED_LOOKBACKS: usize = 16;
const BRUTE_LOOKBACKS: usize = 6;
const REPEATING_LOOKBACKS: usize = 4;
// To help locate similar latents for lz77 encoding, we hash each latent at
// different "coarsenesses" and write them into a vector. e.g. a coarseness
// of 8 means that (l >> 8) gets hashed, so we can lookup recent values by
// quotient by 256.
const COARSENESSES: [Bitlen; 2] = [0, 8];

fn lz77_hash_lookup(
  l: u64,
  i: usize,
  hash_table_n: usize,
  window_n: usize,
  idx_hash_table: &mut [usize],
  proposed_lookbacks: &mut [usize; PROPOSED_LOOKBACKS],
) {
  let hash_mask = hash_table_n - 1;
  // might be possible to improve this hash fn
  let hash_fn = |mut x: u64| {
    // constant is roughly 2**64 / phi
    x = (x ^ (x >> 32)).wrapping_mul(11400714819323197441);
    x = x ^ (x >> 32);
    x as usize & hash_mask
  };

  let mut proposal_idx = BRUTE_LOOKBACKS + REPEATING_LOOKBACKS;
  let mut offset = 0;
  for coarseness in COARSENESSES {
    let bucket = l >> coarseness;
    let buckets = [bucket.wrapping_sub(1), bucket, bucket.wrapping_add(1)];
    let hashes = buckets.map(hash_fn);
    for h in hashes {
      let lookback_to_last_instance = unsafe { i - *idx_hash_table.get_unchecked(offset + h) };
      proposed_lookbacks[proposal_idx] = if lookback_to_last_instance <= window_n {
        lookback_to_last_instance
      } else {
        cmp::min(proposal_idx, i)
      };
      proposal_idx += 1;
    }
    let h = hashes[1];
    unsafe {
      *idx_hash_table.get_unchecked_mut(offset + h) = i;
    }
    offset += hash_table_n;
  }
}

fn lz77_compute_goodness<L: Latent>(
  l: L,
  i: usize,
  latents: &[L],
  proposed_lookbacks: &[usize; PROPOSED_LOOKBACKS],
  lookback_counts: &mut [u32],
  goodnesses: &mut [Bitlen; PROPOSED_LOOKBACKS],
) {
  for lookback_idx in 0..PROPOSED_LOOKBACKS {
    let lookback = proposed_lookbacks[lookback_idx];
    let lookback_count = lookback_counts[lookback - 1];
    let other = unsafe { *latents.get_unchecked(i - lookback) };
    let lookback_goodness = Bitlen::BITS - lookback_count.leading_zeros();
    let delta = L::min(l.wrapping_sub(other), other.wrapping_sub(l));
    let delta_goodness = delta.leading_zeros();
    goodnesses[lookback_idx] = lookback_goodness + delta_goodness;
  }
}

fn lz_goodness_argmax(goodnesses: &[Bitlen; PROPOSED_LOOKBACKS]) -> usize {
  let mut best_goodness = goodnesses[0];
  let mut best_idx = 0;

  for (i, &goodness) in goodnesses.iter().enumerate().skip(1) {
    if goodness > best_goodness {
      best_goodness = goodness;
      best_idx = i;
    }
  }

  best_idx
}

#[inline(never)]
fn choose_lz77_lookbacks<L: Latent>(config: DeltaLz77Config, latents: &[L]) -> Vec<DeltaLookback> {
  let state_n = config.state_n();

  if latents.len() <= state_n {
    return vec![];
  }

  let hash_table_n_log = config.window_n_log + 1;
  let hash_table_n = 1 << hash_table_n_log;
  let window_n = config.window_n();
  assert!(
    window_n >= PROPOSED_LOOKBACKS,
    "we do not support tiny windows during compression"
  );

  let mut lookback_counts = vec![1_u32; cmp::min(window_n, latents.len())];
  let mut lookbacks = vec![MaybeUninit::uninit(); latents.len() - state_n];
  let mut idx_hash_table = vec![0_usize; COARSENESSES.len() * hash_table_n];
  let mut proposed_lookbacks = array::from_fn::<_, PROPOSED_LOOKBACKS, _>(|i| (i + 1).min(state_n));
  let mut goodnesses = [0; PROPOSED_LOOKBACKS];
  let mut best_lookback = 1;
  let mut repeating_lookback_idx: usize = 0;
  for i in state_n..latents.len() {
    let l = latents[i];

    let new_brute_lookback = i.min(PROPOSED_LOOKBACKS);
    proposed_lookbacks[new_brute_lookback - 1] = new_brute_lookback;

    lz77_hash_lookup(
      l.to_u64(),
      i,
      hash_table_n,
      window_n,
      &mut idx_hash_table,
      &mut proposed_lookbacks,
    );
    lz77_compute_goodness(
      l,
      i,
      latents,
      &proposed_lookbacks,
      &mut lookback_counts,
      &mut goodnesses,
    );
    let best_lookback_idx = lz_goodness_argmax(&goodnesses);
    let new_best_lookback = proposed_lookbacks[best_lookback_idx];
    if new_best_lookback != best_lookback {
      repeating_lookback_idx += 1;
    }
    proposed_lookbacks[BRUTE_LOOKBACKS + (repeating_lookback_idx) % REPEATING_LOOKBACKS] =
      new_best_lookback;
    best_lookback = new_best_lookback;
    lookbacks[i - state_n] = MaybeUninit::new(best_lookback as DeltaLookback);
    lookback_counts[best_lookback - 1] += 1;
  }

  unsafe { mem::transmute::<Vec<MaybeUninit<DeltaLookback>>, Vec<DeltaLookback>>(lookbacks) }
}

// All encode in place functions leave junk data (`state_n` latents in this
// case) at the front of the latents.
// Using the front instead of the back is preferable because it means we don't
// need an extra copy of the latents in this case.
#[inline(never)]
fn encode_lz77_in_place<L: Latent>(
  config: DeltaLz77Config,
  lookbacks: &[DeltaLookback],
  latents: &mut [L],
) -> Vec<L> {
  let state_n = config.state_n();
  let real_state_n = cmp::min(latents.len(), state_n);
  // TODO make this fast
  for i in (real_state_n..latents.len()).rev() {
    let lookback = lookbacks[i - state_n] as usize;
    latents[i] = latents[i].wrapping_sub(latents[i - lookback])
  }

  let mut state = vec![L::ZERO; state_n];
  state[state_n - real_state_n..].copy_from_slice(&latents[..real_state_n]);

  toggle_center_in_place(latents);

  state
}

pub fn new_lz77_window_buffer_and_pos<L: Latent>(
  config: DeltaLz77Config,
  state: &[L],
) -> (Vec<L>, usize) {
  let window_n = config.window_n();
  let buffer_n = cmp::max(window_n, FULL_BATCH_N) * 2;
  // TODO better default window
  let mut res = vec![L::ZERO; buffer_n];
  res[window_n - state.len()..window_n].copy_from_slice(state);
  (res, window_n)
}

// returns the new position
pub fn decode_lz77_in_place<L: Latent>(
  config: DeltaLz77Config,
  lookbacks: &[DeltaLookback],
  window_buffer_pos: &mut usize,
  window_buffer: &mut [L],
  latents: &mut [L],
) {
  toggle_center_in_place(latents);

  let (window_n, state_n) = (config.window_n(), config.state_n());
  let mut pos = *window_buffer_pos;
  let batch_n = latents.len();
  if pos + batch_n > window_buffer.len() {
    // we need to cycle the buffer
    for i in 0..window_n {
      window_buffer[i] = window_buffer[i + pos - window_n];
    }
    pos = window_n;
  }

  for (i, (&latent, &lookback)) in latents.iter().zip(lookbacks).enumerate() {
    window_buffer[pos + i] = latent.wrapping_add(window_buffer[pos + i - lookback as usize]);
  }

  let new_pos = pos + batch_n;
  latents.copy_from_slice(&window_buffer[pos - state_n..new_pos - state_n]);
  *window_buffer_pos = new_pos;
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
  range: Range<usize>,
  latents: &mut DynLatents,
) -> DeltaState {
  match_latent_enum!(
    latents,
    DynLatents<L>(inner) => {
      let delta_state = match delta_encoding {
        DeltaEncoding::None => Vec::<L>::new(),
        DeltaEncoding::Consecutive(config) => {
          encode_consecutive_in_place(config.order, &mut inner[range])
        }
        DeltaEncoding::Lz77(config) => {
          let lookbacks = delta_latents.unwrap().downcast_ref::<DeltaLookback>().unwrap();
          encode_lz77_in_place(config, lookbacks, &mut inner[range])
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
  fn test_consecutive_encode_decode() {
    let orig_latents: Vec<u32> = vec![2, 2, 1, u32::MAX, 0];
    let mut deltas = orig_latents.clone();
    let order = 2;
    let mut moments = encode_consecutive_in_place(order, &mut deltas);

    // Encoding left junk deltas at the front,
    // but for decoding we need junk deltas at the end.
    let mut deltas_to_decode = Vec::new();
    deltas_to_decode.extend(&deltas[order..]);
    for _ in 0..order {
      deltas_to_decode.push(1337);
    }
    let mut deltas = deltas_to_decode;

    // decode in two parts to show we keep state properly
    decode_consecutive_in_place::<u32>(&mut moments, &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_latents[..3]);

    decode_consecutive_in_place::<u32>(&mut moments, &mut deltas[3..]);
    assert_eq!(&deltas[3..5], &orig_latents[3..5]);
  }

  #[test]
  fn test_lz77_encode_decode() {
    let original_latents = {
      let mut res = vec![100_u32; 100];
      res[1] = 200;
      res[2] = 201;
      res[3] = 202;
      res[5] = 203;
      res[15] = 204;
      res[50] = 205;
      res
    };
    let config = DeltaLz77Config {
      window_n_log: 4,
      state_n_log: 1,
      secondary_uses_delta: false,
    };

    let mut deltas = original_latents.clone();
    let lookbacks = choose_lz77_lookbacks(config, &original_latents);
    assert_eq!(lookbacks[0], 1); // 201 -> 200
    assert_eq!(lookbacks[2], 4); // 0 -> 0
    assert_eq!(lookbacks[13], 10); // 204 -> 203
    assert_eq!(lookbacks[48], 1); // 205 -> 0; 204 was outside window

    let state = encode_lz77_in_place(config, &lookbacks, &mut deltas);
    assert_eq!(state, vec![100, 200]);

    // Encoding left junk deltas at the front,
    // but for decoding we need junk deltas at the end.
    let mut deltas_to_decode = Vec::<u32>::new();
    deltas_to_decode.extend(&deltas[2..]);
    for _ in 0..2 {
      deltas_to_decode.push(1337);
    }

    let (mut window_buffer, mut pos) = new_lz77_window_buffer_and_pos(config, &state);
    assert_eq!(pos, 16);
    decode_lz77_in_place(
      config,
      &lookbacks,
      &mut pos,
      &mut window_buffer,
      &mut deltas_to_decode,
    );
    assert_eq!(deltas_to_decode, original_latents);
    assert_eq!(pos, 16 + original_latents.len());
  }
}
