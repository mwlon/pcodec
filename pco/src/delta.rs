use crate::data_types::Latent;

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

fn first_order_encode_in_place<L: Latent>(latents: &mut [L]) {
  if latents.is_empty() {
    return;
  }

  for i in 0..latents.len() - 1 {
    latents[i] = latents[i + 1].wrapping_sub(latents[i]);
  }
}

// used for a single page, so we return the delta moments
#[inline(never)]
pub(crate) fn encode_in_place<L: Latent>(mut latents: &mut [L], order: usize) -> DeltaMoments<L> {
  // TODO this function could be made faster by doing all steps on mini batches
  // of ~512 at a time
  let mut page_moments = Vec::with_capacity(order);
  for _ in 0..order {
    page_moments.push(latents.first().copied().unwrap_or(L::ZERO));

    first_order_encode_in_place(latents);
    let truncated_len = latents.len().saturating_sub(1);
    latents = &mut latents[0..truncated_len];
  }
  toggle_center_in_place(latents);

  DeltaMoments(page_moments)
}

fn first_order_decode_in_place<L: Latent>(moment: &mut L, latents: &mut [L]) {
  for delta in latents.iter_mut() {
    let tmp = *delta;
    *delta = *moment;
    *moment = moment.wrapping_add(tmp);
  }
}

// used for a single batch, so we mutate the delta moments
#[inline(never)]
pub(crate) fn decode_in_place<L: Latent>(delta_moments: &mut DeltaMoments<L>, latents: &mut [L]) {
  toggle_center_in_place(latents);
  for moment in delta_moments.0.iter_mut().rev() {
    first_order_decode_in_place(moment, latents);
  }
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
    let mut moments = encode_in_place(&mut deltas, order);

    // add back some padding we lose during compression
    for _ in 0..order {
      deltas.push(zero_delta);
    }

    decode_in_place::<u32>(&mut moments, &mut deltas[..3]);
    assert_eq!(&deltas[..3], &orig_latents[..3]);

    decode_in_place::<u32>(&mut moments, &mut deltas[3..]);
    assert_eq!(&deltas[3..5], &orig_latents[3..5]);
  }
}
