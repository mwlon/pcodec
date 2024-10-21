use crate::ans::{AnsState, Symbol};
use crate::constants::{Bitlen, Weight, ANS_INTERLEAVING};
use crate::data_types::{Latent, Number};
use crate::metadata::Mode;

#[derive(Clone, Debug)]
pub struct PageInfo {
  pub page_n: usize,
  pub start_idx: usize,
  pub end_idx_per_var: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct DissectedPageVar<L: Latent> {
  // These vecs should have the same length.
  pub ans_vals: Vec<AnsState>,
  pub ans_bits: Vec<Bitlen>,
  pub offsets: Vec<L>,
  pub offset_bits: Vec<Bitlen>,

  pub ans_final_states: [AnsState; ANS_INTERLEAVING],
}

#[derive(Clone, Debug)]
pub struct DissectedPage<L: Latent> {
  pub page_n: usize,
  pub per_latent_var: Vec<DissectedPageVar<L>>, // one per latent variable
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BinCompressionInfo<L: Latent> {
  // weight and upper are only used up through bin optimization, not dissection or writing
  pub weight: Weight,
  pub lower: L,
  pub upper: L,
  pub offset_bits: Bitlen,
  // symbol is also the index of this in the list of optimized compression infos
  pub symbol: Symbol,
}

impl<L: Latent> Default for BinCompressionInfo<L> {
  fn default() -> Self {
    BinCompressionInfo {
      weight: 0,
      lower: L::ZERO,
      upper: L::MAX,
      offset_bits: L::BITS,
      symbol: Symbol::MAX,
    }
  }
}

#[allow(clippy::type_complexity)]
pub(crate) struct Bid<T: Number> {
  pub mode: Mode,
  pub bits_saved_per_num: f64,
  // we include a split_fn since modes like FloatMult can benefit from extra
  // information (inv_base) not captured entirely in the mode.  This extra
  // information is an implementation detail of the compressor, not part of the
  // format itself, and is not / does not need to be known to the decompressor.
  pub split_fn: Box<dyn FnOnce(&[T]) -> Vec<Vec<T::L>>>,
}
