use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{
  Bitlen, BITS_TO_ENCODE_DELTA_ENCODING_ORDER, BITS_TO_ENCODE_DELTA_ENCODING_VARIANT,
  BITS_TO_ENCODE_LZ_DELTA_N_LOG,
};
use crate::data_types::{Latent, LatentType};
use crate::delta;
use crate::delta::DeltaState;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::delta_encoding::DeltaEncoding::*;
use crate::metadata::format_version::FormatVersion;
use crate::metadata::per_latent_var::LatentVarKey;
use std::io::Write;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaLz77Config {
  pub state_n_log: Bitlen,
  pub window_n_log: Bitlen,
}

impl DeltaLz77Config {
  pub(crate) fn state_n(&self) -> usize {
    1 << self.state_n_log
  }

  pub(crate) fn window_n(&self) -> usize {
    1 << self.window_n_log
  }
}

/// How Pco does
/// [delta encoding](https://en.wikipedia.org/wiki/Delta_encoding) on this
/// chunk.
///
/// Delta encoding optionally takes differences between nearby numbers,
/// greatly reducing the entropy of the data distribution in some cases.
/// This stage of processing happens after applying the
/// [`Mode`][crate::metadata::Mode].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaEncoding {
  /// No delta encoding; the values are encoded as-is.
  ///
  /// This is best if your data is in random order.
  None,
  /// Encodes the differences between values (or differences between those,
  /// etc.).
  ///
  /// This order is always positive, between 1 and 7.
  Consecutive(usize),
  Lz77(DeltaLz77Config),
}

impl DeltaEncoding {
  pub(crate) unsafe fn read_from(
    version: &FormatVersion,
    reader: &mut BitReader,
  ) -> PcoResult<Self> {
    let delta_encoding_variant = if version.supports_delta_variants() {
      reader.read_bitlen(BITS_TO_ENCODE_DELTA_ENCODING_VARIANT)
    } else {
      0
    };

    let res = match delta_encoding_variant {
      0 => {
        let order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
        if order == 0 {
          None
        } else {
          Consecutive(order)
        }
      }
      1 => {
        let window_n_log = 1 + reader.read_bitlen(BITS_TO_ENCODE_LZ_DELTA_N_LOG);
        let state_n_log = reader.read_bitlen(BITS_TO_ENCODE_LZ_DELTA_N_LOG);
        if state_n_log > window_n_log {
          return Err(PcoError::corruption(format!(
            "LZ delta encoding state size log exceeded window size log: {} vs {}",
            state_n_log, window_n_log
          )));
        }
        Lz77(DeltaLz77Config {
          window_n_log,
          state_n_log,
        })
      }
      value => {
        return Err(PcoError::corruption(format!(
          "unknown delta encoding value: {}",
          value
        )))
      }
    };
    Ok(res)
  }

  pub(crate) unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) {
    // Due to historical reasons, None and Consecutive delta encodings are
    // stored as the 0 variant and differentiated by their delta encoding order
    // bits (order in the case of None).
    let variant = match self {
      None | Consecutive(_) => 0,
      Lz77(_) => 1,
    };
    writer.write_bitlen(
      variant,
      BITS_TO_ENCODE_DELTA_ENCODING_VARIANT,
    );

    match self {
      None => writer.write_bitlen(0, BITS_TO_ENCODE_DELTA_ENCODING_ORDER),
      &Consecutive(order) => writer.write_usize(order, BITS_TO_ENCODE_DELTA_ENCODING_ORDER),
      Lz77(config) => {
        writer.write_bitlen(
          config.window_n_log - 1,
          BITS_TO_ENCODE_LZ_DELTA_N_LOG,
        );
        writer.write_bitlen(
          config.state_n_log,
          BITS_TO_ENCODE_LZ_DELTA_N_LOG,
        );
      }
    }
  }

  pub(crate) fn latent_type(&self) -> Option<LatentType> {
    match self {
      None | Consecutive(_) => Option::None,
      Lz77(_) => Some(LatentType::U16),
    }
  }

  pub(crate) fn applies_to_latent_var(&self, key: LatentVarKey) -> bool {
    match key {
      // We never recursively delta encode.
      LatentVarKey::Delta => false,
      // We always apply the DeltaEncoding to the primary latents.
      LatentVarKey::Primary => true,
      // At present we never apply DeltaEncoding to the secondary latents, but
      // this could be changed in the future.
      LatentVarKey::Secondary => false,
    }
  }

  pub(crate) fn for_latent_var(self, key: LatentVarKey) -> DeltaEncoding {
    if self.applies_to_latent_var(key) {
      self
    } else {
      None
    }
  }

  pub(crate) fn n_latents_per_state(&self) -> usize {
    match self {
      None => 0,
      Consecutive(order) => *order,
      Lz77(config) => 1 << config.state_n_log,
    }
  }
}
