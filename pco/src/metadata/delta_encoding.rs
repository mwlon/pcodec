use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::constants::{
  Bitlen, BITS_TO_ENCODE_DELTA_ENCODING_ORDER, BITS_TO_ENCODE_DELTA_ENCODING_VARIANT,
  BITS_TO_ENCODE_LZ_DELTA_STATE_N_LOG, BITS_TO_ENCODE_LZ_DELTA_WINDOW_N_LOG,
};
use crate::data_types::LatentType;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::delta_encoding::DeltaEncoding::*;
use crate::metadata::format_version::FormatVersion;
use crate::metadata::per_latent_var::LatentVarKey;
use std::io::Write;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaConsecutiveConfig {
  pub order: usize,
  pub secondary_uses_delta: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeltaLz77Config {
  pub state_n_log: Bitlen,
  pub window_n_log: Bitlen,
  pub secondary_uses_delta: bool,
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
  /// Encodes the differences between consecutive values (or differences
  /// between those, etc.).
  ///
  /// This order is always positive, between 1 and 7.
  Consecutive(DeltaConsecutiveConfig),
  /// Encodes an extra "lookback" latent variable and the differences
  /// `x[i] - x[i - lookback[i]]` between values.
  ///
  /// The `window_n_log` parameter specifies how large the maximum lookback
  /// can be.
  Lz77(DeltaLz77Config),
}

impl DeltaEncoding {
  unsafe fn read_from_pre_v3(reader: &mut BitReader) -> Self {
    let order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
    match order {
      0 => None,
      _ => Consecutive(DeltaConsecutiveConfig {
        order,
        secondary_uses_delta: false,
      }),
    }
  }

  pub(crate) unsafe fn read_from(
    version: &FormatVersion,
    reader: &mut BitReader,
  ) -> PcoResult<Self> {
    if !version.supports_delta_variants() {
      return Ok(Self::read_from_pre_v3(reader));
    }

    let delta_encoding_variant = reader.read_bitlen(BITS_TO_ENCODE_DELTA_ENCODING_VARIANT);

    let res = match delta_encoding_variant {
      0 => None,
      1 => {
        let order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
        if order == 0 {
          return Err(PcoError::corruption(
            "Consecutive delta encoding must not be 0",
          ));
        } else {
          Consecutive(DeltaConsecutiveConfig {
            order,
            secondary_uses_delta: reader.read_bool(),
          })
        }
      }
      2 => {
        let window_n_log = 1 + reader.read_bitlen(BITS_TO_ENCODE_LZ_DELTA_WINDOW_N_LOG);
        let state_n_log = reader.read_bitlen(BITS_TO_ENCODE_LZ_DELTA_STATE_N_LOG);
        if state_n_log > window_n_log {
          return Err(PcoError::corruption(format!(
            "LZ delta encoding state size log exceeded window size log: {} vs {}",
            state_n_log, window_n_log
          )));
        }
        Lz77(DeltaLz77Config {
          window_n_log,
          state_n_log,
          secondary_uses_delta: reader.read_bool(),
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
    let variant = match self {
      None => 0,
      Consecutive(_) => 1,
      Lz77(_) => 2,
    };
    writer.write_bitlen(
      variant,
      BITS_TO_ENCODE_DELTA_ENCODING_VARIANT,
    );

    match self {
      None => (),
      Consecutive(config) => {
        writer.write_usize(
          config.order,
          BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
        );
        writer.write_bool(config.secondary_uses_delta);
      }
      Lz77(config) => {
        writer.write_bitlen(
          config.window_n_log - 1,
          BITS_TO_ENCODE_LZ_DELTA_WINDOW_N_LOG,
        );
        writer.write_bitlen(
          config.state_n_log,
          BITS_TO_ENCODE_LZ_DELTA_STATE_N_LOG,
        );
        writer.write_bool(config.secondary_uses_delta);
      }
    }
  }

  pub(crate) fn latent_type(&self) -> Option<LatentType> {
    match self {
      None | Consecutive(_) => Option::None,
      Lz77(_) => Some(LatentType::U32),
    }
  }

  pub(crate) fn applies_to_latent_var(&self, key: LatentVarKey) -> bool {
    match (self, key) {
      // We never recursively delta encode.
      (_, LatentVarKey::Delta) => false,
      // We always apply the DeltaEncoding to the primary latents.
      (_, LatentVarKey::Primary) => true,
      (None, LatentVarKey::Secondary) => false,
      (Consecutive(config), LatentVarKey::Secondary) => config.secondary_uses_delta,
      (Lz77(config), LatentVarKey::Secondary) => config.secondary_uses_delta,
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
      Consecutive(config) => config.order,
      Lz77(config) => 1 << config.state_n_log,
    }
  }

  pub(crate) fn exact_bit_size(&self) -> Bitlen {
    let payload_bits = match self {
      None => 0,
      // For consecutive and LZ77, we have a +1 bit for whether the
      // secondary latent is delta-encoded or not.
      Consecutive(_) => BITS_TO_ENCODE_DELTA_ENCODING_ORDER + 1,
      Lz77(_) => {
        // We encode both (window n log) and (state n log)
        BITS_TO_ENCODE_LZ_DELTA_WINDOW_N_LOG * 2 + 1
      }
    };
    BITS_TO_ENCODE_DELTA_ENCODING_VARIANT + payload_bits
  }
}
