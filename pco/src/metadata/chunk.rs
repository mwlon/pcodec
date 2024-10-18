use std::io::Write;

use better_io::BetterBufRead;

use crate::bit_reader::BitReaderBuilder;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::Latent;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::chunk_latent_var::ChunkLatentVarMeta;
use crate::metadata::delta_encoding::DeltaEncoding;
use crate::metadata::dyn_latent::DynLatent;
use crate::metadata::format_version::FormatVersion;
use crate::metadata::Mode;

/// The metadata of a pco chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChunkMeta {
  /// The formula `pco` used to compress each number at a low level.
  pub mode: Mode,
  /// How many times delta encoding was applied during compression.
  /// This is between 0 and 7, inclusive.
  ///
  /// See [`ChunkConfig`][crate::ChunkConfig] for more details.
  pub delta_encoding: DeltaEncoding,
  /// Metadata about the interleaved streams needed by `pco` to
  /// compress/decompress the inputs
  /// according to the formula used by `mode`.
  pub per_latent_var: Vec<ChunkLatentVarMeta>,
}

impl ChunkMeta {
  pub(crate) fn new(
    mode: Mode,
    delta_encoding: DeltaEncoding,
    per_latent_var: Vec<ChunkLatentVarMeta>,
  ) -> Self {
    ChunkMeta {
      mode,
      delta_encoding,
      per_latent_var,
    }
  }

  pub(crate) fn exact_size(&self) -> usize {
    let extra_bits_for_mode = match self.mode {
      Mode::Classic => 0,
      Mode::IntMult(inner) => inner.bits(),
      Mode::FloatMult(inner) => inner.bits(),
      Mode::FloatQuant(_) => BITS_TO_ENCODE_QUANTIZE_K,
    };
    let bits_for_latent_vars: usize = self
      .per_latent_var
      .iter()
      .map(ChunkLatentVarMeta::exact_bit_size)
      .sum();
    let n_bits = BITS_TO_ENCODE_MODE as usize
      + extra_bits_for_mode as usize
      + BITS_TO_ENCODE_DELTA_ENCODING_ORDER as usize
      + bits_for_latent_vars;
    n_bits.div_ceil(8)
  }

  pub(crate) fn exact_page_meta_size(&self) -> usize {
    let bit_size: usize = self
      .per_latent_var
      .iter()
      .enumerate()
      .map(|(latent_var_idx, latent_var)| {
        let delta_encoding = self
          .mode
          .delta_encoding_for_latent_var(latent_var_idx, self.delta_encoding);
        latent_var.exact_page_meta_bit_size(delta_encoding)
      })
      .sum();
    bit_size.div_ceil(8)
  }

  pub(crate) unsafe fn read_from<L: Latent, R: BetterBufRead>(
    reader_builder: &mut BitReaderBuilder<R>,
    version: &FormatVersion,
  ) -> PcoResult<Self> {
    let (mode, delta_encoding) = reader_builder.with_reader(|reader| {
      let mode = match reader.read_usize(BITS_TO_ENCODE_MODE) {
        0 => Ok(Mode::Classic),
        1 => {
          if version.used_old_gcds() {
            return Err(PcoError::compatibility(
              "unable to decompress data from v0.0.0 of pco with different GCD encoding",
            ));
          }

          let base = DynLatent::read_uncompressed_from::<L>(reader);
          Ok(Mode::IntMult(base))
        }
        2 => {
          let base_latent = DynLatent::read_uncompressed_from::<L>(reader);
          Ok(Mode::FloatMult(base_latent))
        }
        3 => {
          let k = reader.read_bitlen(BITS_TO_ENCODE_QUANTIZE_K);
          Ok(Mode::FloatQuant(k))
        }
        value => Err(PcoError::corruption(format!(
          "unknown mode value {}",
          value
        ))),
      }?;

      let delta_encoding_order = reader.read_usize(BITS_TO_ENCODE_DELTA_ENCODING_ORDER);
      let delta_encoding = if delta_encoding_order == 0 {
        DeltaEncoding::None
      } else {
        DeltaEncoding::Consecutive(delta_encoding_order)
      };

      Ok((mode, delta_encoding))
    })?;

    let n_latent_vars = mode.n_latent_vars();

    let mut per_latent_var = Vec::with_capacity(n_latent_vars);

    for _ in 0..n_latent_vars {
      per_latent_var.push(ChunkLatentVarMeta::read_from::<L, R>(
        reader_builder,
      )?)
    }

    reader_builder.with_reader(|reader| {
      reader.drain_empty_byte("nonzero bits in end of final byte of chunk metadata")
    })?;

    Ok(Self {
      mode,
      delta_encoding,
      per_latent_var,
    })
  }

  pub(crate) unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) -> PcoResult<()> {
    let mode_value = match self.mode {
      Mode::Classic => 0,
      Mode::IntMult(_) => 1,
      Mode::FloatMult { .. } => 2,
      Mode::FloatQuant { .. } => 3,
    };
    writer.write_usize(mode_value, BITS_TO_ENCODE_MODE);
    match self.mode {
      Mode::Classic => (),
      Mode::IntMult(base) => {
        base.write_uncompressed_to(writer);
      }
      Mode::FloatMult(base_latent) => {
        base_latent.write_uncompressed_to(writer);
      }
      Mode::FloatQuant(k) => {
        writer.write_uint(k, BITS_TO_ENCODE_QUANTIZE_K);
      }
    };

    match self.delta_encoding {
      DeltaEncoding::None => writer.write_usize(0, BITS_TO_ENCODE_DELTA_ENCODING_ORDER),
      DeltaEncoding::Consecutive(order) => {
        writer.write_usize(order, BITS_TO_ENCODE_DELTA_ENCODING_ORDER)
      }
    }

    writer.flush()?;

    for latents in &self.per_latent_var {
      latents.write_to(writer)?;
    }

    writer.finish_byte();
    writer.flush()?;
    Ok(())
  }

  pub(crate) fn delta_encoding_for_latent_var(&self, latent_idx: usize) -> DeltaEncoding {
    self
      .mode
      .delta_encoding_for_latent_var(latent_idx, self.delta_encoding)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::macros::match_latent_enum;
  use crate::metadata::dyn_bins::DynBins;
  use crate::metadata::dyn_latents::DynLatents;
  use crate::metadata::page::PageMeta;
  use crate::metadata::page_latent_var::PageLatentVarMeta;
  use crate::metadata::Bin;

  fn check_exact_sizes(meta: &ChunkMeta) -> PcoResult<()> {
    let buffer_size = 8192;
    let mut dst = Vec::new();
    let mut writer = BitWriter::new(&mut dst, buffer_size);
    unsafe { meta.write_to(&mut writer)? };
    writer.flush()?;
    assert_eq!(meta.exact_size(), dst.len());

    // page meta size
    let mut dst = Vec::new();
    let mut writer = BitWriter::new(&mut dst, buffer_size);
    let page_meta = PageMeta {
      per_latent_var: (0..meta.per_latent_var.len())
        .map(|latent_var_idx| {
          let delta_encoding = meta
            .mode
            .delta_encoding_for_latent_var(latent_var_idx, meta.delta_encoding);
          let delta_moments = match_latent_enum!(
            &meta.per_latent_var[latent_var_idx].bins,
            DynBins<L>(_bins) => {
              DynLatents::new(vec![L::ZERO; delta_encoding.n_latents_per_state()]).unwrap()
            }
          );
          PageLatentVarMeta {
            delta_moments,
            ans_final_state_idxs: [0; ANS_INTERLEAVING],
          }
        })
        .collect(),
    };
    unsafe {
      page_meta.write_to(
        meta
          .per_latent_var
          .iter()
          .map(|var_meta| var_meta.ans_size_log),
        &mut writer,
      )
    };
    writer.flush()?;
    assert_eq!(meta.exact_page_meta_size(), dst.len());
    Ok(())
  }

  #[test]
  fn exact_size_binless() -> PcoResult<()> {
    let meta = ChunkMeta {
      mode: Mode::Classic,
      delta_encoding: DeltaEncoding::Consecutive(5),
      per_latent_var: vec![ChunkLatentVarMeta {
        ans_size_log: 0,
        bins: DynBins::U32(vec![]),
      }],
    };

    check_exact_sizes(&meta)
  }

  #[test]
  fn exact_size_trivial() -> PcoResult<()> {
    let meta = ChunkMeta {
      mode: Mode::Classic,
      delta_encoding: DeltaEncoding::None,
      per_latent_var: vec![ChunkLatentVarMeta {
        ans_size_log: 0,
        bins: DynBins::U64(vec![Bin {
          weight: 1,
          lower: 77_u64,
          offset_bits: 0,
        }]),
      }],
    };

    check_exact_sizes(&meta)
  }

  #[test]
  fn exact_size_float_mult() -> PcoResult<()> {
    let meta = ChunkMeta {
      mode: Mode::FloatMult(DynLatent::U32(777_u32)),
      delta_encoding: DeltaEncoding::Consecutive(3),
      per_latent_var: vec![
        ChunkLatentVarMeta {
          ans_size_log: 7,
          bins: DynBins::U32(vec![
            Bin {
              weight: 11,
              lower: 0_u32,
              offset_bits: 0,
            },
            Bin {
              weight: 117,
              lower: 1,
              offset_bits: 0,
            },
          ]),
        },
        ChunkLatentVarMeta {
          ans_size_log: 3,
          bins: DynBins::U32(vec![
            Bin {
              weight: 3,
              lower: 0_u32,
              offset_bits: 0,
            },
            Bin {
              weight: 5,
              lower: 1,
              offset_bits: 0,
            },
          ]),
        },
      ],
    };

    check_exact_sizes(&meta)
  }
}
