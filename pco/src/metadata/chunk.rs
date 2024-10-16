use std::io::Write;

use better_io::BetterBufRead;

use crate::bit_reader::BitReaderBuilder;
use crate::bit_writer::BitWriter;
use crate::constants::*;
use crate::data_types::Latent;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::chunk_latent_var::ChunkLatentVarMeta;
use crate::metadata::format_version::FormatVersion;
use crate::metadata::Mode;

/// The metadata of a pco chunk.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkMeta<L: Latent> {
  /// The formula `pco` used to compress each number at a low level.
  pub mode: Mode<L>,
  /// How many times delta encoding was applied during compression.
  /// This is between 0 and 7, inclusive.
  ///
  /// See [`ChunkConfig`][crate::ChunkConfig] for more details.
  pub delta_encoding_order: usize,
  /// Metadata about the interleaved streams needed by `pco` to
  /// compress/decompress the inputs
  /// according to the formula used by `mode`.
  pub per_latent_var: Vec<ChunkLatentVarMeta>,
}

impl<L: Latent> ChunkMeta<L> {
  pub(crate) fn new(
    mode: Mode<L>,
    delta_encoding_order: usize,
    per_latent_var: Vec<ChunkLatentVarMeta>,
  ) -> Self {
    ChunkMeta {
      mode,
      delta_encoding_order,
      per_latent_var,
    }
  }

  pub(crate) fn exact_size(&self) -> usize {
    let extra_bits_for_mode = match self.mode {
      Mode::Classic => 0,
      Mode::IntMult(_) => L::BITS,
      Mode::FloatQuant(_) => BITS_TO_ENCODE_QUANTIZE_K,
      Mode::FloatMult(_) => L::BITS,
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
        let delta_order = self
          .mode
          .delta_order_for_latent_var(latent_var_idx, self.delta_encoding_order);
        latent_var.ans_size_log as usize * ANS_INTERLEAVING + L::BITS as usize * delta_order
      })
      .sum();
    bit_size.div_ceil(8)
  }

  pub(crate) unsafe fn read_from<R: BetterBufRead>(
    reader_builder: &mut BitReaderBuilder<R>,
    version: &FormatVersion,
  ) -> PcoResult<Self> {
    let (mode, delta_encoding_order) = reader_builder.with_reader(|reader| {
      let mode = match reader.read_usize(BITS_TO_ENCODE_MODE) {
        0 => Ok(Mode::Classic),
        1 => {
          if version.used_old_gcds() {
            return Err(PcoError::compatibility(
              "unable to decompress data from v0.0.0 of pco with different GCD encoding",
            ));
          }

          let base = reader.read_uint::<L>(L::BITS);
          Ok(Mode::IntMult(base))
        }
        2 => {
          let base_latent = reader.read_uint::<L>(L::BITS);
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

      Ok((mode, delta_encoding_order))
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
      delta_encoding_order,
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
        writer.write_uint(base, L::BITS);
      }
      Mode::FloatMult(base_latent) => {
        writer.write_uint(base_latent, L::BITS);
      }
      Mode::FloatQuant(k) => {
        writer.write_uint(k, BITS_TO_ENCODE_QUANTIZE_K);
      }
    };

    writer.write_usize(
      self.delta_encoding_order,
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
    );
    writer.flush()?;

    for latents in &self.per_latent_var {
      latents.write_to(writer)?;
    }

    writer.finish_byte();
    writer.flush()?;
    Ok(())
  }

  pub(crate) fn delta_order_for_latent_var(&self, latent_idx: usize) -> usize {
    self
      .mode
      .delta_order_for_latent_var(latent_idx, self.delta_encoding_order)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::metadata::dyn_latents::DynLatents;
  use crate::metadata::page::PageMeta;
  use crate::metadata::page_latent_var::PageLatentVarMeta;
  use crate::metadata::Bin;

  fn check_exact_sizes<L: Latent>(meta: &ChunkMeta<L>) -> PcoResult<()> {
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
          let delta_order = meta
            .mode
            .delta_order_for_latent_var(latent_var_idx, meta.delta_encoding_order);
          PageLatentVarMeta {
            delta_moments: DynLatents::try_from(vec![L::ZERO; delta_order]).unwrap(),
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
    let meta = ChunkMeta::<u32> {
      mode: Mode::Classic,
      delta_encoding_order: 5,
      per_latent_var: vec![ChunkLatentVarMeta {
        ans_size_log: 0,
        bins: Vec::<Bin<u32>>::new().into(),
      }],
    };

    check_exact_sizes(&meta)
  }

  #[test]
  fn exact_size_trivial() -> PcoResult<()> {
    let meta = ChunkMeta::<u64> {
      mode: Mode::Classic,
      delta_encoding_order: 0,
      per_latent_var: vec![ChunkLatentVarMeta {
        ans_size_log: 0,
        bins: vec![Bin {
          weight: 1,
          lower: 77_u64,
          offset_bits: 0,
        }]
        .into(),
      }],
    };

    check_exact_sizes(&meta)
  }

  #[test]
  fn exact_size_float_mult() -> PcoResult<()> {
    let meta = ChunkMeta::<u32> {
      mode: Mode::FloatMult(777_u32),
      delta_encoding_order: 3,
      per_latent_var: vec![
        ChunkLatentVarMeta {
          ans_size_log: 7,
          bins: vec![
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
          ]
          .into(),
        },
        ChunkLatentVarMeta {
          ans_size_log: 3,
          bins: vec![
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
          ]
          .into(),
        },
      ],
    };

    check_exact_sizes(&meta)
  }
}
