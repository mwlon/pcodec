use std::cmp::min;
use std::io::Write;

use better_io::BetterBufRead;

use crate::bin::Bin;
use crate::bit_reader::BitReaderBuilder;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;
use crate::constants::*;
use crate::data_types::Latent;
use crate::errors::{PcoError, PcoResult};
use crate::format_version::FormatVersion;
use crate::Mode;

pub(crate) fn bin_exact_bit_size<L: Latent>(ans_size_log: Bitlen) -> Bitlen {
  ans_size_log + L::BITS + bits_to_encode_offset_bits::<L>()
}

/// Part of [`ChunkMeta`][crate::ChunkMeta] that describes a latent
/// variable interleaved into the compressed data.
///
/// For instance, with
/// [classic mode][crate::Mode::Classic], there is a single latent variable
/// corresponding to the actual numbers' (or deltas') bins.
///
/// This is mainly useful for inspecting how compression was done.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkLatentVarMeta<L: Latent> {
  /// The log2 of the number of the number of states in this chunk's tANS
  /// table.
  ///
  /// See <https://en.wikipedia.org/wiki/Asymmetric_numeral_systems>.
  pub ans_size_log: Bitlen,
  /// How the numbers or deltas are encoded, depending on their numerical
  /// range.
  pub bins: Vec<Bin<L>>,
}

impl<L: Latent> ChunkLatentVarMeta<L> {
  pub(crate) fn max_bits_per_offset(&self) -> Bitlen {
    self
      .bins
      .iter()
      .map(|bin| bin.offset_bits)
      .max()
      .unwrap_or_default()
  }

  pub(crate) fn avg_bits_per_delta(&self) -> f64 {
    let total_weight = (1 << self.ans_size_log) as f64;
    self
      .bins
      .iter()
      .map(|bin| {
        let ans_bits = self.ans_size_log as f64 - (bin.weight as f64).log2();
        (ans_bits + bin.offset_bits as f64) * bin.weight as f64 / total_weight
      })
      .sum()
  }
}

unsafe fn read_bin_batch<L: Latent, R: BetterBufRead>(
  reader_builder: &mut BitReaderBuilder<R>,
  ans_size_log: Bitlen,
  batch_size: usize,
  dst: &mut Vec<Bin<L>>,
) -> PcoResult<()> {
  reader_builder.with_reader(|reader| {
    let offset_bits_bits = bits_to_encode_offset_bits::<L>();
    for _ in 0..batch_size {
      let weight = reader.read_uint::<Weight>(ans_size_log) + 1;
      let lower = reader.read_uint::<L>(L::BITS);

      let offset_bits = reader.read_bitlen(offset_bits_bits);
      if offset_bits > L::BITS {
        reader.check_in_bounds()?;
        return Err(PcoError::corruption(format!(
          "offset bits of {} exceeds data type of {} bits",
          offset_bits,
          L::BITS,
        )));
      }

      dst.push(Bin {
        weight,
        lower,
        offset_bits,
      });
    }
    Ok(())
  })?;

  Ok(())
}

impl<L: Latent> ChunkLatentVarMeta<L> {
  unsafe fn read_from<R: BetterBufRead>(
    reader_builder: &mut BitReaderBuilder<R>,
  ) -> PcoResult<Self> {
    let (ans_size_log, n_bins) = reader_builder.with_reader(|reader| {
      let ans_size_log = reader.read_bitlen(BITS_TO_ENCODE_ANS_SIZE_LOG);
      let n_bins = reader.read_usize(BITS_TO_ENCODE_N_BINS);
      Ok((ans_size_log, n_bins))
    })?;

    if 1 << ans_size_log < n_bins {
      return Err(PcoError::corruption(format!(
        "ANS size log ({}) is too small for number of bins ({})",
        ans_size_log, n_bins,
      )));
    }
    if n_bins == 1 && ans_size_log > 0 {
      return Err(PcoError::corruption(format!(
        "Only 1 bin but ANS size log is {} (should be 0)",
        ans_size_log,
      )));
    }
    if ans_size_log > MAX_ANS_BITS {
      return Err(PcoError::corruption(format!(
        "ANS size log ({}) should not be greater than {}",
        ans_size_log, MAX_ANS_BITS,
      )));
    }

    let mut bins = Vec::with_capacity(n_bins);
    while bins.len() < n_bins {
      let batch_size = min(n_bins - bins.len(), FULL_BIN_BATCH_SIZE);
      read_bin_batch(
        reader_builder,
        ans_size_log,
        batch_size,
        &mut bins,
      )?;
    }

    Ok(Self { bins, ans_size_log })
  }

  unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) -> PcoResult<()> {
    writer.write_bitlen(
      self.ans_size_log,
      BITS_TO_ENCODE_ANS_SIZE_LOG,
    );

    write_bins(&self.bins, self.ans_size_log, writer)
  }

  pub(crate) fn is_trivial(&self) -> bool {
    self.bins.is_empty() || (self.bins.len() == 1 && self.bins[0].offset_bits == 0)
  }

  fn exact_bit_size(&self) -> usize {
    BITS_TO_ENCODE_ANS_SIZE_LOG as usize
      + BITS_TO_ENCODE_N_BINS as usize
      + self.bins.len() * bin_exact_bit_size::<L>(self.ans_size_log) as usize
  }
}

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
  pub per_latent_var: Vec<ChunkLatentVarMeta<L>>,
}

unsafe fn write_bins<L: Latent, W: Write>(
  bins: &[Bin<L>],
  ans_size_log: Bitlen,
  writer: &mut BitWriter<W>,
) -> PcoResult<()> {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let offset_bits_bits = bits_to_encode_offset_bits::<L>();
  for bin_batch in bins.chunks(FULL_BIN_BATCH_SIZE) {
    for bin in bin_batch {
      writer.write_uint(bin.weight - 1, ans_size_log);
      writer.write_uint(bin.lower, L::BITS);
      writer.write_bitlen(bin.offset_bits, offset_bits_bits);
    }
    writer.flush()?;
  }
  Ok(())
}

impl<L: Latent> ChunkMeta<L> {
  pub(crate) fn new(
    mode: Mode<L>,
    delta_encoding_order: usize,
    per_latent_var: Vec<ChunkLatentVarMeta<L>>,
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
      per_latent_var.push(ChunkLatentVarMeta::read_from(
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
  use crate::delta::DeltaMoments;
  use crate::page_meta::{PageLatentVarMeta, PageMeta};

  use super::*;

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
      per_var: (0..meta.per_latent_var.len())
        .map(|latent_var_idx| {
          let delta_order = meta
            .mode
            .delta_order_for_latent_var(latent_var_idx, meta.delta_encoding_order);
          PageLatentVarMeta {
            delta_moments: DeltaMoments(vec![L::ZERO; delta_order]),
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
        bins: vec![],
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
          lower: 77,
          offset_bits: 0,
        }],
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
              lower: 0,
              offset_bits: 0,
            },
            Bin {
              weight: 117,
              lower: 1,
              offset_bits: 0,
            },
          ],
        },
        ChunkLatentVarMeta {
          ans_size_log: 3,
          bins: vec![
            Bin {
              weight: 3,
              lower: 0,
              offset_bits: 0,
            },
            Bin {
              weight: 5,
              lower: 1,
              offset_bits: 0,
            },
          ],
        },
      ],
    };

    check_exact_sizes(&meta)
  }
}
