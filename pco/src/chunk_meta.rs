use better_io::BetterBufRead;
use std::cmp::min;
use std::io::Write;

use crate::bin::Bin;
use crate::bit_reader::BitReaderBuilder;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;
use crate::constants::*;
use crate::data_types::{FloatLike, NumberLike, UnsignedLike};
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::format_version::FormatVersion;
use crate::modes::{gcd, Mode};

/// Part of [`ChunkMeta`][crate::ChunkMeta] that describes a latent
/// variable interleaved into the compressed data.
///
/// For instance, with
/// [classic mode][crate::Mode::Classic], there is a single latent variable
/// corresponding to the actual numbers' (or deltas') bins.
///
/// This is mainly useful for inspecting how compression was done.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkLatentVarMeta<U: UnsignedLike> {
  /// The log2 of the number of the number of states in this chunk's tANS
  /// table.
  ///
  /// See <https://en.wikipedia.org/wiki/Asymmetric_numeral_systems>.
  pub ans_size_log: Bitlen,
  /// How the numbers or deltas are encoded, depending on their numerical
  /// range.
  pub bins: Vec<Bin<U>>,
}

impl<U: UnsignedLike> ChunkLatentVarMeta<U> {
  pub(crate) fn max_bits_per_offset(&self) -> Bitlen {
    self
      .bins
      .iter()
      .map(|bin| bin.offset_bits)
      .max()
      .unwrap_or_default()
  }

  pub(crate) fn max_bits_per_ans(&self) -> Bitlen {
    self.ans_size_log
      - self
        .bins
        .iter()
        .map(|bin| bin.weight.ilog2() as Bitlen)
        .min()
        .unwrap_or_default()
  }
}

fn parse_bin_batch<U: UnsignedLike, R: BetterBufRead>(
  reader_builder: &mut BitReaderBuilder<R>,
  mode: Mode<U>,
  ans_size_log: Bitlen,
  batch_size: usize,
  dst: &mut Vec<Bin<U>>,
) -> PcoResult<()> {
  reader_builder.with_reader(|reader| {
    let offset_bits_bits = bits_to_encode_offset_bits::<U>();
    for _ in 0..batch_size {
      let weight = reader.read_uint::<Weight>(ans_size_log) + 1;
      let lower = reader.read_uint::<U>(U::BITS);

      let offset_bits = reader.read_bitlen(offset_bits_bits);
      if offset_bits > U::BITS {
        reader.check_in_bounds()?;
        return Err(PcoError::corruption(format!(
          "offset bits of {} exceeds data type of {} bits",
          offset_bits,
          U::BITS,
        )));
      }

      let gcd = match mode {
        Mode::Gcd if offset_bits != 0 => reader.read_uint(U::BITS),
        _ => U::ONE,
      };

      dst.push(Bin {
        weight,
        lower,
        offset_bits,
        gcd,
      });
    }
    Ok(())
  })?;

  Ok(())
}

impl<U: UnsignedLike> ChunkLatentVarMeta<U> {
  fn parse_from<R: BetterBufRead>(
    reader_builder: &mut BitReaderBuilder<R>,
    mode: Mode<U>,
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
      parse_bin_batch(
        reader_builder,
        mode,
        ans_size_log,
        batch_size,
        &mut bins,
      )?;
    }

    Ok(Self { bins, ans_size_log })
  }

  fn write_to<W: Write>(&self, mode: Mode<U>, writer: &mut BitWriter<W>) -> PcoResult<()> {
    writer.write_bitlen(
      self.ans_size_log,
      BITS_TO_ENCODE_ANS_SIZE_LOG,
    );

    write_bins(&self.bins, mode, self.ans_size_log, writer)
  }

  pub(crate) fn is_trivial(&self) -> bool {
    self.bins.is_empty() || (self.bins.len() == 1 && self.bins[0].offset_bits == 0)
  }

  pub(crate) fn needs_gcd(&self, mode: Mode<U>) -> bool {
    match mode {
      Mode::Gcd => gcd::use_gcd_arithmetic(&self.bins),
      _ => false,
    }
  }
}

/// The metadata of a pco chunk.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ChunkMeta<U: UnsignedLike> {
  /// The formula `pco` used to compress each number at a low level.
  pub mode: Mode<U>,
  /// How many times delta encoding was applied during compression.
  /// This is between 0 and 7, inclusive.
  ///
  /// See [`ChunkConfig`][crate::ChunkConfig] for more details.
  pub delta_encoding_order: usize,
  /// Metadata about the interleaved streams needed by `pco` to
  /// compress/decompress the inputs
  /// according to the formula used by `mode`.
  pub per_latent_var: Vec<ChunkLatentVarMeta<U>>,
}

fn write_bins<U: UnsignedLike, W: Write>(
  bins: &[Bin<U>],
  mode: Mode<U>,
  ans_size_log: Bitlen,
  writer: &mut BitWriter<W>,
) -> PcoResult<()> {
  writer.write_usize(bins.len(), BITS_TO_ENCODE_N_BINS);
  let offset_bits_bits = bits_to_encode_offset_bits::<U>();
  for bin_batch in bins.chunks(FULL_BIN_BATCH_SIZE) {
    for bin in bin_batch {
      writer.write_uint(bin.weight - 1, ans_size_log);
      writer.write_uint(bin.lower, U::BITS);
      writer.write_bitlen(bin.offset_bits, offset_bits_bits);

      match mode {
        Mode::Classic => (),
        Mode::Gcd => {
          if bin.offset_bits > 0 {
            writer.write_uint(bin.gcd, U::BITS);
          }
        }
        Mode::FloatMult { .. } => (),
      }
    }
    writer.flush()?;
  }
  Ok(())
}

impl<U: UnsignedLike> ChunkMeta<U> {
  pub(crate) fn new(
    mode: Mode<U>,
    delta_encoding_order: usize,
    per_latent_var: Vec<ChunkLatentVarMeta<U>>,
  ) -> Self {
    ChunkMeta {
      mode,
      delta_encoding_order,
      per_latent_var,
    }
  }

  pub(crate) fn parse_from<R: BetterBufRead>(
    reader_builder: &mut BitReaderBuilder<R>,
    _version: &FormatVersion,
  ) -> PcoResult<Self> {
    let (mode, delta_encoding_order) = reader_builder.with_reader(|reader| {
      let mode = match reader.read_usize(BITS_TO_ENCODE_MODE) {
        0 => Ok(Mode::Classic),
        1 => Ok(Mode::Gcd),
        2 => {
          let base = U::Float::from_unsigned(reader.read_uint::<U>(U::BITS));
          Ok(Mode::FloatMult(FloatMultConfig {
            base,
            inv_base: base.inv(),
          }))
        }
        value => Err(PcoError::compatibility(format!(
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
      per_latent_var.push(ChunkLatentVarMeta::parse_from(
        reader_builder,
        mode,
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

  pub(crate) fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) -> PcoResult<()> {
    let mode_value = match self.mode {
      Mode::Classic => 0,
      Mode::Gcd => 1,
      Mode::FloatMult { .. } => 2,
    };
    writer.write_usize(mode_value, BITS_TO_ENCODE_MODE);
    if let Mode::FloatMult(config) = self.mode {
      writer.write_uint(config.base.to_unsigned(), U::BITS);
    }

    writer.write_usize(
      self.delta_encoding_order,
      BITS_TO_ENCODE_DELTA_ENCODING_ORDER,
    );
    writer.flush()?;

    for latents in &self.per_latent_var {
      latents.write_to(self.mode, writer)?;
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
