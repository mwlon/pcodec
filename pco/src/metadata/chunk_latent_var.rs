use crate::bit_reader::BitReaderBuilder;
use crate::bit_writer::BitWriter;
use crate::bits::bits_to_encode_offset_bits;
use crate::constants::{
  Bitlen, Weight, BITS_TO_ENCODE_ANS_SIZE_LOG, BITS_TO_ENCODE_N_BINS, FULL_BIN_BATCH_SIZE,
  MAX_ANS_BITS,
};
use crate::data_types::Latent;
use crate::errors::{PcoError, PcoResult};
use crate::metadata::{bin, Bin};
use better_io::BetterBufRead;
use std::cmp::min;
use std::io::Write;

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

impl<L: Latent> ChunkLatentVarMeta<L> {
  pub(crate) unsafe fn read_from<R: BetterBufRead>(
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

  pub(crate) unsafe fn write_to<W: Write>(&self, writer: &mut BitWriter<W>) -> PcoResult<()> {
    writer.write_bitlen(
      self.ans_size_log,
      BITS_TO_ENCODE_ANS_SIZE_LOG,
    );

    write_bins(&self.bins, self.ans_size_log, writer)
  }

  pub(crate) fn is_trivial(&self) -> bool {
    self.bins.is_empty() || (self.bins.len() == 1 && self.bins[0].offset_bits == 0)
  }

  pub(crate) fn exact_bit_size(&self) -> usize {
    BITS_TO_ENCODE_ANS_SIZE_LOG as usize
      + BITS_TO_ENCODE_N_BINS as usize
      + self.bins.len() * bin::bin_exact_bit_size::<L>(self.ans_size_log) as usize
  }
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
