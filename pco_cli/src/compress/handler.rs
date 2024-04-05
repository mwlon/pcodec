use std::fs::OpenOptions;

use anyhow::Result;
use arrow::datatypes::Schema;

use pco::standalone::FileCompressor;
use pco::ChunkConfig;

use crate::arrow_handlers::ArrowHandlerImpl;
use crate::compress::CompressOpt;
use crate::dtypes::ArrowNumberLike;
use crate::{input, utils};

pub trait CompressHandler {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()>;
}

impl<P: ArrowNumberLike> CompressHandler for ArrowHandlerImpl<P> {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()> {
    let mut open_options = OpenOptions::new();
    open_options.write(true);
    if opt.overwrite {
      open_options.create(true);
      open_options.truncate(true);
    } else {
      open_options.create_new(true);
    }
    let file = open_options.open(&opt.path)?;

    let config = ChunkConfig::default()
      .with_compression_level(opt.level)
      .with_delta_encoding_order(opt.delta_encoding_order)
      .with_int_mult_spec(opt.int_mult)
      .with_float_mult_spec(opt.float_mult);
    let fc = FileCompressor::default();
    fc.write_header(&file)?;

    let col_idx = utils::find_col_idx(
      schema,
      opt.input_column.col_idx,
      &opt.input_column.col_name,
    )?;
    let reader = input::new_column_reader(schema, col_idx, &opt.input_file)?;
    let mut num_buffer = Vec::<P::Pco>::new();
    for array_result in reader {
      let array = array_result?;
      num_buffer.extend(utils::arrow_to_nums::<P>(array));
      if num_buffer.len() >= opt.chunk_size {
        fc.chunk_compressor(&num_buffer[..opt.chunk_size], &config)?
          .write_chunk(&file)?;
        num_buffer.drain(..opt.chunk_size);
      }
    }
    if !num_buffer.is_empty() {
      fc.chunk_compressor(&num_buffer, &config)?
        .write_chunk(&file)?;
    }

    fc.write_footer(&file)?;
    Ok(())
  }
}
