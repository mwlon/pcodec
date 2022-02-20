use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::csv::Reader as CsvReader;
use arrow::datatypes::{Schema, SchemaRef};
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;

use q_compress::{BitWriter, Compressor, CompressorConfig};
use q_compress::data_types::NumberLike;
use crate::handlers::HandlerImpl;

use crate::opt::CompressOpt;
use crate::arrow_number_like::ArrowNumberLike;
use crate::utils;

pub trait CompressHandler {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()>;
}

impl<T: ArrowNumberLike> CompressHandler for HandlerImpl<T> {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()> {
    if !T::IS_ARROW {
      return Err(anyhow!(
        "data type {} not supported by arrow converters",
        utils::dtype_name::<T>()
      ));
    }

    let config = CompressorConfig {
      compression_level: opt.level,
      delta_encoding_order: opt.delta_encoding_order,
    };
    let compressor = Compressor::<T>::from_config(config);

    let mut open_options = OpenOptions::new();
    open_options.write(true);
    if opt.overwrite {
      open_options.create(true);
      open_options.truncate(true);
    } else {
      open_options.create_new(true);
    }
    let mut file = open_options.open(&opt.qco_path)?;
    let mut writer = BitWriter::default();
    compressor.header(&mut writer)?;

    match (&opt.csv_path, &opt.parquet_path) {
      (Some(csv_path), None) => compress_csv_chunks(
        &compressor,
        schema,
        csv_path,
        opt,
        &mut file,
        &mut writer,
      )?,
      (None, Some(parquet_path)) => compress_parquet_chunks(
        &compressor,
        schema,
        parquet_path,
        opt,
        &mut file,
        &mut writer,
      )?,
      _ => unreachable!("should have already checked that file is uniquely specified")
    }
    compressor.footer(&mut writer)?;
    file.write_all(&writer.pop())?;
    Ok(())
  }
}

fn compress_parquet_chunks<T: ArrowNumberLike>(
  compressor: &Compressor<T>,
  schema: &Schema,
  parquet_path: &Path,
  opt: &CompressOpt,
  file: &mut File,
  writer: &mut BitWriter,
) -> Result<()> {
  let reader = SerializedFileReader::new(File::open(parquet_path)?)?;
  let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
  let col_idx = utils::find_col_idx(schema, opt);
  let batch_reader = arrow_reader.get_record_reader_by_columns(
    vec![col_idx],
    opt.chunk_size,
  )?;
  let mut nums_buffer = Vec::new();
  for batch_result in batch_reader {
    let batch = batch_result?;
    let arrow_array = batch.column(col_idx);
    nums_buffer.extend(&utils::arrow_to_vec::<T>(arrow_array));
    if nums_buffer.len() >= opt.chunk_size {
      write_chunk(
        compressor,
        &nums_buffer[0..opt.chunk_size],
        file,
        writer,
      )?;
      nums_buffer = nums_buffer[opt.chunk_size..].to_vec()
    }
  }
  if !nums_buffer.is_empty() {
    write_chunk(
      compressor,
      &nums_buffer,
      file,
      writer,
    )?;
  }

  Ok(())
}

fn compress_csv_chunks<T: ArrowNumberLike>(
  compressor: &Compressor<T>,
  schema: &Schema,
  csv_path: &Path,
  opt: &CompressOpt,
  file: &mut File,
  writer: &mut BitWriter,
) -> Result<()> {
  let reader = CsvReader::from_reader(
    File::open(csv_path)?,
    SchemaRef::new(schema.clone()),
    opt.csv_has_header()?,
    Some(opt.delimiter as u8),
    opt.chunk_size,
    None,
    None,
    Some(opt.timestamp_format.clone()),
  );
  let col_idx = utils::find_col_idx(schema, opt);

  for batch_result in reader {
    let batch = batch_result?;
    let arrow_array = batch.column(col_idx);
    let nums = utils::arrow_to_vec::<T>(arrow_array);
    write_chunk(
      compressor,
      &nums,
      file,
      writer,
    )?;
  }

  Ok(())
}

fn write_chunk<T: NumberLike>(
  compressor: &Compressor<T>,
  nums: &[T],
  file: &mut File,
  writer: &mut BitWriter,
) -> Result<()> {
  compressor.chunk(nums, writer)?;
  file.write_all(&writer.pop())?;
  *writer = BitWriter::default();
  Ok(())
}
