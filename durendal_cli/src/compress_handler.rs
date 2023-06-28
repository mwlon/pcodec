use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use arrow::csv::Reader as CsvReader;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;

use durendal::data_types::NumberLike;
use durendal::standalone::Compressor;
use durendal::{standalone, CompressorConfig};

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::CompressOpt;
use crate::utils;

const AUTO_DELTA_LIMIT: usize = 1000;

pub trait CompressHandler {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()>;
}

impl<P: NumberLikeArrow> CompressHandler for HandlerImpl<P> {
  fn compress(&self, opt: &CompressOpt, schema: &Schema) -> Result<()> {
    let mut open_options = OpenOptions::new();
    open_options.write(true);
    if opt.overwrite {
      open_options.create(true);
      open_options.truncate(true);
    } else {
      open_options.create_new(true);
    }
    let mut file = open_options.open(&opt.qco_path)?;

    let delta_encoding_order = if let Some(order) = opt.delta_encoding_order {
      order
    } else {
      println!(
        "automatically choosing delta encoding order based on first nums (specify --delta-order to skip)",
      );
      let head_nums = head_nums::<P>(schema, opt)?;
      let best_order =
        standalone::auto_compressor_config(&head_nums, opt.level).delta_encoding_order;
      println!(
        "determined best delta encoding order: {}",
        best_order
      );
      best_order
    };

    let config = CompressorConfig::default()
      .with_compression_level(opt.level)
      .with_delta_encoding_order(delta_encoding_order)
      .with_use_gcds(!opt.disable_gcds);
    let mut compressor = Compressor::<P::Num>::from_config(config);

    compressor.header()?;

    let mut reader = new_column_reader::<P>(schema, opt)?;
    let mut num_buffer = Vec::new();
    while let Some(batch_result) = reader.next_batch() {
      let batch = batch_result?;
      num_buffer.extend(&batch);
      if num_buffer.len() >= opt.chunk_size {
        write_chunk(
          &mut compressor,
          &num_buffer[..opt.chunk_size],
          &mut file,
        )?;
        num_buffer = num_buffer[opt.chunk_size..].to_vec();
      }
    }
    if !num_buffer.is_empty() {
      write_chunk(&mut compressor, &num_buffer, &mut file)?;
    }

    compressor.footer()?;
    file.write_all(&compressor.drain_bytes())?;
    Ok(())
  }
}

fn new_column_reader<P: NumberLikeArrow>(
  schema: &Schema,
  opt: &CompressOpt,
) -> Result<Box<dyn ColumnReader<P>>> {
  let res: Box<dyn ColumnReader<P>> = match (&opt.csv_path, &opt.parquet_path) {
    (Some(csv_path), None) => Box::new(CsvColumnReader::new(schema, csv_path, opt)?),
    (None, Some(parquet_path)) => Box::new(ParquetColumnReader::new(
      schema,
      parquet_path,
      opt,
    )?),
    _ => unreachable!("should have already checked that file is uniquely specified"),
  };
  Ok(res)
}

trait ColumnReader<P: NumberLikeArrow> {
  fn new(schema: &Schema, path: &Path, opt: &CompressOpt) -> Result<Self>
  where
    Self: Sized;
  fn next_arrow_batch(&mut self) -> Option<arrow::error::Result<RecordBatch>>;
  fn col_idx(&self) -> usize;

  fn next_batch(&mut self) -> Option<Result<Vec<P::Num>>> {
    self.next_arrow_batch().map(|batch_result| {
      let batch = batch_result?;
      let arrow_array = batch.column(self.col_idx());
      Ok(utils::arrow_to_vec::<P>(arrow_array))
    })
  }
}

struct ParquetColumnReader<T> {
  batch_reader: ParquetRecordBatchReader,
  phantom: PhantomData<T>,
}

impl<P: NumberLikeArrow> ColumnReader<P> for ParquetColumnReader<P> {
  fn new(schema: &Schema, path: &Path, opt: &CompressOpt) -> Result<Self> {
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
    let col_idx = utils::find_col_idx(schema, opt);
    let batch_reader = arrow_reader.get_record_reader_by_columns(vec![col_idx], opt.chunk_size)?;
    Ok(Self {
      batch_reader,
      phantom: PhantomData,
    })
  }

  fn next_arrow_batch(&mut self) -> Option<arrow::error::Result<RecordBatch>> {
    self.batch_reader.next()
  }

  fn col_idx(&self) -> usize {
    // 0 because we told arrow to only read the exact column we want
    0
  }
}

struct CsvColumnReader<P: NumberLikeArrow> {
  csv_reader: CsvReader<File>,
  col_idx: usize,
  phantom: PhantomData<P>,
}

impl<P: NumberLikeArrow> ColumnReader<P> for CsvColumnReader<P> {
  fn new(schema: &Schema, path: &Path, opt: &CompressOpt) -> Result<Self>
  where
    Self: Sized,
  {
    let csv_reader = CsvReader::from_reader(
      File::open(path)?,
      SchemaRef::new(schema.clone()),
      opt.csv_has_header()?,
      Some(opt.delimiter as u8),
      opt.chunk_size,
      None,
      None,
      Some(opt.timestamp_format.clone()),
    );
    let col_idx = utils::find_col_idx(schema, opt);

    Ok(Self {
      csv_reader,
      col_idx,
      phantom: PhantomData,
    })
  }

  fn next_arrow_batch(&mut self) -> Option<arrow::error::Result<RecordBatch>> {
    self.csv_reader.next()
  }

  fn col_idx(&self) -> usize {
    self.col_idx
  }
}

fn write_chunk<T: NumberLike>(
  compressor: &mut Compressor<T>,
  nums: &[T],
  file: &mut File,
) -> Result<()> {
  compressor.chunk(nums)?;
  file.write_all(&compressor.drain_bytes())?;
  Ok(())
}

fn head_nums<P: NumberLikeArrow>(schema: &Schema, opt: &CompressOpt) -> Result<Vec<P::Num>> {
  let mut reader = new_column_reader::<P>(schema, opt)?;
  let mut head_nums = Vec::with_capacity(AUTO_DELTA_LIMIT);
  while let Some(batch_result) = reader.next_batch() {
    head_nums.extend(batch_result?);
    if head_nums.len() >= AUTO_DELTA_LIMIT {
      break;
    }
  }
  if head_nums.len() > AUTO_DELTA_LIMIT {
    head_nums = head_nums[0..AUTO_DELTA_LIMIT].to_vec();
  }
  Ok(head_nums)
}