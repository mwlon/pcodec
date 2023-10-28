use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::Path;

use anyhow::Result;
use arrow::csv;
use arrow::csv::Reader as CsvReader;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;

use pco::standalone::FileCompressor;
use pco::ChunkConfig;

use crate::handlers::HandlerImpl;
use crate::number_like_arrow::NumberLikeArrow;
use crate::opt::CompressOpt;
use crate::utils;

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
    let file = open_options.open(&opt.pco_path)?;

    let config = ChunkConfig::default()
      .with_compression_level(opt.level)
      .with_delta_encoding_order(opt.delta_encoding_order)
      .with_use_gcds(!opt.disable_gcds);
    let fc = FileCompressor::default();
    fc.write_header(&file)?;

    let mut reader = new_column_reader::<P>(schema, opt)?;
    let mut num_buffer = Vec::<P::Num>::new();
    while let Some(batch_result) = reader.next_batch() {
      let batch = batch_result?;
      num_buffer.extend(&batch);
      if num_buffer.len() >= opt.chunk_size {
        fc.chunk_compressor(&num_buffer[..opt.chunk_size], &config)?
          .write_chunk(&file)?;
        // this could be made more efficient
        num_buffer = num_buffer[opt.chunk_size..].to_vec();
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
    let file = File::open(path)?;
    let batch_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let parquet_schema = parquet::arrow::arrow_to_parquet_schema(schema)?;
    let col_idx = utils::find_col_idx(schema, opt);
    let batch_reader = batch_reader_builder
      .with_projection(ProjectionMask::leaves(
        &parquet_schema,
        vec![col_idx],
      ))
      .build()?;
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
    let csv_reader = csv::ReaderBuilder::new(SchemaRef::new(schema.clone()))
      .has_header(opt.csv_has_header()?)
      .with_batch_size(opt.chunk_size)
      .with_delimiter(opt.delimiter as u8)
      .build(File::open(path)?)?;
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
