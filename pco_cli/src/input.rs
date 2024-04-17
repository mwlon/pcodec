use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayData, ArrayRef, Float32Array, Int32Array};
use arrow::buffer::Buffer;
use arrow::csv;
use arrow::csv::Reader as CsvReader;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use clap::Parser;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use wav::BitDepth;

use crate::{parse, utils};

const MAX_INFER_SCHEMA_RECORDS: usize = 1000;

#[derive(Clone, Debug, Default, Parser)]
pub struct InputColumnOpt {
  /// A specific data type to interpret the column as. Only useful for data
  /// coming from CSVs where the type is ambiguous.
  #[arg(long, value_parser = parse::arrow_dtype)]
  pub dtype: Option<DataType>,
  /// Either this or col-idx must be specified.
  #[arg(long)]
  pub col_name: Option<String>,
  /// Either this or col-name must be specified.
  #[arg(long)]
  pub col_idx: Option<usize>,
}

#[derive(Clone, Debug, Parser)]
pub struct InputFileOpt {
  /// Path to a directory containing binary files to be used as input.
  /// Each binary file must be prefixed with its data type, e.g.
  /// `i32_foo.bar` will be read as a flat buffer of 32-bit signed ints, using
  /// system native memory layout.
  #[arg(long)]
  pub binary_dir: Option<PathBuf>,
  /// Path to a Parquet file to use as input.
  /// Only numerical, non-null values in the file will be used.
  #[arg(long = "parquet", short)]
  pub parquet_path: Option<PathBuf>,
  /// Path to a CSV file to use as input.
  /// Only numerical, non-null values in the file will be used.
  #[arg(long = "csv")]
  pub csv_path: Option<PathBuf>,
  /// Path to a directory containing wav files to be used as input.
  #[arg(long = "wav")]
  pub wav_path: Option<PathBuf>,

  #[arg(long)]
  pub csv_has_header: bool,
  #[arg(long, default_value = ",")]
  pub csv_delimiter: char,
}

fn schema_from_field_paths(mut field_paths: Vec<(Field, PathBuf)>) -> Result<Schema> {
  field_paths.sort_by_key(|(field, _)| field.name().to_string());
  let mut metadata = HashMap::new();
  for (i, (_, path)) in field_paths.iter().enumerate() {
    metadata.insert(
      i.to_string(),
      path.to_str().unwrap().to_string(),
    );
  }
  let fields = field_paths.into_iter().map(|(f, _)| f).collect::<Vec<_>>();
  Ok(Schema::new_with_metadata(fields, metadata))
}

fn get_binary_field(path: &Path) -> Result<Field> {
  let no_ext = path
    .file_stem()
    .expect("weird file name")
    .to_str()
    .expect("somehow not unicode");
  let mut split = no_ext.split('_');
  let invalid_filename = || {
    anyhow!(
      "filename must be of the format <DTYPE>_<NAME>, but was {:?}",
      path
    )
  };
  let dtype_str = split.next().ok_or_else(invalid_filename)?;
  let dtype = parse::arrow_dtype(dtype_str)?;
  let name = split.collect::<Vec<_>>().join("_");
  Ok(Field::new(name, dtype, false))
}

fn infer_binary_schema(dir: &Path) -> Result<Schema> {
  let mut field_paths = Vec::new();
  for f in fs::read_dir(dir)? {
    let path = f?.path();
    field_paths.push((get_binary_field(&path)?, path));
  }
  schema_from_field_paths(field_paths)
}

fn infer_csv_schema(col_opt: &InputColumnOpt, file_opt: &InputFileOpt) -> Result<Schema> {
  // arrow API is kinda bad right now, so we have to convert the paths
  // back to strings
  let inferred_schema = csv::infer_schema_from_files(
    &[file_opt
      .csv_path
      .clone()
      .unwrap()
      .to_str()
      .unwrap()
      .to_string()],
    file_opt.csv_delimiter as u8,
    Some(MAX_INFER_SCHEMA_RECORDS),
    file_opt.csv_has_header,
  )?;

  let Some(dtype) = &col_opt.dtype else {
    return Ok(inferred_schema);
  };

  let mut fields = Vec::new();
  for (col_idx, field) in inferred_schema.fields().iter().enumerate() {
    let new_field = match (&col_opt.col_name, &col_opt.col_idx) {
      (Some(name), None) if name == field.name() => Field::new(name, dtype.clone(), false),
      (None, Some(idx)) if *idx == col_idx => Field::new(field.name(), dtype.clone(), false),
      _ => field.as_ref().clone(),
    };
    fields.push(new_field);
  }
  Ok(Schema::new(fields))
}

fn infer_parquet_schema(col_opt: &InputColumnOpt, path: &Path) -> Result<Schema> {
  let file = File::open(path)?;
  let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
  let schema = reader.schema().as_ref().clone();

  if let Some(arrow_dtype) = &col_opt.dtype {
    let col_idx = utils::find_col_idx(&schema, col_opt.col_idx, &col_opt.col_name)?;
    let field = schema.field(col_idx);
    if field.data_type() != arrow_dtype {
      return Err(anyhow!(
        "optionally specified dtype {:?} did not match parquet schema {:?}",
        arrow_dtype,
        field.data_type(),
      ));
    }
  }
  Ok(schema)
}

fn get_wav_field(path: &Path) -> Result<Field> {
  // this is excessively slow, but easy for now
  let mut file = File::open(path)?;
  let (header, _) = wav::read(&mut file)?;
  let dtype = match header.bytes_per_sample {
    1 | 2 | 3 => Ok(DataType::Int32),
    4 => Ok(DataType::Float32),
    _ => Err(anyhow!(
      "invalid number of bytes per wav file sample"
    )),
  }?;
  let no_ext = path
    .file_stem()
    .expect("weird file name")
    .to_str()
    .expect("somehow not unicode");
  Ok(Field::new(no_ext, dtype, false))
}

fn infer_wav_schema(dir: &Path) -> Result<Schema> {
  let mut field_paths = Vec::new();
  for f in fs::read_dir(dir)? {
    let path = f?.path();
    if path.extension().unwrap().to_str().unwrap() == "wav" {
      field_paths.push((get_wav_field(&path)?, path));
    }
  }
  schema_from_field_paths(field_paths)
}

pub fn get_schema(col_opt: &InputColumnOpt, file_opt: &InputFileOpt) -> Result<Schema> {
  match (
    &file_opt.binary_dir,
    &file_opt.csv_path,
    &file_opt.parquet_path,
    &file_opt.wav_path,
  ) {
    // maybe one day I should structure this better
    (Some(path), None, None, None) => infer_binary_schema(path),
    (None, Some(_), None, None) => infer_csv_schema(col_opt, file_opt),
    (None, None, Some(path), None) => infer_parquet_schema(col_opt, path),
    (None, None, None, Some(path)) => infer_wav_schema(path),
    (None, None, None, None) => Err(anyhow!(
      "no input file or directory was specified"
    )),
    _ => Err(anyhow!(
      "multiple input files or directories were specified"
    )),
  }
}

pub fn new_column_reader(
  schema: &Schema,
  col_idx: usize,
  opt: &InputFileOpt,
) -> Result<Box<dyn Iterator<Item = Result<ArrayRef>>>> {
  let res: Box<dyn Iterator<Item = Result<ArrayRef>>> = match (
    &opt.binary_dir,
    &opt.csv_path,
    &opt.parquet_path,
    &opt.wav_path,
  ) {
    (Some(_), None, None, None) => Box::new(BinaryColumnReader::new(schema, col_idx)?),
    (None, Some(csv_path), None, None) => Box::new(CsvColumnReader::new(
      schema, csv_path, col_idx, opt,
    )?),
    (None, None, Some(parquet_path), None) => Box::new(ParquetColumnReader::new(
      schema,
      parquet_path,
      col_idx,
    )?),
    (None, None, None, Some(_)) => Box::new(WavColumnReader::new(schema, col_idx)?),
    _ => unreachable!("should have already checked that file is uniquely specified"),
  };
  Ok(res)
}

struct BinaryColumnReader {
  col_path: PathBuf,
  dtype: DataType,
  did_read: bool,
}

impl BinaryColumnReader {
  fn new(schema: &Schema, col_idx: usize) -> Result<Self> {
    let col_path = PathBuf::from(schema.metadata.get(&col_idx.to_string()).unwrap());
    let dtype = schema.field(col_idx).data_type().clone();
    Ok(BinaryColumnReader {
      col_path,
      dtype,
      did_read: false,
    })
  }
}

impl BinaryColumnReader {
  fn get_array(&self) -> Result<ArrayRef> {
    let bytes = fs::read(&self.col_path)?;
    let n_bytes = bytes.len();
    let buffer = Buffer::from_vec(bytes);

    let array_data = ArrayData::builder(self.dtype.clone())
      .add_buffer(buffer)
      .len(n_bytes / self.dtype.primitive_width().unwrap())
      .build()?;
    let array = arrow::array::make_array(array_data);

    Ok(array)
  }
}

impl Iterator for BinaryColumnReader {
  type Item = Result<ArrayRef>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.did_read {
      return None;
    }

    self.did_read = true;
    Some(self.get_array())
  }
}

struct ParquetColumnReader(ParquetRecordBatchReader);

impl ParquetColumnReader {
  fn new(schema: &Schema, path: &Path, col_idx: usize) -> Result<Self> {
    let file = File::open(path)?;
    let batch_reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let parquet_schema = parquet::arrow::arrow_to_parquet_schema(schema)?;
    let batch_reader = batch_reader_builder
      .with_projection(ProjectionMask::leaves(
        &parquet_schema,
        vec![col_idx],
      ))
      .build()?;
    Ok(Self(batch_reader))
  }
}

impl Iterator for ParquetColumnReader {
  type Item = Result<ArrayRef>;

  fn next(&mut self) -> Option<Result<ArrayRef>> {
    self.0.next().map(|batch_result| {
      let batch = batch_result?;
      // 0 because we told arrow to only read the exact column we want
      Ok(batch.column(0).clone())
    })
  }
}

struct CsvColumnReader {
  csv_reader: CsvReader<File>,
  col_idx: usize,
}

impl CsvColumnReader {
  fn new(schema: &Schema, path: &Path, col_idx: usize, opt: &InputFileOpt) -> Result<Self> {
    let csv_reader = csv::ReaderBuilder::new(SchemaRef::new(schema.clone()))
      .with_header(opt.csv_has_header)
      .with_delimiter(opt.csv_delimiter as u8)
      .build(File::open(path)?)?;

    Ok(Self {
      csv_reader,
      col_idx,
    })
  }
}

impl Iterator for CsvColumnReader {
  type Item = Result<ArrayRef>;

  fn next(&mut self) -> Option<Result<ArrayRef>> {
    self.csv_reader.next().map(|batch_result| {
      let batch = batch_result?;
      Ok(batch.column(self.col_idx).clone())
    })
  }
}

struct WavColumnReader {
  col_path: PathBuf,
  dtype: DataType,
  did_read: bool,
}

impl WavColumnReader {
  fn new(schema: &Schema, col_idx: usize) -> Result<Self> {
    let col_path = PathBuf::from(schema.metadata.get(&col_idx.to_string()).unwrap());
    let dtype = schema.field(col_idx).data_type().clone();
    Ok(WavColumnReader {
      col_path,
      dtype,
      did_read: false,
    })
  }
}

fn i32s_from_u8s(u8s: Vec<u8>) -> Vec<i32> {
  u8s.into_iter().map(|x| x as i32).collect()
}

fn i32s_from_i16s(i16s: Vec<i16>) -> Vec<i32> {
  i16s.into_iter().map(|x| x as i32).collect()
}

fn array_from_i32s(i32s: Vec<i32>) -> ArrayRef {
  Arc::new(Int32Array::from(i32s))
}

fn array_from_f32s(f32s: Vec<f32>) -> ArrayRef {
  Arc::new(Float32Array::from(f32s))
}

impl WavColumnReader {
  fn get_array(&self) -> Result<ArrayRef> {
    let mut inp_file = File::open(&self.col_path)?;
    let (_, data) = wav::read(&mut inp_file)?;
    let array = match data {
      BitDepth::Eight(u8s) => {
        let i32s = i32s_from_u8s(u8s);
        array_from_i32s(i32s)
      }
      BitDepth::Sixteen(i16s) => {
        let i32s = i32s_from_i16s(i16s);
        array_from_i32s(i32s)
      }
      BitDepth::TwentyFour(i32s) => array_from_i32s(i32s),
      BitDepth::ThirtyTwoFloat(f32s) => array_from_f32s(f32s),
      BitDepth::Empty => {
        if self.dtype == DataType::Int32 {
          array_from_i32s(vec![])
        } else {
          array_from_f32s(vec![])
        }
      }
    };
    Ok(array)
  }
}

impl Iterator for WavColumnReader {
  type Item = Result<ArrayRef>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.did_read {
      return None;
    }

    self.did_read = true;
    Some(self.get_array())
  }
}
