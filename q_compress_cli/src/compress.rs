use std::fs::File;
use std::path::Path;

use anyhow::anyhow;
use anyhow::Result;
use arrow::csv;
use arrow::datatypes::{Field, Schema};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::utils;
use crate::dtype::DType;
use crate::handlers;
use crate::opt::CompressOpt;

const MAX_INFER_SCHEMA_RECORDS: usize = 1000;

fn infer_csv_schema(path: &Path, opt: &CompressOpt) -> Result<Schema> {
  // arrow API is kinda bad right now, so we have to convert the paths
  // back to strings
  let inferred_schema = csv::infer_schema_from_files(
    &[path.to_str().unwrap().to_string()],
    opt.delimiter as u8,
    Some(MAX_INFER_SCHEMA_RECORDS),
    opt.csv_has_header()?,
  )?;

  if let Some(dtype) = &opt.dtype {
    let mut fields = Vec::new();
    let arrow_dtype = dtype.to_arrow()?;
    for (col_idx, field) in inferred_schema.fields().iter().enumerate() {
      match (&opt.col_name, &opt.col_idx) {
        (Some(name), None) if name == field.name() => {
          fields.push(Field::new(name, arrow_dtype.clone(), false));
        },
        (None, Some(idx)) if *idx == col_idx => {
          fields.push(Field::new(field.name(), arrow_dtype.clone(), false));
        },
        _ => {
          fields.push(field.clone());
        },
      }
    }
    Ok(Schema::new(fields))
  } else {
    Ok(inferred_schema)
  }
}

fn infer_parquet_schema(path: &Path, opt: &CompressOpt) -> Result<Schema> {
  let file = File::open(path)?;
  let reader = SerializedFileReader::new(file)?;
  let file_meta = reader.metadata().file_metadata();
  let parquet_schema = file_meta.schema_descr();
  let res = parquet::arrow::parquet_to_arrow_schema(
    parquet_schema,
    file_meta.key_value_metadata(),
  )?;
  let col_idx = utils::find_col_idx(&res, opt);
  let field = &res.fields()[col_idx];
  if let Some(dtype) = opt.dtype {
    let arrow_dtype = dtype.to_arrow()?;
    if field.data_type() != &arrow_dtype {
      return Err(anyhow!(
        "optionally specified dtype {:?} did not match parquet schema {:?}",
        arrow_dtype,
        field.data_type(),
      ));
    }
  }
  Ok(res)
}

pub fn compress(opt: CompressOpt) -> Result<()> {
  let schema = match (&opt.csv_path, &opt.parquet_path) {
    (Some(csv_path), None) => infer_csv_schema(csv_path, &opt),
    (None, Some(parquet_path)) => infer_parquet_schema(parquet_path, &opt),
    _ => Err(anyhow!(
      "conflicting or incomplete dtype information: dtype={:?}, csv-path={:?}, parquet-path={:?}",
      opt.dtype,
      opt.csv_path,
      opt.parquet_path,
    ))
  }?;
  let arrow_dtype = match (&opt.col_idx, &opt.col_name) {
    (Some(col_idx), None) => Ok(schema.fields()[*col_idx].data_type()),
    (None, Some(col_name)) => Ok(schema.field_with_name(col_name)?.data_type()),
    _ => Err(anyhow!("incomplete or incompatible col name and col idx")),
  }?;
  let dtype = DType::from_arrow(arrow_dtype)?;
  let handler = handlers::from_dtype(dtype)?;
  handler.compress(&opt, &schema)
}