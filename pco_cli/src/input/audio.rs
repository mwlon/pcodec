use crate::input::schema_from_field_paths;
use anyhow::anyhow;
use arrow::array::{ArrayRef, Float32Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use wav::BitDepth;

fn get_wav_field(path: &Path) -> anyhow::Result<Field> {
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

pub fn infer_wav_schema(dir: &Path) -> anyhow::Result<Schema> {
  let mut field_paths = Vec::new();
  for f in fs::read_dir(dir)? {
    let path = f?.path();
    if path.extension().unwrap().to_str().unwrap() == "wav" {
      field_paths.push((get_wav_field(&path)?, path));
    }
  }
  schema_from_field_paths(field_paths)
}

pub struct WavColumnReader {
  col_path: PathBuf,
  dtype: DataType,
  did_read: bool,
}

impl WavColumnReader {
  pub fn new(schema: &Schema, col_idx: usize) -> anyhow::Result<Self> {
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
  fn get_array(&self) -> anyhow::Result<ArrayRef> {
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
  type Item = anyhow::Result<ArrayRef>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.did_read {
      return None;
    }

    self.did_read = true;
    Some(self.get_array())
  }
}
