use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, Float32Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use wav::BitDepth;

pub fn get_wav_field(path: &Path) -> Result<Option<Field>> {
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
  let name = path
    .file_stem()
    .unwrap()
    .to_str()
    .expect("somehow not unicode");
  Ok(Some(Field::new(name, dtype, false)))
}

pub struct WavColumnReader {
  col_path: PathBuf,
  dtype: DataType,
  did_read: bool,
}

impl WavColumnReader {
  pub fn new(schema: &Schema, col_idx: usize) -> Result<Self> {
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
