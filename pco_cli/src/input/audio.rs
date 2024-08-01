use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, Float32Array, Int16Array, Int32Array, UInt16Array};
use arrow::datatypes::{DataType, Field, Schema};
use wav::BitDepth;

pub fn get_wav_field(path: &Path) -> Result<Option<Field>> {
  // this is excessively slow, but easy for now
  let mut file = File::open(path)?;
  let (header, _) = wav::read(&mut file)?;
  let dtype = match (header.audio_format, header.bits_per_sample) {
    (wav::WAV_FORMAT_PCM, 8 | 16) => Ok(DataType::Int16),
    (wav::WAV_FORMAT_PCM, 24 | 32) => Ok(DataType::Int32),
    (wav::WAV_FORMAT_IEEE_FLOAT, 32) => Ok(DataType::Float32),
    _ => Err(anyhow!(
      "audio format {} with {} bits per sample not supported",
      header.audio_format,
      header.bits_per_sample
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

impl WavColumnReader {
  fn get_array(&self) -> Result<ArrayRef> {
    let mut inp_file = File::open(&self.col_path)?;
    let (_, data) = wav::read(&mut inp_file)?;
    let array: ArrayRef = match data {
      BitDepth::Eight(u8s) => {
        let u16s = u8s.into_iter().map(|x| x as u16).collect::<Vec<_>>();
        Arc::new(UInt16Array::from(u16s))
      }
      BitDepth::Sixteen(i16s) => Arc::new(Int16Array::from(i16s)),
      BitDepth::TwentyFour(i32s) => Arc::new(Int32Array::from(i32s)),
      BitDepth::ThirtyTwoFloat(f32s) => Arc::new(Float32Array::from(f32s)),
      BitDepth::Empty => {
        if self.dtype == DataType::Int32 {
          Arc::new(Int32Array::from(Vec::<i32>::new()))
        } else if self.dtype == DataType::Float32 {
          Arc::new(Float32Array::from(Vec::<f32>::new()))
        } else {
          Arc::new(Int16Array::from(Vec::<i16>::new()))
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
