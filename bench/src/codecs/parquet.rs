use std::convert::TryInto;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterPropertiesPtr};
use parquet::file::reader::{SerializedFileReader, SerializedPageReader};
use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter};
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath};
use parquet::file::reader::{FileReader};
use std::{fs::File, path::Path};
use parquet::column::reader::get_typed_column_reader;

use crate::codecs::{CodecInternal, utils};
use crate::dtypes::Dtype;

const ZSTD: &'static str = "zstd";

#[derive(Clone, Debug)]
pub struct ParquetConfig {
  compression: Compression
}

impl Default for ParquetConfig {
  fn default() -> Self {
    Self {
      compression: Compression::UNCOMPRESSED
    }
  }
}

fn str_to_compression(s: &str) -> Result<Compression> {
  let res = match s.to_lowercase().as_str() {
    "uncompressed" => Compression::UNCOMPRESSED,
    "snappy" => Compression::SNAPPY,
    _ => {
      if s.starts_with(ZSTD) {
        let level = if s.len() > ZSTD.len() {
          ZstdLevel::try_new(s[4..].to_string().parse::<i32>()?)?
        } else {
          ZstdLevel::default()
        };
        Compression::ZSTD(level)
      } else {
        return Err(anyhow!("unknown parquet codec {}", s))
      }
    }
  };
  Ok(res)
}

fn compression_to_string(compression: &Compression) -> String {
  match compression {
    Compression::UNCOMPRESSED => "uncompressed".to_string(),
    Compression::SNAPPY => "snappy".to_string(),
    Compression::ZSTD(level) => format!("{}{}", ZSTD, level.compression_level()),
    _ => panic!("should be unreachable"),
  }
}

// This approach compresses the vector as
impl CodecInternal for ParquetConfig {
  fn name(&self) -> &'static str {
    "parquet"
  }

  fn get_conf(&self, key: &str) -> String {
    match key {
      "compression" => compression_to_string(&self.compression),
      _ => panic!("bad conf"),
    }
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    match key {
      "compression" => self.compression = str_to_compression(&value)?,
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: Dtype>(&self, nums: &[T]) -> Vec<u8> {
    let mut res = Vec::new();
    let message_type = format!("message schema {{ REQUIRED {} nums; }}", T::PARQUET_DTYPE_STR);
    let schema = Arc::new(parse_message_type(&message_type).unwrap());
    let mut writer = SerializedFileWriter::new(
      &mut res,
      schema,
      Arc::new(WriterProperties::builder().set_compression(self.compression).build()
      )
    ).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
      {
        let typed = col_writer.typed::<T::Parquet>();
        typed.write_batch(T::slice_to_parquet(nums), None, None).unwrap();
      }
      col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    // let col_desc = ColumnDescPtr::new(ColumnDescriptor::new(
    //   T::parquet_type,
    //   0,
    //   0,
    //   ColumnPath::new(vec!["nums".to_string()]),
    // ));
    // let writer_properties = WriterPropertiesPtr::new(WriterProperties::builder()
    //   .set_compression(self.compression)
    //   .build()
    // );
    // let page_writer = Box::new(SerializedPageWriter::new(&mut res));
    // let mut col_writer = parquet::column::writer::get_column_writer(
    //   col_desc,
    //   writer_properties,
    //   page_writer,
    // );
    // let mut col_writer = parquet::column::writer::get_typed_column_writer::<T::Parquet>(col_writer);
    // col_writer.write_batch(nums, None, None).unwrap();
    res
  }

  fn decompress<T: Dtype>(&self, bytes: &[u8]) -> Vec<T> {
    // couldn't find a way to make a parquet reader without a fully copy of the compressed bytes;
    // maybe this can be improved
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes.to_vec())).unwrap();

    let parquet_metadata = reader.metadata();
    let mut n = 0;
    for row_group_meta in parquet_metadata.row_groups() {
      n += row_group_meta.num_rows();
    }

    let mut res = Vec::with_capacity(n as usize);
    unsafe {
      res.set_len(n as usize);
    }
    for i in 0..parquet_metadata.num_row_groups() {
      let row_group_reader = reader.get_row_group(i).unwrap();
      let mut col_reader = get_typed_column_reader::<T::Parquet>(
        row_group_reader.get_column_reader(0).unwrap()
      );
      col_reader.read_records(usize::MAX, None, None, &mut res).unwrap();
    }
    // let col_desc = ColumnDescPtr::new(ColumnDescriptor::new(
    //   T::parquet_type,
    //   0,
    //   0,
    //   ColumnPath::new(vec!["nums".to_string()]),
    // ));
    // let page_reader = Box::new(SerializedPageReader::new(
    //
    // ));
    // let col_reader = parquet::column::reader::get_column_reader(
    //   col_desc,
    //   page_reader,
    // );
    // let mut col_reader = parquet::column::reader::get_typed_column_reader::<T::Parquet>(col_reader);
    // col_reader.read_records(usize::MAX, None, None, &mut res).unwrap();
    T::vec_from_parquet(res)
  }
}
