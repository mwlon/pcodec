use std::sync::Arc;

use anyhow::{anyhow, Result};
use parquet::basic::{Compression, ZstdLevel};
use parquet::column::reader::get_typed_column_reader;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::file::reader::FileReader;
use parquet::file::reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::bench::codecs::CodecInternal;
use crate::dtypes::PcoNumberLike;

const ZSTD: &str = "zstd";

#[derive(Clone, Debug)]
pub struct ParquetConfig {
  compression: Compression,
  group_size: usize,
}

impl Default for ParquetConfig {
  fn default() -> Self {
    Self {
      compression: Compression::UNCOMPRESSED,
      // Larger group sizes work better on some datasets, and smaller ones on
      // others, sometimes with dramatic impact.
      // Based on experiments with zstd compression, 2^20 seems like a good default.
      group_size: 1 << 20,
    }
  }
}

fn str_to_compression(s: &str) -> Result<Compression> {
  let res = match s.to_lowercase().as_str() {
    "uncompressed" => Compression::UNCOMPRESSED,
    "snappy" => Compression::SNAPPY,
    _ => {
      if let Some(zstd_level_str) = s.strip_prefix(ZSTD) {
        let level = if zstd_level_str.is_empty() {
          ZstdLevel::default()
        } else {
          ZstdLevel::try_new(zstd_level_str.parse::<i32>()?)?
        };
        Compression::ZSTD(level)
      } else {
        return Err(anyhow!("unknown parquet codec {}", s));
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
    _ => unreachable!(),
  }
}

// This approach compresses the vector as
impl CodecInternal for ParquetConfig {
  fn name(&self) -> &'static str {
    "parquet"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![
      (
        "compression",
        compression_to_string(&self.compression),
      ),
      ("group_size", self.group_size.to_string()),
    ]
  }

  fn set_conf(&mut self, key: &str, value: String) -> Result<()> {
    match key {
      "compression" => self.compression = str_to_compression(&value)?,
      "group_size" => self.group_size = value.parse().unwrap(),
      _ => return Err(anyhow!("unknown conf: {}", key)),
    }
    Ok(())
  }

  fn compress<T: PcoNumberLike>(&self, nums: &[T]) -> Vec<u8> {
    let mut res = Vec::new();
    let message_type = format!(
      "message schema {{ REQUIRED {} nums; }}",
      T::PARQUET_DTYPE_STR
    );
    let schema = Arc::new(parse_message_type(&message_type).unwrap());
    let properties_builder = WriterProperties::builder()
      .set_writer_version(WriterVersion::PARQUET_2_0)
      .set_compression(self.compression);
    let mut writer = SerializedFileWriter::new(
      &mut res,
      schema,
      Arc::new(properties_builder.build()),
    )
    .unwrap();

    for col_chunk in nums.chunks(self.group_size) {
      let mut row_group_writer = writer.next_row_group().unwrap();
      let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
      let typed = col_writer.typed::<T::Parquet>();
      typed
        .write_batch(T::nums_to_parquet(col_chunk), None, None)
        .unwrap();
      col_writer.close().unwrap();
      row_group_writer.close().unwrap();
    }
    writer.close().unwrap();

    res
  }

  fn decompress<T: PcoNumberLike>(&self, bytes: &[u8]) -> Vec<T> {
    // couldn't find a way to make a parquet reader without a fully copy of the compressed bytes;
    // maybe this can be improved
    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes.to_vec())).unwrap();

    let parquet_meta = reader.metadata();
    let mut n = 0;
    for row_group_meta in parquet_meta.row_groups() {
      n += row_group_meta.num_rows();
    }

    let mut res = Vec::with_capacity(n as usize);
    unsafe {
      res.set_len(n as usize);
    }
    let mut start = 0;
    for i in 0..parquet_meta.num_row_groups() {
      let row_group_reader = reader.get_row_group(i).unwrap();
      let mut col_reader =
        get_typed_column_reader::<T::Parquet>(row_group_reader.get_column_reader(0).unwrap());
      let (n_records_read, _, _) = col_reader
        .read_records(usize::MAX, None, None, &mut res[start..])
        .unwrap();
      start += n_records_read
    }

    T::parquet_to_nums(res)
  }
}
