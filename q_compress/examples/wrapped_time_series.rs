// This example compresses and decompresses a simple TimeSeries struct.
// The wrapped format here attaches min and max timestamp metadata, which
// allows faster decompression for queries filtering on a < timestamp < b.

use std::convert::{TryFrom, TryInto};
use std::io::Write;
use std::time::{Duration, Instant, SystemTime};
use rand::Rng;

use q_compress::data_types::{NumberLike, TimestampNanos};
use q_compress::errors::QCompressResult;
use q_compress::wrapped::{ChunkSpec, Compressor, Decompressor};

#[derive(Clone, Debug, Default)]
struct TimeSeries {
  timestamps: Vec<SystemTime>,
  values: Vec<f32>,
}

impl TimeSeries {
  fn len(&self) -> usize {
    self.timestamps.len()
  }

  fn q_timestamps(&self) -> QCompressResult<Vec<TimestampNanos>> {
    let mut res = Vec::with_capacity(self.len());
    for &t in &self.timestamps {
      res.push(TimestampNanos::try_from(t)?);
    }
    Ok(res)
  }
}

fn compress_time_series(series: &TimeSeries) -> QCompressResult<Vec<u8>> {
  const DATA_PAGE_SIZE: usize = 3000;

  let mut data_page_sizes = Vec::new();
  let mut count = series.len();
  while count > 0 {
    let page_size = count.min(DATA_PAGE_SIZE);
    data_page_sizes.push(page_size);
    count -= page_size;
  }
  let chunk_spec = ChunkSpec::default().with_page_sizes(data_page_sizes.clone());
  let q_timestamps = series.q_timestamps()?;

  let mut res = Vec::new();
  let mut t_compressor = Compressor::<TimestampNanos>::from_config(
    q_compress::auto_compressor_config(&q_timestamps, q_compress::DEFAULT_COMPRESSION_LEVEL)
  );
  let mut v_compressor = Compressor::<f32>::from_config(
    q_compress::auto_compressor_config(&series.values, q_compress::DEFAULT_COMPRESSION_LEVEL)
  );


  t_compressor.header()?;
  t_compressor.chunk_metadata(&q_timestamps, &chunk_spec)?;
  res.extend((t_compressor.byte_size() as u32).to_be_bytes());
  res.extend(t_compressor.drain_bytes());

  v_compressor.header()?;
  v_compressor.chunk_metadata(&series.values, &chunk_spec)?;
  res.extend((v_compressor.byte_size() as u32).to_be_bytes());
  res.extend(v_compressor.drain_bytes());

  let mut idx = 0;
  for page_idx in 0..((series.len() - 1) / DATA_PAGE_SIZE) {
    // Each page consists of
    // 1. count
    // 2. timestamp min and max (for fast decompression filtering)
    // 3. timestamp compressed body size
    // 4. timestamp page
    // 5. values compressed body size
    // 6. values page

    // 1.
    let page_size = data_page_sizes[page_idx];
    res.extend((page_size as u32).to_be_bytes());

    // 2.
    // There's no reason you have to serialize the timestamp metadata in the
    // same way as q_compress. Here we do it out of convenience.
    let t_min = q_timestamps[idx];
    idx += page_size;
    let t_max = q_timestamps[idx - 1];
    res.extend(t_min.to_bytes());
    res.extend(t_max.to_bytes());

    // 3.
    t_compressor.data_page()?;
    res.extend((t_compressor.byte_size() as u32).to_be_bytes());

    // 4.
    res.extend(t_compressor.drain_bytes());

    // 5.
    v_compressor.data_page()?;
    res.extend((v_compressor.byte_size() as u32).to_be_bytes());

    // 6.
    res.extend(v_compressor.drain_bytes());
  }

  Ok(res)
}

fn decompress_time_series_between(mut compressed: &[u8], t0: SystemTime, t1: SystemTime) -> QCompressResult<TimeSeries> {
  let mut series = TimeSeries::default();
  let ts = &mut series.timestamps;
  let vs = &mut series.values;

  let mut t_decompressor = Decompressor::<TimestampNanos>::default();
  let mut v_decompressor = Decompressor::<f32>::default();

  let mut size;
  fn read_usize(slice: &[u8]) -> (&[u8], usize) {
    let byte_size = u32::from_be_bytes(slice[0..4].try_into().unwrap());
    (&slice[4..], byte_size as usize)
  }

  (compressed, size) = read_usize(compressed);
  t_decompressor.write_all(&compressed[..size]).unwrap();
  t_decompressor.header()?;
  t_decompressor.chunk_metadata()?;
  compressed = &compressed[size..];

  (compressed, size) = read_usize(compressed);
  v_decompressor.write_all(&compressed[..size]).unwrap();
  v_decompressor.header()?;
  v_decompressor.chunk_metadata()?;
  compressed = &compressed[size..];

  while !compressed.is_empty() {
    (compressed, size) = read_usize(compressed);
    let n = size;

    let t_min = TimestampNanos::from_bytes(compressed[..8].try_into().unwrap())?;
    compressed = &compressed[8..];

    if SystemTime::from(t_min) > t1 {
      break;
    }

    let t_max = TimestampNanos::from_bytes(compressed[..8].try_into().unwrap())?;
    compressed = &compressed[8..];

    if SystemTime::from(t_max) < t0 {
      // we can skip this data
      (compressed, size) = read_usize(compressed);
      compressed = &compressed[size..];
      (compressed, size) = read_usize(compressed);
      compressed = &compressed[size..];
    } else {
      // we need to filter and append this data
      (compressed, size) = read_usize(compressed);
      t_decompressor.write_all(&compressed[..size]).unwrap();
      let page_t = t_decompressor.data_page(n, size)?;
      compressed = &compressed[size..];

      (compressed, size) = read_usize(compressed);
      v_decompressor.write_all(&compressed[..size]).unwrap();
      let page_v = v_decompressor.data_page(n, size)?;
      compressed = &compressed[size..];

      let filtered = page_t.into_iter()
        .zip(page_v)
        .filter(|(t, _)| (t0..t1).contains(&SystemTime::from(*t)))
        .collect::<Vec<_>>();

      ts.extend(filtered.iter().map(|(t, _)| SystemTime::from(*t)));
      vs.extend(filtered.into_iter().map(|(_, v)| v));
    }
  }

  Ok(series)
}

fn main() -> QCompressResult<()> {
  let mut rng = rand::thread_rng();

  let mut series = TimeSeries::default();
  let t0 = SystemTime::now();
  let mut t = t0;
  let mut v = 100.0;
  for _ in 0..100000 {
    t += Duration::from_nanos(1_000_000_000 + rng.gen_range(0..1_000_000));
    v += rng.gen_range(0.0..1.0);
    series.timestamps.push(t);
    series.values.push(v);
  }

  let compressed = compress_time_series(&series)?;
  println!("compressed to {} bytes", compressed.len());

  let filter_t0 = t0 + Duration::from_secs(10000);
  let filter_t1 = t0 + Duration::from_secs(20000);
  let benchmark_instant = Instant::now();
  let decompressed = decompress_time_series_between(
    &compressed,
    filter_t0,
    filter_t1,
  )?;
  let benchmark_dt = Instant::now() - benchmark_instant;
  println!(
    "decompressed {} numbers matching filter in {:?}",
    decompressed.len(),
    benchmark_dt,
  );

  Ok(())
}