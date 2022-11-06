use std::convert::TryInto;
use std::io::Write;

use crate::{CompressorConfig, DecompressorConfig};
use crate::chunk_metadata::ChunkSpec;
use crate::data_types::NumberLike;
use crate::errors::QCompressResult;
use crate::wrapped::{Compressor, Decompressor};

pub struct WrappedFormat;

fn encode_usize(x: usize) -> [u8; 4] {
  (x as u32).to_be_bytes()
}

fn decode_usize(bytes: &mut [u8]) -> (usize, &mut [u8]) {
  let res = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
  (res, &mut bytes[4..])
}

// an example implementation of a simple wrapping format
impl WrappedFormat {
  pub fn new() -> Self {
    WrappedFormat
  }

  pub fn compress<T: NumberLike>(
    &mut self,
    nums: &[T],
    config: CompressorConfig,
    sizess: Vec<Vec<usize>>,
  ) -> QCompressResult<Vec<u8>> {
    let mut res = Vec::new();

    let mut compressor = Compressor::<T>::from_config(config);
    compressor.header()?;
    let header = compressor.drain_bytes();
    res.extend(encode_usize(header.len()));
    res.extend(encode_usize(sizess.len()));
    res.extend(header);

    let mut start = 0;
    for sizes in sizess {
      let end = start + sizes.iter().sum::<usize>();
      println!("slicing {}..{}", start, end);
      let chunk_nums = &nums[start..end];
      start = end;
      let spec = ChunkSpec::default().with_page_sizes(sizes.clone());
      println!("nums {:?} spec {:?}", chunk_nums, spec);

      compressor.chunk_metadata(chunk_nums, &spec)?;
      println!("compress meta");
      let meta = compressor.drain_bytes();
      res.extend(encode_usize(meta.len()));
      res.extend(encode_usize(sizes.len()));
      res.extend(meta);

      println!("compress pages: {}", res.len());
      for size in sizes {
        compressor.data_page()?;
        let page = compressor.drain_bytes();
        res.extend(encode_usize(page.len()));
        res.extend(encode_usize(size));
        res.extend(page);
        println!("size: {}", res.len());
      }
    }

    Ok(res)
  }

  pub fn decompress<T: NumberLike>(
    &mut self,
    mut compressed: Vec<u8>,
    config: DecompressorConfig,
  ) -> QCompressResult<Vec<T>> {
    let mut res = Vec::new();
    let mut decompressor = Decompressor::<T>::from_config(config);

    let buf = &mut compressed;
    let (header_len, buf) = decode_usize(buf);
    let (n_chunks, mut buf) = decode_usize(buf);
    decompressor.write_all(&buf[..header_len]).unwrap();
    let flags = decompressor.header()?;
    println!("{:?}", flags);
    buf = &mut buf[header_len..];

    for _ in 0..n_chunks {
      println!("start chunk");
      let (meta_len, newbuf) = decode_usize(buf);
      buf = newbuf;
      let (n_pages, newbuf) = decode_usize(buf);
      buf = newbuf;
      decompressor.write_all(&buf[..meta_len]).unwrap();
      println!("{} {}", meta_len, n_pages);
      let meta = decompressor.chunk_metadata()?.unwrap();
      println!("meta {:?}", meta);
      buf = &mut buf[meta_len..];

      for _ in 0..n_pages {
        println!("start page");
        let (page_len, newbuf) = decode_usize(buf);
        buf = newbuf;
        let (size, newbuf) = decode_usize(buf);
        buf = newbuf;
        println!("page stuff {} {}", page_len, size);
        decompressor.write_all(&buf[..page_len]).unwrap();
        res.extend(decompressor.data_page(size, page_len)?);
        decompressor.free_compressed_memory();
        buf = &mut buf[page_len..];
        println!("end page {:?}", res);
      }
    }

    Ok(res)
  }
}

#[test]
fn test_dummy_wrapped_format_recovery() -> QCompressResult<()> {
  let nums = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  let config = CompressorConfig {
    delta_encoding_order: 2,
    ..Default::default()
  };
  let sizess = vec![vec![4, 2, 1], vec![3]];
  let compressed = WrappedFormat::new().compress(&nums, config, sizess)?;
  println!("COMPRESSED TO {:?} BYTES", compressed.len());
  let recovered = WrappedFormat::new().decompress::<i32>(compressed, DecompressorConfig::default())?;
  println!("GOT BACK {:?}", recovered);
  assert_eq!(recovered, nums);
  Ok(())
}