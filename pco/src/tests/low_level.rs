use std::cmp::min;
use crate::chunk_config::ChunkConfig;
use crate::errors::{ErrorKind, PcoResult};
use crate::PagingSpec;
use crate::wrapped::{FileCompressor, FileDecompressor, PageDecompressor};

struct Chunk {
  nums: Vec<i32>,
  config: ChunkConfig,
}

fn try_decompressing_page_until_sufficient_data(
  pd: &mut PageDecompressor<i32>,
  src: &[u8],
  page_size: usize,
) -> PcoResult<(Vec<i32>, usize)> {
  // we try adding more data incrementally to test that the
  // PageDecompressor doesn't get into a bad state

  let backoff = 1.2;
  let mut n_bytes = 0;
  let mut nums = vec![0; page_size];
  loop {
    match pd.decompress(&src[..n_bytes], &mut nums) {
      Ok((progress, additional)) => {
        assert_eq!(progress.n_processed, page_size);
        assert!(progress.finished_page);
        return Ok((nums, additional));
      }
      Err(e) if matches!(e.kind, ErrorKind::InsufficientData) => (),
      Err(e) => panic!("{}", e),
    }

    assert!(n_bytes < src.len());
    n_bytes = min(
      (n_bytes as f32 * backoff) as usize + 1,
      src.len(),
    );
  }
}

fn test_wrapped(chunks: &[Chunk]) -> PcoResult<()> {
  // COMPRESS
  let mut compressed = Vec::new();
  let fc = FileCompressor::default();
  fc.write_header(&mut compressed)?;

  let mut page_sizess = Vec::new();
  for chunk in chunks {
    let cc = fc.chunk_compressor(&chunk.nums, &chunk.config)?;
    cc.write_chunk_meta(&mut compressed)?;
    for page_idx in 0..cc.page_sizes().len() {
      cc.write_page(page_idx, &mut compressed)?;
    }
    page_sizess.push(cc.page_sizes().to_vec());
  }

  // DECOMPRESS
  let (fd, mut consumed) = FileDecompressor::new(&compressed)?;
  for (chunk_idx, chunk) in chunks.iter().enumerate() {
    let (cd, additional) = fd.chunk_decompressor(&compressed[consumed..])?;
    consumed += additional;

    let mut page_start = 0;
    for &page_size in &page_sizess[chunk_idx] {
      let page_end = page_start + page_size;
      let (mut pd, additional) = cd.page_decompressor(page_size, &compressed[consumed..])?;
      consumed += additional;
      let (page_nums, additional) = try_decompressing_page_until_sufficient_data(
        &mut pd,
        &compressed[consumed..],
        page_size,
      )?;
      assert_eq!(&page_nums, &chunk.nums[page_start..page_end]);
      consumed += additional;
      page_start = page_end;
    }
  }

  Ok(())
}

#[test]
fn test_low_level_wrapped() -> PcoResult<()> {
  test_wrapped(&[
    Chunk {
      nums: (0..1111).collect::<Vec<_>>(),
      config: ChunkConfig {
        delta_encoding_order: Some(0),
        paging_spec: PagingSpec::EqualPagesUpTo(500),
        ..Default::default() },
    },
    Chunk {
      nums: vec![1, 2, 3],
      config: ChunkConfig::default(),
    },
    Chunk {
      nums: vec![1, 2, 3],
      config: ChunkConfig {
        paging_spec: PagingSpec::EqualPagesUpTo(1),
        ..Default::default()
      },
    },
  ])
}
