use crate::chunk_config::ChunkConfig;
use crate::errors::{ErrorKind, PcoResult};
use crate::wrapped::{FileCompressor, FileDecompressor, PageDecompressor};
use crate::PagingSpec;
use std::cmp::min;

struct Chunk {
  nums: Vec<i32>,
  config: ChunkConfig,
}

fn try_decompressing_page_until_sufficient_data(
  pd: &mut PageDecompressor<i32>,
  src: &[u8],
  page_n: usize,
) -> PcoResult<(Vec<i32>, usize)> {
  // we try adding more data incrementally to test that the
  // PageDecompressor doesn't get into a bad state

  let backoff = 1.3;
  let mut n_bytes = 0;
  let mut nums = vec![0; page_n];
  loop {
    match pd.decompress(&src[..n_bytes], &mut nums) {
      Ok((progress, additional)) => {
        assert_eq!(progress.n_processed, page_n);
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

  let mut n_per_pages = Vec::new();
  for chunk in chunks {
    let cc = fc.chunk_compressor(&chunk.nums, &chunk.config)?;
    cc.write_chunk_meta(&mut compressed)?;
    for page_idx in 0..cc.n_per_page().len() {
      cc.write_page(page_idx, &mut compressed)?;
    }
    n_per_pages.push(cc.n_per_page().to_vec());
  }

  // DECOMPRESS
  let (fd, mut consumed) = FileDecompressor::new(&compressed)?;
  for (chunk_idx, chunk) in chunks.iter().enumerate() {
    let (cd, additional) = fd.chunk_decompressor(&compressed[consumed..])?;
    consumed += additional;

    let mut page_start = 0;
    for &page_n in &n_per_pages[chunk_idx] {
      let page_end = page_start + page_n;
      let (mut pd, additional) = cd.page_decompressor(page_n, &compressed[consumed..])?;
      consumed += additional;
      let (page_nums, additional) =
        try_decompressing_page_until_sufficient_data(&mut pd, &compressed[consumed..], page_n)?;
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
      nums: (0..1700).collect::<Vec<_>>(),
      config: ChunkConfig {
        delta_encoding_order: Some(0),
        paging_spec: PagingSpec::EqualPagesUpTo(600),
        ..Default::default()
      },
    },
    Chunk {
      nums: (0..500).collect::<Vec<_>>(),
      config: ChunkConfig {
        delta_encoding_order: Some(2),
        paging_spec: PagingSpec::ExactPageSizes(vec![1, 499]),
        ..Default::default()
      },
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
