use crate::chunk_config::ChunkConfig;
use crate::errors::PcoResult;
use crate::wrapped::{FileCompressor, FileDecompressor, PageDecompressor};
use crate::{PagingSpec, FULL_BATCH_N};
use std::cmp::min;

struct Chunk {
  nums: Vec<u32>,
  config: ChunkConfig,
}

fn decompress_by_batch<'a>(
  pd: &mut PageDecompressor<u32>,
  mut src: &'a [u8],
  page_n: usize,
) -> PcoResult<(Vec<u32>, &'a [u8])> {
  let mut nums = vec![0; page_n];
  let mut start = 0;
  loop {
    let end = min(start + FULL_BATCH_N, page_n);
    let batch_size = end - start;
    let (progress, new_src) = pd.decompress(src, &mut nums[start..end])?;
    src = new_src;
    assert_eq!(progress.n_processed, batch_size);
    start = end;
    if end == page_n {
      assert!(progress.finished_page);
    }
    if progress.finished_page {
      break;
    }
  }
  Ok((nums, src))
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
  let (fd, mut src) = FileDecompressor::new(compressed.as_slice())?;
  for (chunk_idx, chunk) in chunks.iter().enumerate() {
    let (cd, new_src) = fd.chunk_decompressor(src)?;
    src = new_src;

    let mut page_start = 0;
    for &page_n in &n_per_pages[chunk_idx] {
      let page_end = page_start + page_n;
      let (mut pd, new_src) = cd.page_decompressor(page_n, src)?;
      src = new_src;
      let (page_nums, new_src) = decompress_by_batch(&mut pd, src, page_n)?;
      src = new_src;
      assert_eq!(&page_nums, &chunk.nums[page_start..page_end]);
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
