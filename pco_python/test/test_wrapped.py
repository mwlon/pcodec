import numpy as np
from pcodec import ChunkConfig, wrapped
import pytest

np.random.seed(12345)
all_dtypes = ('f4', 'f8', 'i4', 'i8', 'u4', 'u8')

@pytest.mark.parametrize("dtype", all_dtypes)
def test_compress(dtype):
  data = np.random.uniform(0, 1000, size=[10]).astype(dtype)
  pco_dtype = dtype[0].upper() + str(int(dtype[1]) * 8)

  # compress
  fc = wrapped.FileCompressor()
  header = fc.write_header()
  cc = fc.chunk_compressor(data, ChunkConfig(max_page_n=5)) # so there are 2 pages
  chunk_meta = cc.write_chunk_meta()
  page0 = cc.write_page(0)
  page1 = cc.write_page(1)
  with pytest.raises(RuntimeError, match="page idx exceeds num pages"):
    cc.write_page(2)

  # decompress
  fd, n_bytes_read = wrapped.FileDecompressor.from_header(header)
  assert n_bytes_read == len(header)
  # check that undershooting is fine
  _, n_bytes_read = wrapped.FileDecompressor.from_header(header + b'foo')
  assert n_bytes_read == len(header)
  cd, n_bytes_read = fd.read_chunk_meta(chunk_meta, pco_dtype)
  assert n_bytes_read == len(chunk_meta)

  # page 1, which has elements 5-10
  dst1 = np.zeros(100).astype(dtype)
  progress, n_bytes_read = cd.read_page_into(page1, 5, dst1)
  np.testing.assert_array_equal(dst1[5:], np.zeros(95))
  np.testing.assert_array_equal(dst1[:5], data[5:])

  # page 0, which has elements 0-5
  dst0 = np.zeros(5).astype(dtype)
  progress, n_bytes_read = cd.read_page_into(page0, 5, dst0)
  np.testing.assert_array_equal(dst0, data[:5])





