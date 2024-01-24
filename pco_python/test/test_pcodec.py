import numpy as np
from pcodec import auto_compress, simple_decompress_into, auto_decompress
import pytest

np.random.seed(12345)

all_shapes = (
  [],
  [900],
  [9, 100],
)

all_dtypes = ('f4', 'f8', 'i4', 'i8', 'u4', 'u8')

@pytest.mark.parametrize("shape", all_shapes)
@pytest.mark.parametrize("dtype", all_dtypes)
def test_round_trip_decompress_into(shape, dtype):
  data = np.random.uniform(0, 1000, size=shape).astype(dtype)
  compressed = auto_compress(data)

  # decompress exactly
  out = np.empty_like(data)
  progress = simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(data, out)
  assert progress.n_processed == data.size
  assert progress.finished


@pytest.mark.parametrize("shape", all_shapes)
@pytest.mark.parametrize("dtype", all_dtypes)
def test_round_trip_auto_decompress(shape, dtype):
  data = np.random.uniform(0, 1000, size=shape).astype(dtype)
  compressed = auto_compress(data, max_page_n=300)
  out = auto_decompress(compressed)
  # data are decompressed into a 1D array; ensure it can be reshaped to the original shape
  out.shape = shape
  np.testing.assert_array_equal(data, out)


def test_inexact_decompression():
  data = np.random.uniform(size=300)
  compressed = auto_compress(data)

  # decompress partially
  out = np.zeros(3)
  progress = simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(out, data[:3])
  assert progress.n_processed == 3
  assert not progress.finished

  # decompress with room to spare
  out = np.zeros(600)
  progress = simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(out[:300], data)
  np.testing.assert_array_equal(out[300:], np.zeros(300))
  assert progress.n_processed == 300
  assert progress.finished

def test_simple_decompress_into_errors():
  """Test possible error states for simple_decompress_into"""
  data = np.random.uniform(size=100).astype(np.float32)
  compressed = auto_compress(data)

  out = np.zeros(100).astype(np.float64)
  with pytest.raises(RuntimeError, match="data type byte does not match"):
    simple_decompress_into(compressed, out)


def test_auto_decompress_errors():
  """Test possible error states for auto_decompress"""
  data = np.random.uniform(size=100).astype(np.float32)
  compressed = bytearray(auto_compress(data))

  truncated = compressed[:8]
  with pytest.raises(RuntimeError, match="chunk data is empty"):
      auto_decompress(bytes(truncated))

  # corrupt the data with unknown dtype byte
  # (is this safe to hard code? could the length of the header change in future version?)
  compressed[8] = 99
  with pytest.raises(RuntimeError, match="unrecognized dtype byte"):
      auto_decompress(bytes(compressed))

  # this happens if the user passed in a file with no chunks.
  compressed[8] = 0
  assert auto_decompress(bytes(compressed)) is None


def test_compression_options():
  data = np.random.normal(size=100).astype(np.float32)
  default_size = len(auto_compress(data))

  # this is mostly just to check that there is no error, but these settings
  # should give worse compression than the defaults
  assert len(auto_compress(
    data,
    compression_level=0,
    delta_encoding_order=1,
    int_mult_spec='disabled',
    float_mult_spec='DISABLED',
    max_page_n=77,
  )) > default_size
