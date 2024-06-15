import numpy as np
from pcodec import ChunkConfig, FloatMultSpec, FloatQuantSpec, IntMultSpec, PagingSpec, standalone
import pytest

np.random.seed(12345)

all_shapes = (
  [],
  [900],
  [9, 100],
)

all_dtypes = ("f2", "f4", "f8", "i2", "i4", "i8", "u2", "u4", "u8")

all_int_mult_specs = (
    IntMultSpec.enabled(),
    IntMultSpec.disabled(),
    IntMultSpec.provided(2),
    IntMultSpec.provided(10),
)

all_float_mult_specs = (
    FloatMultSpec.enabled(),
    FloatMultSpec.disabled(),
    FloatMultSpec.provided(np.pi),
    FloatMultSpec.provided(10 * np.pi),
)

all_float_quant_specs = (
    FloatQuantSpec.disabled(),
    FloatQuantSpec.provided(4),
    FloatQuantSpec.provided(16),
)


@pytest.mark.parametrize("shape", all_shapes)
@pytest.mark.parametrize("dtype", all_dtypes)
def test_round_trip_decompress_into(shape, dtype):
  data = np.random.uniform(0, 1000, size=shape).astype(dtype)
  compressed = standalone.simple_compress(data, ChunkConfig())

  # decompress exactly
  out = np.empty_like(data)
  progress = standalone.simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(data, out)
  assert progress.n_processed == data.size
  assert progress.finished


@pytest.mark.parametrize("shape", all_shapes)
@pytest.mark.parametrize("dtype", all_dtypes)
def test_round_trip_simple_decompress(shape, dtype):
  data = np.random.uniform(0, 1000, size=shape).astype(dtype)
  compressed = standalone.simple_compress(data, ChunkConfig(paging_spec=PagingSpec.equal_pages_up_to(300)))
  out = standalone.simple_decompress(compressed)
  # data are decompressed into a 1D array; ensure it can be reshaped to the original shape
  out.shape = shape
  np.testing.assert_array_equal(data, out)


def test_inexact_decompression():
  data = np.random.uniform(size=300)
  compressed = standalone.simple_compress(data, ChunkConfig())

  # decompress partially
  out = np.zeros(3)
  progress = standalone.simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(out, data[:3])
  assert progress.n_processed == 3
  assert not progress.finished

  # decompress with room to spare
  out = np.zeros(600)
  progress = standalone.simple_decompress_into(compressed, out)
  np.testing.assert_array_equal(out[:300], data)
  np.testing.assert_array_equal(out[300:], np.zeros(300))
  assert progress.n_processed == 300
  assert progress.finished

def test_simple_decompress_into_errors():
  """Test possible error states for standalone.simple_decompress_into"""
  data = np.random.uniform(size=100).astype(np.float32)
  compressed = standalone.simple_compress(data, ChunkConfig())

  out = np.zeros(100).astype(np.float64)
  with pytest.raises(RuntimeError, match="data type byte does not match"):
    standalone.simple_decompress_into(compressed, out)


def test_simple_decompress_errors():
  """Test possible error states for standalone.simple_decompress"""
  data = np.random.uniform(size=100).astype(np.float32)
  compressed = bytearray(standalone.simple_compress(data, ChunkConfig()))

  truncated = compressed[:8]
  with pytest.raises(RuntimeError, match="empty bytes"):
      standalone.simple_decompress(bytes(truncated))

  # corrupt the data with unknown dtype byte
  # (is this safe to hard code? could the length of the header change in future version?)
  compressed[8] = 99
  with pytest.raises(RuntimeError, match="unrecognized dtype byte"):
      standalone.simple_decompress(bytes(compressed))

  # this happens if the user passed in a file with no chunks.
  compressed[8] = 0
  assert standalone.simple_decompress(bytes(compressed)) is None


def test_compression_options():
  data = np.random.normal(size=100).astype(np.float32)
  default_size = len(standalone.simple_compress(data, ChunkConfig()))

  # this is mostly just to check that there is no error, but these settings
  # should give worse compression than the defaults
  assert (len(
      standalone.simple_compress(
          data,
          ChunkConfig(
              compression_level=0,
              delta_encoding_order=1,
              int_mult_spec=IntMultSpec.disabled(),
              float_mult_spec=FloatMultSpec.disabled(),
              float_quant_spec=FloatQuantSpec.disabled(),
              paging_spec=PagingSpec.equal_pages_up_to(77),
          ),
      )) > default_size)


@pytest.mark.parametrize("int_mult_spec", all_int_mult_specs)
def test_compression_int_mult_spec_options(int_mult_spec):
  data = (np.random.normal(size=100) * 1000).astype(np.int32)

  # check for errors and that some compressed data was produced
  assert (0 < len(standalone.simple_compress(
      data,
      ChunkConfig(int_mult_spec=int_mult_spec),
  )) < data.nbytes)


@pytest.mark.parametrize("float_mult_spec", all_float_mult_specs)
def test_compression_float_mult_spec_options(float_mult_spec):
  data = (np.random.normal(size=100) * 1000).astype(np.int32) * np.pi

  # check for errors and that some compressed data was produced
  assert (0 < len(standalone.simple_compress(
      data,
      ChunkConfig(float_mult_spec=float_mult_spec),
  )) < data.nbytes)


@pytest.mark.parametrize("float_quant_spec", all_float_quant_specs)
def test_compression_float_quant_spec_options(float_quant_spec):
  data = np.random.normal(size=100).astype(np.float32).astype(np.float64)

  # check for errors and that some compressed data was produced
  assert (0 < len(
      standalone.simple_compress(
          data,
          ChunkConfig(
              float_mult_spec=FloatMultSpec.disabled(),
              float_quant_spec=float_quant_spec,
          ),
      )) < data.nbytes)
