import numpy as np
from pcodec import auto_compress, simple_decompress_into, auto_decompress_f32
import pytest

np.random.seed(12345)

all_shapes = [
    (100,),
    (100, 100),
    (10, 10, 100),
    (2, 10, 10, 50),
]

@pytest.mark.parametrize("shape", all_shapes)
@pytest.mark.parametrize("dtype", ['f4', 'f8', 'i4', 'i8', 'u4', 'u8'])
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
def test_round_trip_auto_decompress_f32(shape, dtype='f4'):
    data = np.random.uniform(0, 1000, size=shape).astype(dtype)
    compressed = auto_compress(data)

    out = auto_decompress_f32(compressed)
    # data are decompressed into a 1D array
    np.testing.assert_array_equal(data.ravel(), out)


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

def test_errors():
    data = np.random.uniform(size=100).astype(np.float32)
    compressed = auto_compress(data)

    out = np.zeros(100).astype(np.float64)
    with pytest.raises(RuntimeError):
        simple_decompress_into(compressed, out)

