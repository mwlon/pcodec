import numpy as np
from pcodec import auto_compress, simple_decompress_into
import pytest

np.random.seed(12345)

@pytest.mark.parametrize(
    "shape",
    [
        (100,),
        (100, 100),
        (10, 10, 100),
        (2, 10, 10, 50),
    ],
)
@pytest.mark.parametrize("dtype", ['f4', 'f8', 'i4', 'i8', 'u4', 'u8'])
def test_round_trip(shape, dtype):
    data = np.random.uniform(0, 1000, size=shape).astype(dtype)
    compressed = auto_compress(data)
    out = np.empty_like(data)
    simple_decompress_into(compressed, out)
    np.testing.assert_array_equal(data, out)
    with pytest.raises(RuntimeError, match="too small"):
        simple_decompress_into(compressed, np.empty_like(out, dtype=dtype)[:-1])
