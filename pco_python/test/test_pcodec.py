import numpy as np
from pcodec import compress, decompress
import pytest

rng = np.random.default_rng(12345)

@pytest.mark.parametrize(
    "shape",
    [
        (100,),
        (100, 100),
        (10, 10, 100),
        (2, 10, 10, 50),
    ],
)
@pytest.mark.parametrize("dtype", ['f4', 'f8'])
def test_round_trip(shape, dtype):
    data = rng.random(shape, dtype)
    compressed = compress(data)
    out = np.empty_like(data)
    decompress(compressed, out)
    np.testing.assert_array_equal(data, out)
    with pytest.raises(RuntimeError, match="too small"):
        decompress(compressed, np.empty_like(out, dtype=dtype)[:-1])
