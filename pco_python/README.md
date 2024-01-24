[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/pypi/v/pcodec.svg
[crates-url]: https://pypi.org/project/pcodec/

# Pcodec Python API

Pcodec is a codec for numerical sequences. Example usage:

```python
import pcodec
import numpy as np

nums = np.random.normal(size=1000000)

# compress
compressed = pcodec.auto_compress(nums)
print(f'compressed to {len(compressed)} bytes')

# decompress
recovered = pcodec.auto_decompress(compressed)

np.testing.assert_array_equal(recovered, nums)
```

For pcodec's uses, design, and benchmarks, [see the main repo](https://github.com/mwlon/pcodec).
