[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/pypi/v/pcodec.svg
[crates-url]: https://pypi.org/project/pcodec/

# Pcodec Python API

Pcodec is a codec for numerical sequences. Example usage:

```python
import pcodec
import numpy

n = 1000000
nums = np.random.normal(size=n)

# compress
compressed = pcodec.auto_compress(nums)
print(f'compressed to {len(compressed)} bytes')

# decompress
recovered = np.empty(n)
pcodec.simple_decompress_into(compressed, recovered)

assert np.testing.array_equal(recovered, nums)
```

For pcodec's uses, design, and benchmarks, [see the main repo](https://github.com/mwlon/pcodec).
