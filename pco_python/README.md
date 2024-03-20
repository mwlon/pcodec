[![Crates.io][crates-badge]][crates-url]

[crates-badge]: https://img.shields.io/pypi/v/pcodec.svg

[crates-url]: https://pypi.org/project/pcodec/

# Pcodec Python API

Pcodec is a codec for numerical sequences. Example usage:

```python
>>> from pcodec import standalone, ChunkConfig
>>> import numpy as np
>>> 
>>> np.random.seed(0)
>>> nums = np.random.normal(size=1000000)
>>> 
>>> # compress
>>> compressed = standalone.simple_compress(nums, ChunkConfig())
>>> print(f'compressed to {len(compressed)} bytes')
compressed to 6946318 bytes
>>> 
>>> # decompress
>>> recovered = standalone.simple_decompress(compressed)
>>> 
>>> np.testing.assert_array_equal(recovered, nums)

```

For pcodec's uses, design, and benchmarks, [see the main repo](https://github.com/mwlon/pcodec).

At the moment, we don't have sphinx + a website set up, so run `help(pcodec)`
(or whatever module name) in Python to read pcodec's documentation.
