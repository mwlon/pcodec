# Known Panics in `q_compress`

* Decompressing with insufficient data available causes a panic.
Attempting to replacing these with `Result`s caused ~20% decompression
performance degradation, so it is up to the user to ensure you have provided
enough data.
* Decompressing delta-encoded timestamps that return out of bounds causes a
panic.
Attempting to use results instead of panics in this code path caused ~5%
performance degradations for decompressing delta-encoded data for any data
type.
