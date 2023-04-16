Durendal is a temporary name for a possible successor to q_compress.
It decompresses about 50% faster, owing to
* little-endian instead of big-endian
* less branching by giving each bin size exactly 2^n
* addressing raw bytes instead of usizes

More features to come.