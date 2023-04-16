# python 3
# pip requirement: numpy, pyarrow

import numpy as np
import pyarrow as pa
from pyarrow import parquet as pq
from datetime import datetime
import os

n = 1000000
short_n = 3000

base_dir = 'bench/data'

os.makedirs(f'{base_dir}/txt', exist_ok=True)
os.makedirs(f'{base_dir}/parquet', exist_ok=True)
os.makedirs(f'{base_dir}/snappy_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/gzip_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/zstd_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/binary', exist_ok=True)

def write_parquet_tables(arr, full_name):
  print(f'writing parquet for {full_name}...')
  table = pa.Table.from_pydict({'nums': arr})
#   pq.write_table(table, f'{base_dir}/parquet/{full_name}.parquet', compression='NONE')
#   pq.write_table(table, f'{base_dir}/snappy_parquet/{full_name}.snappy.parquet', compression='snappy')
#   pq.write_table(table, f'{base_dir}/gzip_parquet/{full_name}.gzip.parquet', compression='gzip', compression_level=6)
  pq.write_table(table, f'{base_dir}/zstd_parquet/{full_name}.zstd.parquet', compression='zstd', compression_level=3)

def write_generic(strs, arr, full_name):
  joined = '\n'.join(strs)
  with open(f'{base_dir}/txt/{full_name}.txt', 'w') as f:
    f.write(joined)
  with open(f'{base_dir}/binary/{full_name}.bin', 'wb') as f:
    f.write(arr.tobytes())
  write_parquet_tables(arr, full_name)

def write_i64(arr, name):
  if arr.dtype != np.int64:
    arr = np.floor(arr).astype(np.int64)
  strs = [str(x) for x in arr]
  full_name = f'i64_{name}'
  write_generic(strs, arr, full_name)

def write_bool(arr, name):
  if arr.dtype != np.int8:
    arr = np.floor(arr).astype(np.int8)
  strs = [str(x) for x in arr]
  full_name = f'bool_{name}'
  write_generic(strs, arr, full_name)

def write_f64(arr, name):
  arr = arr.astype(np.float64)
  strs = [str(x) for x in arr]
  full_name = f'f64_{name}'
  write_generic(strs, arr, full_name)

def write_timestamp_micros(arr, name):
  if arr.dtype != np.int64:
    arr = np.floor(arr).astype(np.int64)
  ts = [datetime.utcfromtimestamp(x / 10 ** 6) for x in arr]
  strs = [x.strftime('%Y-%m-%dT%H:%M:%S:%fZ') for x in ts]
  full_name = f'micros_{name}'
  write_generic(strs, arr, full_name)

np.random.seed(0)
write_i64(np.random.geometric(p=0.5, size=n), 'geo2')

def fixed_median_lomax(a, median):
  unscaled_median = 2 ** (1 / a) - 1
  return np.random.pareto(a=a, size=n) / unscaled_median * median
np.random.seed(0)
lomax05 = fixed_median_lomax(0.5, 1000)
write_i64(lomax05, 'lomax05_long')
write_i64(lomax05[:short_n], 'lomax05_short')
np.random.seed(0)
write_i64(fixed_median_lomax(2.5, 1000), 'lomax25')

np.random.seed(0)
write_i64(np.random.randint(-2**63, 2**63, size=n), 'uniform')

write_i64(np.repeat(77777, n), 'constant')

np.random.seed(0)
write_i64(np.random.binomial(1, p=0.01, size=n), 'sparse')

np.random.seed(0)
dollars = np.floor(fixed_median_lomax(1.5, 5)).astype(np.int64)
cents = np.random.randint(0, 100, size=n)
p = np.random.uniform(size=n)
cents[p < 0.9] = 99
cents[p < 0.75] = 98
cents[p < 0.6] = 95
cents[p < 0.45] = 75
cents[p < 0.4] = 50
cents[p < 0.25] = 25
cents[p < 0.15] = 0
total_cents = dollars * 100 + cents
write_i64(dollars, 'dollars')
write_i64(cents, 'cents')
write_i64(total_cents, 'total_cents')

np.random.seed(0)
amplitude = 100000
periods = 103 # something prime
period = n / periods
slow_cosine = 100000 * np.cos(np.arange(n) * 2 * np.pi / (period))
write_i64(slow_cosine, 'slow_cosine')
write_f64(slow_cosine, 'slow_cosine')

np.random.seed(0)
write_f64(np.random.normal(size=n), 'normal_at_0')
np.random.seed(0)
write_f64(np.random.normal(loc=1E6, size=n), 'normal_at_1M')

# timestamps increasing 1s at a time on average from 2022-01-01T00:00:00 with
# 1s random jitter
def nl_timestamps():
  return 10**6 * (1640995200 + np.arange(n) + np.random.normal(size=n))
np.random.seed(0)
write_timestamp_micros(nl_timestamps(), 'near_linear')

# millisecond timestamps compressed as microseconds
def milli_micro_timestamps():
  return 10 ** 3 * (1640995200000  + np.random.randint(0, 10 ** 9, size=n))
np.random.seed(0)
write_timestamp_micros(milli_micro_timestamps(), 'millis')

# integers compressed as floats
np.random.seed(0)
write_f64(np.random.randint(0, 2 ** 30, size=n), 'integers')

# decimal floats
np.random.seed(0)
write_f64(np.random.randint(1000, 10000, size=n) / 100, 'decimal')

# 10 interleaved 1st order sequences with different scales
np.random.seed(0)
deltas = np.random.randint(-10, 10, size=[n // 10, 10])
bases = 10 ** np.arange(10)
interleaved = bases[None, :] + np.cumsum(deltas, axis=0)
write_i64(interleaved.reshape(-1), 'interl')

# the same as interleaved, but shuffled within each group of 10
np.random.seed(0)
idxs = np.random.rand(*interleaved.shape).argsort(axis=1)
interleaved_scrambled = np.take_along_axis(interleaved, idxs, axis=1)
write_i64(interleaved_scrambled.reshape(-1), 'interl_scrambl')

# randomly one of 3 distinct values
np.random.seed(0)
values = [77, 777, 7777]
bad_huffman = np.random.choice(values, size=n, p=[0.5, 0.49, 0.01])
write_i64(bad_huffman, 'bad_huffman')

# a sequence whose variance gradually increases
np.random.seed(0)
log_std = np.linspace(-1.5, 25, n)
dist_shift = 0.5 + np.exp(log_std) * np.random.normal(size=n)
write_i64(dist_shift, 'dist_shift')

# a diabolically hard sequence
# * float decimals plus small jitter
# * numerous interleaved subsequences with occasionally missing elements
# * each subsequence benefits from delta
# * distribution shift on delta size
np.random.seed(0)
subseq_idx = 0
n_subseqs = 77
subseq_vals = np.random.randint(1E4, 1E5, size=n_subseqs)
diablo = []
log_delta_scale = 0.0
frequency = 1E-4
add_scale = np.sqrt(np.exp(2 * frequency) - 1)
mult = np.exp(-frequency)
while len(diablo) < n:
  diablo.extend(subseq_vals[np.random.uniform(size=n_subseqs) > 0.9])
  log_delta_scale += np.random.normal() * add_scale
  log_delta_scale *= mult
  delta_scale = 3 * np.exp(log_delta_scale)
  subseq_vals += np.random.uniform(-delta_scale, delta_scale, size=n_subseqs).astype(int)
diablo = np.array(diablo[:n]).astype(np.float64)
diablo /= 100.0
machine_eps = 1.0E-52
diablo *= np.random.uniform(1 - 2 * machine_eps, 1 + 3 * machine_eps, size=n)
write_f64(diablo, 'diablo_long')
write_f64(diablo[:short_n], 'diablo_short')
