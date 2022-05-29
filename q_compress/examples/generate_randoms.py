# python 3
# pip requirement: numpy, pyarrow

import numpy as np
import pyarrow as pa
from pyarrow import parquet as pq
from datetime import datetime
import os

np.random.seed(0)
n = 1000000

base_dir = 'q_compress/examples/data'

os.makedirs(f'{base_dir}/txt', exist_ok=True)
os.makedirs(f'{base_dir}/parquet', exist_ok=True)
os.makedirs(f'{base_dir}/snappy_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/gzip_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/zstd_parquet', exist_ok=True)
os.makedirs(f'{base_dir}/binary', exist_ok=True)

def write_parquet_tables(nums, full_name):
  print(f'writing parquet for {full_name}...')
  table = pa.Table.from_pydict({'nums': nums})
  pq.write_table(table, f'{base_dir}/parquet/{full_name}.parquet', compression='NONE')
  pq.write_table(table, f'{base_dir}/snappy_parquet/{full_name}.snappy.parquet', compression='snappy')
  pq.write_table(table, f'{base_dir}/gzip_parquet/{full_name}.gzip.parquet', compression='gzip', compression_level=9)
  pq.write_table(table, f'{base_dir}/zstd_parquet/{full_name}.zstd.parquet', compression='zstd', compression_level=22)

def write_i64(arr, name):
  if arr.dtype != np.int64:
    floored = np.floor(arr).astype(np.int64)
  else:
    floored = arr
  ints = [str(x) for x in floored]
  joined = '\n'.join(ints)
  full_name = f'i64_{name}'
  with open(f'{base_dir}/txt/{full_name}.txt', 'w') as f:
    f.write(joined)
  with open(f'{base_dir}/binary/{full_name}.bin', 'wb') as f:
    f.write(floored.tobytes())
  write_parquet_tables(floored, full_name)

def write_bool(arr, name):
  if arr.dtype != np.int8:
    floored = np.floor(arr).astype(np.int8)
  else:
    floored = arr
  ints = [str(x) for x in floored]
  joined = '\n'.join(ints)
  full_name = f'bool_{name}'
  with open(f'{base_dir}/txt/{full_name}.txt', 'w') as f:
    f.write(joined)
  with open(f'{base_dir}/binary/{full_name}.bin', 'wb') as f:
    f.write(floored.tobytes())
  write_parquet_tables(floored, full_name)

def write_f64(arr, name):
  arr = arr.astype(np.float64)
  floats = [str(x) for x in arr]
  joined = '\n'.join(floats)
  full_name = f'f64_{name}'
  with open(f'{base_dir}/txt/{full_name}.txt', 'w') as f:
    f.write(joined)
  with open(f'{base_dir}/binary/{full_name}.bin', 'wb') as f:
    f.write(arr.tobytes())
  write_parquet_tables(arr, full_name)

def write_timestamp_micros(arr, name):
  if arr.dtype != np.int64:
    floored = np.floor(arr).astype(np.int64)
  else:
    floored = arr
  ts = [datetime.utcfromtimestamp(x / 10 ** 6) for x in floored]
  strs = [x.strftime('%Y-%m-%dT%H:%M:%S:%fZ') for x in ts]
  joined = '\n'.join(strs)
  full_name = f'micros_{name}'
  with open(f'{base_dir}/txt/{full_name}.txt', 'w') as f:
    f.write(joined)
  with open(f'{base_dir}/binary/{full_name}.bin', 'wb') as f:
    f.write(floored.tobytes())
  write_parquet_tables(ts, full_name)

write_i64(np.random.normal(scale=1.0, size=n), 'normal1')
write_i64(np.random.normal(scale=10.0, size=n), 'normal10')
write_i64(np.random.normal(scale=1000000.0, size=n), 'normal1M')

write_i64(np.random.geometric(p=0.5, size=n), 'geo2')
write_i64(np.random.geometric(p=0.000001, size=n), 'geo1M')

def fixed_median_lomax(a, median):
  unscaled_median = 2 ** (1 / a) - 1
  return np.random.pareto(a=a, size=n) / unscaled_median * median
write_i64(fixed_median_lomax(0.5, 1000), 'lomax05')
write_i64(fixed_median_lomax(1.5, 1000), 'lomax15')
write_i64(fixed_median_lomax(2.5, 1000), 'lomax25')

write_i64(np.random.randint(-2**63, 2**63, size=n), 'uniform')

write_i64(np.repeat(77777, n), 'constant')

write_i64(np.where(np.random.uniform(size=n) < 0.5, -2**63, 2**63 - 1), 'extremes')

write_i64(np.random.binomial(1, p=0.01, size=n), 'sparse')

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

amplitude = 100000
periods = 103 # something prime
period = n / periods
slow_cosine = 100000 * np.cos(np.arange(n) * 2 * np.pi / (period))
write_i64(slow_cosine, 'slow_cosine')
write_f64(slow_cosine, 'slow_cosine')

write_f64(np.random.normal(size=n), 'normal_at_0')
write_f64(np.random.normal(loc=1000.0, size=n), 'normal_at_1000')

edge_case_floats = np.random.normal(size=n)
p = np.random.uniform(size=n)
edge_case_floats[p < 0.5] *= 2.0 ** -1022 # often denormalized values
edge_case_floats[p < 0.4] = np.inf
edge_case_floats[p < 0.3] = np.nan
edge_case_floats[p < 0.2] = -np.nan  # yes, it is different
edge_case_floats[p < 0.1] = np.NINF
write_f64(edge_case_floats, 'edge_cases')

write_bool(np.random.randint(2, size=n), 'random')

# timestamps increasing 1s at a time on average from 2022-01-01T00:00:00 with
# 1s random jitter
nl_timestamps = 10**6 * (1640995200 + np.arange(n) + np.random.normal(size=n))
write_timestamp_micros(nl_timestamps, 'near_linear')

# millisecond timestamps compressed as microseconds
milli_micro_timestamps = 10 ** 3 * (1640995200000  + np.random.randint(0, 10 ** 9, size=n))
write_timestamp_micros(milli_micro_timestamps, 'millis')

# integers compressed as floats
int_floats = np.random.randint(0, 2 ** 30, size=n)
write_f64(int_floats, 'integers')
