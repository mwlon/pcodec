# python 3
# The only pip requirement: numpy

import numpy as np
import os

os.makedirs('data/txt', exist_ok=True)
os.makedirs('data/binary', exist_ok=True)

def write_i64(arr, name):
  if arr.dtype != np.int64:
    floored = np.floor(arr).astype(np.int64)
  else:
    floored = arr
  ints = [str(x) for x in floored]
  joined = '\n'.join(ints)
  with open(f'data/txt/i64_{name}.txt', 'w') as f:
    f.write(joined)
  with open(f'data/binary/i64_{name}.bin', 'wb') as f:
    f.write(floored.tobytes())

def write_f64(arr, name):
  arr = arr.astype(np.float64)
  floats = [str(x) for x in arr]
  joined = '\n'.join(floats)
  with open(f'data/txt/f64_{name}.txt', 'w') as f:
    f.write(joined)
  with open(f'data/binary/f64_{name}.bin', 'wb') as f:
    f.write(arr.tobytes())

n = 100000
write_i64(np.random.normal(scale=1.0, size=n), 'normal1')
write_i64(np.random.normal(scale=10.0, size=n), 'normal10')
write_i64(np.random.normal(scale=1000000.0, size=n), 'normal1M')

write_i64(np.random.geometric(p=0.5, size=n), 'geo2')
write_i64(np.random.geometric(p=0.000001, size=n), 'geo1M')

def fixed_median_lomax(a):
  unscaled_median = 2 ** (1 / a) - 1
  return np.random.pareto(a=a, size=n) / unscaled_median * 1000
write_i64(fixed_median_lomax(0.5), 'lomax05')
write_i64(fixed_median_lomax(1.5), 'lomax15')
write_i64(fixed_median_lomax(2.5), 'lomax25')

write_i64(np.random.randint(-2**63, 2**63, size=n), 'uniform')

write_i64(np.repeat(77777, n), 'constant')

write_i64(np.where(np.random.uniform(size=n) < 0.5, -2**63, 2**63 - 1), 'extremes')

write_i64(np.random.binomial(1, p=0.01, size=n), 'sparse')

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
