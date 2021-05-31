# python 3
# The only pip requirement: numpy

import numpy as np
import os

os.makedirs('data/txt', exist_ok=True)
os.makedirs('data/binary', exist_ok=True)

def write(arr, name):
  floored = np.floor(arr).astype(np.int64)
  ints = [str(x) for x in floored]
  joined = '\n'.join(ints)
  with open(f'data/txt/{name}.txt', 'w') as f:
    f.write(joined)
  with open(f'data/binary/{name}.bin', 'wb') as f:
    f.write(floored.tobytes())

n = 100000
write(np.random.normal(scale=1.0, size=n), 'normal1')
write(np.random.normal(scale=10.0, size=n), 'normal10')
write(np.random.normal(scale=1000000.0, size=n), 'normal1M')

write(np.random.geometric(p=0.5, size=n), 'geo2')
write(np.random.geometric(p=0.000001, size=n), 'geo1M')

def fixed_median_lomax(a):
  unscaled_median = 2 ** (1 / a) - 1
  return np.random.pareto(a=a, size=n) / unscaled_median * 1000
write(fixed_median_lomax(0.5), 'lomax05')
write(fixed_median_lomax(1.5), 'lomax15')
write(fixed_median_lomax(2.5), 'lomax25')

write(np.random.randint(-2**63, 2**63, size=n), 'uniform')
