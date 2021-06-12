import numpy as np
import csv
import matplotlib as mpl
mpl.use('MacOSX')
from matplotlib import pyplot as plt

series = {}
for key in ['dollars', 'cents', 'total_cents']:
  series[key] = []

def write(arr, name):
  if arr.dtype != np.int64:
    floored = np.floor(arr).astype(np.int64)
  else:
    floored = arr
  ints = [str(x) for x in floored]
  joined = '\n'.join(ints)
  with open(f'data/txt/{name}.txt', 'w') as f:
    f.write(joined)
  with open(f'data/binary/{name}.bin', 'wb') as f:
    f.write(floored.tobytes())

header = True
for line in csv.reader(open('/Users/martin/Downloads/nypl_menu_data_2021_06_01/MenuItem.csv')):
  if header:
    header = False
    continue
  price = line[2]
  if price == '':
    continue
  price_items = price.split('.')
  if len(price_items) == 1:
    dollars = int(price_items[0])
    cents = 0
  elif len(price_items) == 2:
    dollars, cents = price_items
    dollars = int(dollars)
    if len(cents) == 2:
      cents = int(cents)
    elif len(cents) == 1:
      cents = 10 * int(cents)
    else:
      print(f'unexpected cents digits in: {price}')
      continue
  else:
    raise Exception(f'unexpected number of price items in: {price}')

  if dollars >= 100:
    print(f'unexpectedly high dollar amount in: {line[0]} {price}')
    continue

  series['dollars'].append(dollars)
  series['cents'].append(cents)
  series['total_cents'].append(100 * dollars + cents)

for k, v in series.items():
  write(np.array(v), k)
  plt.hist(v, bins=50)
  plt.title(k)
  plt.show()