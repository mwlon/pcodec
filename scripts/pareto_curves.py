import polars as pl
import numpy as np
from matplotlib import pyplot as plt
import os
import sys
from dataclasses import dataclass
import seaborn as sns
from matplotlib.ticker import ScalarFormatter

from pyarrow.dataset import dataset


@dataclass
class Codec:
    qualified_name: str
    display_name: str
    default_level: int
    min_level: int = -1
    max_level: int = 100
    color: str | None = None

palette = {
    'Pco': '#347269',
    'Zstd': '#a0a',
    'Parquet Dictionary+Zstd': '#b70',
    'Parquet Delta+Zstd': '#a00',
    'Blosc Shuffle+Zstd': '#63b',
    'SPDP': '#0a0',
    #'Turbo PFor': '#06a',
    'Turbo PFor+Zstd': '#06a',
    #'Snappy': '#aa0',
}

@dataclass
class Dataset:
    full_name: str
    display_name: str
    uncompressed_size: int

row_limit = 5000000
datasets = pl.DataFrame([
    # Dataset('lomax', 'Lomax', 8000000),
    Dataset('air quality', 'Air Quality', 823743 * 76),
    Dataset('taxi', 'Taxi', min(19144903, row_limit) * 120),
    Dataset('r/place', 'r/place', min(160353104, row_limit) * 28),
])

families = pl.DataFrame([
    Codec('pco', 'Pco', 8, min_level=2, color='#347269'),
    Codec('zstd', 'Zstd', 3),
    Codec('parquet', 'Parquet Dictionary+Zstd', -1),
    Codec('parquet:int-encoding=delta', 'Parquet Delta+Zstd', -1),
    Codec('blosc:cname=zstd', 'Blosc Shuffle+Zstd', 9, min_level=2, max_level=9),
    Codec('spdp', 'SPDP', 5),
    #Codec('tpfor', 'Turbo PFor', 0),
    Codec('tpfor:cname=zstd', 'Turbo PFor+Zstd', 3),
    #Codec('snappy', 'Snappy', 0),
])

results = (
    pl.read_csv(sys.argv[1])
    .rename({'codec': 'full_codec'})
    .with_columns(
        pl.col('full_codec').str.replace(r':[^:]*(level=|compression=zstd)-?\d+', '').alias('qualified_codec')
    )
    .join(datasets, left_on='input', right_on='full_name', how='left', suffix='_dataset')
    .join(families, left_on='qualified_codec', right_on='qualified_name', how='left', suffix='_codec')
    .with_columns(
        pl.col('full_codec')
        .str
        .extract(r':(level=|zstd-level=|compression=zstd)(\d+).*', 2)
        .str
        .to_integer()
        .fill_null(pl.col('default_level'))
        .alias('level'),
        pl.col('display_name').alias('dataset'),
        pl.col('display_name_codec').alias('codec'),
        (pl.col('uncompressed_size') / pl.col("compressed_size")).alias("compression ratio"),
        (pl.col('uncompressed_size') / pl.col("compress_dt") / 2**20).alias("compression speed / MiB/s"),
        (pl.col('uncompressed_size') / pl.col("decompress_dt") / 2**20).alias("decompression speed / MiB/s"),
        )
    .filter(pl.col('level').is_between(pl.col('min_level'), pl.col('max_level')))
    .sort(by=['input', 'codec', 'level'])
)
# inputs = sorted(results['input'].unique())
# fig, axes = plt.subplots(len(inputs), 2, squeeze=False, figsize=(10, 4), sharey="row")

# seaborn styles
sns.set_style(style="whitegrid")
sns.relplot(
    results,
    x="compression speed / MiB/s",
    y="compression ratio",
    row="dataset",
    hue='codec',
    style='codec',
    palette=palette,
    kind='line',
    markers=True,
    dashes=False,
    facet_kws=dict(
        sharey=False,
        sharex=True,
    ),
)
# plt.xscale('log')
plt.yscale('log')
plt.xlim(0, 400)
ax = plt.gca()

# Disable scientific notation on both axes
ax.xaxis.set_major_formatter(ScalarFormatter())
ax.yaxis.set_major_formatter(ScalarFormatter())
# for (input_,), input_results in sorted(
#         results.group_by('input'), key=lambda kv: kv[0]
# ):
#     input_idx = inputs.index(input_)
#     dataset_display_name = input_results[0]['display_name']
#
#     family_axes = axes[input_idx]
#     if input_idx == len(inputs) - 1:
#         family_axes[0].set_xlabel("compression speed / (MiB/s)")
#         family_axes[1].set_xlabel("decompression speed / (MiB/s)")
#     family_axes[0].set_ylabel(f"{dataset_display_name}\ncompression ratio")
#
#     for ax in family_axes:
#         ax.set_xscale('log')
#         ax.set_yscale('log')
#
#     for ax_idx, x in [(0, "compression_speed"), (1, "decompression_speed")]:
#         sns.relplot(input_results, x=x, y="compression_ratio", label=family_display_name, ax=family_axes[ax_idx])
#
# axes[0, 0].legend()
plt.savefig('pareto_curves.png')
plt.show()
