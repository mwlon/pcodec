import polars as pl
import numpy as np
from matplotlib import pyplot as plt
import os
import sys
from dataclasses import dataclass
import seaborn as sns

from pyarrow.dataset import dataset


@dataclass
class Family:
    qualified_name: str
    display_name: str
    default_level: int
    min_level: int = -1
    color: str | None = None

@dataclass
class Dataset:
    full_name: str
    display_name: str
    uncompressed_size: int

datasets = pl.DataFrame([
    Dataset('lomax', 'Lomax', 8000000),
    Dataset('air quality', 'Air Quality', 823743 * 76),
    Dataset('taxi', 'Taxi', 19144903 * 120),
    Dataset('r/place', 'r/place', 160353104 * 28),
])

families = pl.DataFrame([
    Family('pco', 'Pco', 8, min_level=2, color='#347269'),
    Family('zstd', 'Zstd', 3),
    Family('parquet', 'Parquet Dictionary+Zstd', -1),
    Family('parquet:int-encoding=delta', 'Parquet Delta+Zstd', -1),
    Family('blosc:cname=zstd', 'Blosc Shuffle+Zstd', 9, min_level=1),
    Family('spdp', 'SPDP', 5),
    Family('tpfor', 'Turbo PFor', 0),
    Family('tpfor:cname=zstd', 'Turbo PFor+Zstd', 3),
    Family('snappy', 'Snappy', 0),
])

results = (
    pl.read_csv(sys.argv[1])
    .with_columns(
        pl.col('codec').str.replace(r':[^:]*(level|compression)=-?\d+', '').alias('codec_family')
    )
    .join(datasets, left_on='input', right_on='full_name', how='left', suffix='_dataset')
    .join(families, left_on='codec_family', right_on='qualified_name', how='left', suffix='_family')
    .with_columns(
        pl.col('codec')
        .str
        .extract(r':(level=|zstd-level=|compression=zstd)(\d+).*', 2)
        .str
        .to_integer()
        .fill_null(pl.col('default_level'))
        .alias('level'),
        (pl.col('uncompressed_size') / pl.col("compressed_size")).alias("compression_ratio"),
        (pl.col('uncompressed_size') / pl.col("compress_dt") / 2**20).alias("compression_speed"),
        (pl.col('uncompressed_size') / pl.col("decompress_dt") / 2**20).alias("decompression_speed"),
        )
    .filter(pl.col('level') >= pl.col('min_level'))
    .sort(by=['input', 'codec_family', 'level'])
)
# inputs = sorted(results['input'].unique())
# fig, axes = plt.subplots(len(inputs), 2, squeeze=False, figsize=(10, 4), sharey="row")

sns.relplot(
    results,
    x="compression_speed",
    y="compression_ratio",
    style="display_name_family",
    hue='display_name_family',
    row="display_name",
    kind='line',
    markers=True,
    dashes=False,
)
plt.xscale('log')
plt.yscale('log')
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