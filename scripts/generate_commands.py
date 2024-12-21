datasets = [
    #('data/contrib/reddit_2022_place_numerical.parquet', 'r/place', 5),
    #('data/contrib/fhvhv_tripdata_2023-04.parquet', 'taxi', 5),
    ('data/contrib/devinrsmith-air-quality.20220714.zstd.parquet', 'air quality', 31),
    #('data/contrib/u64_lomax.parquet', 'lomax', 101),
]

codecs = []
for level in range(13):
    codecs.append(f'pco:level={level}')
for level in range(1, 14):
    codecs.append(f'parquet:compression=zstd{level}')
    codecs.append(f'parquet:int-encoding=delta:compression=zstd{level}')
    codecs.append(f'zstd:level={level}')
    codecs.append(f'tpfor:zstd-level={level}')
for level in range(0, 10):
    codecs.append(f'blosc:cname=zstd:clevel={level}')
for level in range(10):
    codecs.append(f'spdp:level={level}')
codecs.append('snappy')
codecs.append('tpfor')

for path, dataset, iters in datasets:
    for codec in codecs:
        print(
            f'./target/release/pcodec bench '
            f'-i {path} '
            f'--input-name "{dataset}" '
            f'--iters {iters} '
            f'-c {codec} '
            f'--results-csv results.csv '
        )