datasets = [
#    ('data/contrib/r_place.parquet', 'r/place', 5),
#    ('data/contrib/taxi.parquet', 'taxi', 5),
#    ('data/contrib/air_quality.parquet', 'air quality', 31),
#    ('data/contrib/u64_lomax.parquet', 'lomax', 101),
    ('data/binary/u64_lomax05_wider.bin', 'lomax', 1),
]

codecs = []
for level in range(13):
    codecs.append(f'pco:level={level}')
for level in range(1, 16):
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

i = 0
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
        i += 1
