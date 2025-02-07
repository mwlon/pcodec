import multiprocessing
import sys

datasets = [
    ('data/contrib/reddit_2022_place_numerical.parquet', 'r/place', 3),
    ('data/contrib/fhvhv_tripdata_2023-04.parquet', 'taxi', 3),
    ('data/contrib/devinrsmith-air-quality.20220714.zstd.parquet', 'air quality', 9),
    ('data/contrib/cms_open_payments.parquet', 'payments', 5),
    ('data/contrib/california_housing.parquet', 'housing', 39),
    ('data/contrib/twitter.csv', 'twitter', 5),
    ('data/contrib/u64_lomax.bin', 'lomax', 9),
]
multithread = sys.argv[1].lower() in ['t', 'true', 'y']
nproc = 48 # multiprocessing.cpu_count()

codecs = []
for level in range(2, 13):
    codecs.append(f'pco:level={level}')
for level in range(1, 12):
    codecs.append(f'parquet:compression=zstd{level}')
    codecs.append(f'parquet:compression=zstd{level}:int-encoding=delta')
    codecs.append(f'zstd:level={level}')
    codecs.append(f'tpfor:cname=zstd:level={level}')
for level in range(2, 10):
    codecs.append(f'blosc:clevel={level}:cname=zstd')
for level in range(10):
    codecs.append(f'spdp:level={level}')

for path, dataset, iters in datasets:
    for codec in codecs:
        for step_skipped in ['compress', 'decompress']:
            if multithread:
                results_file = 'results_multi.csv'
            else:
                results_file = 'results.csv'
            args = [
                f'echo {dataset} {codec} &&',
                f'if ! grep -q "{dataset},{codec},[0-9]*\.[0-9]*,[0-9]*\.[0-9]*," {results_file}; then',
                f'./target/release/pcodec bench',
                f'-i {path}',
                f'--input-name "{dataset}"',
                f'-c {codec}',
                f'--results-csv {results_file}',
                '--limit 2000000',
                f'--no-{step_skipped}',
            ]
            if multithread:
                args += [
                    '--iters 1',
                    f'--threads {nproc}',
                ]
            else:
                args += [
                    f'--iters {iters}',
                ]
            args += ['; fi']
            print(' '.join(args))