import multiprocessing
import sys

datasets = [
    ('data/contrib/reddit_2022_place_numerical.parquet', 'r/place', 5),
    ('data/contrib/fhvhv_tripdata_2023-04.parquet', 'taxi', 5),
    ('data/contrib/devinrsmith-air-quality.20220714.zstd.parquet', 'air quality', 9),
    ('data/contrib/cms_open_payments.parquet', 'payments', 9),
    ('data/contrib/california_housing.parquet', 'housing', 19),
    ('data/contrib/twitter.csv', 'twitter', 9),
]
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

commands_single = open('commands_single.sh', 'w')
commands_multi = open('commands_multi.sh', 'w')
codec_str = ','.join(codecs)
for path, dataset, iters in datasets:
    for step_skipped in ['compress', 'decompress']:
        for command_file, results_file, threads in [
            (commands_single, 'results.csv', 1),
            (commands_multi, 'results_multi.csv', nproc),
        ]:
            args = [
                f'echo {dataset} &&',
                f'./target/release/pcodec bench',
                f'-i {path}',
                f'--input-name "{dataset}"',
                f'-c {codec_str}',
                f'--results-csv {results_file}',
                '--limit 2000000',
                f'--no-{step_skipped}',
                f'--iters {iters}',
            ]
            if threads > 1:
                args += [f'--threads {threads}']
            command_file.write(' '.join(args) + '\n')