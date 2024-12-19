#!/bin/bash
set -eoux pipefail

# meant to run on rocky or red hat or centos

sudo subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms
sudo dnf install "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm"
sudo dnf update && sudo d f install git htop vim tuned gcc perf
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.bashrc
python3 -m venv venv
source venv/bin/activate
pip install numpy
git clone https://github.com/mwlon/pcodec.git -b paper-benchmarks
cd pcodec
#python bench/generate_randoms.py

mkdir -p data/contrib
pushd data/contrib
aws s3 cp s3://pcodec-public/reddit_2022_place_numerical.parquet r_place.parquet
aws s3 cp s3://pcodec-public/fhvhv_tripdata_2023-04.parquet taxi.parquet
aws s3 cp s3://pcodec-public/devinrsmith-air-quality.20220714.zstd.parquet air_quality.parquet
aws s3 cp s3://pcodec-public/u64_lomax.bin u64_lomax.bin
popd

cargo build --release --bin bench --features full_bench
taskset -c 14 ./target/release/bench --iters 1
