#!/bin/bash
set -eoux pipefail

# meant to run on something like rocky/redhat/aws linux/centos

#sudo subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms
#sudo dnf install "https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm"
sudo dnf update && sudo dnf install git htop vim gcc perf clang parallel
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
wget "https://pcodec-public.s3.amazonaws.com/reddit_2022_place_numerical.parquet"
wget "https://pcodec-public.s3.amazonaws.com/fhvhv_tripdata_2023-04.parquet"
wget "https://pcodec-public.s3.amazonaws.com/devinrsmith-air-quality.20220714.zstd.parquet"
wget "https://pcodec-public.s3.amazonaws.com/u64_lomax.bin"
popd

cargo build --release --features full_bench -p pco_cli
taskset -c 14 ./target/release/pcodec bench --iters 1 -i data/contrib/devinrsmith-air-quality.20220714.zstd.parquet