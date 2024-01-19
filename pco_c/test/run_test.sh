#!/bin/bash
set -e

cd "${0%/*}"
gcc -g test_cpcodec.c -o test_cpcodec -L../../target/debug -lcpcodec -Wl,-R../../target/debug
./test_cpcodec
rm test_cpcodec
