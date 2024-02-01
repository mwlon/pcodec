#!/bin/bash
set -e

cd "${0%/*}"
cbindgen --config cbindgen.toml --crate cpcodec --output include/cpcodec_generated.h