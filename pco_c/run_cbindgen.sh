#!/bin/bash
cd "${0%/*}"
cbindgen --config cbindgen.toml --crate cpcodec --output include/cpcodec.h
echo Warning, you must fix the constants in the header manually as cbindgen does not properly evalutate constants.