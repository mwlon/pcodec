mkdir -p data/snappy
ls data/binary | xargs -I{} sh run_single_snappy.sh {}
mv data/binary/*.sz data/snappy/
