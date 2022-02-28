cd q_compress/examples
for level in 1 9; do
  echo compressing for level $level
  mkdir -p data/gzip_$level
  ls data/binary | xargs -I{} sh run_single_gzip.sh $level {}
done
