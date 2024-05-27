#include <stddef.h>

typedef unsigned char byte_t;
size_t spdp_compress_batch(
  const byte_t level,
  const size_t length,
  byte_t* const buf1,
  byte_t* const buf2
);
size_t spdp_decompress_batch(
  const byte_t level,
  const size_t length,
  byte_t* const buf2,
  byte_t* const buf1
);
