#include "../include/cpcodec.h"
#include <stdio.h>

int is_empty(struct PcoFfiVec *vec) {
  return vec->len == 0 && vec->ptr == NULL && vec->raw_box == NULL;
}

int main() {
  float input[] = {1.1f, 2.2f, 3.3f, 4.4f};
  int num_elems = sizeof(input) / sizeof(input[0]);
  int retcode = 0;

  struct PcoFfiVec cvec;
  enum PcoError res = auto_compress(&input, num_elems, PCO_TYPE_F32, 8, &cvec);
  if (res != Success) {
    printf("Error compressing: %d\n", res);
    retcode = 1;
    goto cleanup_none;
  }
  printf("Compressed %d floats to %d bytes\n", num_elems, cvec.len);

  struct PcoFfiVec dvec;
  res = auto_decompress(cvec.ptr, cvec.len, PCO_TYPE_F32, &dvec);
  if (res != Success) {
    printf("Error decompressing: %d\n", res);
    free_pcovec(&cvec);
    retcode = 1;
    goto cleanup_cvec;
  }
  printf("Decompressed %d floats\n", dvec.len);
  if (dvec.len != num_elems) {
    printf("Sizes do not match!!!\n");
    retcode = 1;
    goto cleanup_all;
  }

  for (int i = 0; i < num_elems; i++) {
    if (input[i] != ((float *)dvec.ptr)[i]) {
      printf("Values do not match!!!\n");
      retcode = 1;
      goto cleanup_all;
    }
  }
  printf("Values match\n");

cleanup_all:
  free_pcovec(&dvec);
  if (!is_empty(&dvec)) {
    printf("Decompression vector not freed!!!\n");
    retcode = 1;
  }
cleanup_cvec:
  free_pcovec(&cvec);
  if (!is_empty(&cvec)) {
    printf("Compression vector not freed!!!\n");
    retcode = 1;
  }
cleanup_none:
  return retcode;
}