#include "../include/cpcodec.h"
#include <stdio.h>

int main() {
  float input[] = {1.1f, 2.2f, 3.3f, 4.4f};
  int num_elems = sizeof(input) / sizeof(input[0]);

  struct PcoVec cvec;
  enum PcoError res = auto_compress(&input, num_elems, PCO_TYPE_F32, 8, &cvec);
  if (res != Success) {
    printf("Error compressing: %d\n", res);
    return -1;
  }
  printf("Compressed %d floats to %d bytes\n", num_elems, cvec.len);

  struct PcoVec dvec;
  res = auto_decompress(cvec.ptr, cvec.len, PCO_TYPE_F32, &dvec);
  if (res != Success) {
    printf("Error decompressing: %d\n", res);
    free_pcovec(&cvec);
    return -1;
  }
  printf("Decompressed %d floats\n", dvec.len);
  if (dvec.len != num_elems) {
    printf("Sizes do not match!!!\n");
    goto cleanup;
  }

  for (int i = 0; i < num_elems; i++) {
    if (input[i] != ((float *)dvec.ptr)[i]) {
      printf("Values do not match!!!\n");
      goto cleanup;
    }
  }
  printf("Values match\n");

cleanup:
  free_pcovec(&cvec);
  free_pcovec(&dvec);
  return 0;
}