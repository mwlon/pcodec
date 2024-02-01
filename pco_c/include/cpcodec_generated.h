typedef enum PcoError {
  Success,
  InvalidType,
  DecompressionError,
} PcoError;

typedef struct PcoFfiVec {
  const void *ptr;
  unsigned int len;
  const void *raw_box;
} PcoFfiVec;

enum PcoError auto_compress(const void *nums,
                            unsigned int len,
                            unsigned char dtype,
                            unsigned int level,
                            struct PcoFfiVec *dst);

enum PcoError auto_decompress(const void *compressed,
                              unsigned int len,
                              unsigned char dtype,
                              struct PcoFfiVec *dst);

enum PcoError free_pcovec(struct PcoFfiVec *ffi_vec);
