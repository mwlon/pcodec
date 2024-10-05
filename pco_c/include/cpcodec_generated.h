typedef enum PcoError {
  PcoSuccess,
  PcoInvalidType,
  PcoCompressionError,
  PcoDecompressionError,
} PcoError;

typedef struct PcoFfiVec {
  const void *ptr;
  size_t len;
  const void *raw_box;
} PcoFfiVec;

enum PcoError pco_simpler_compress(const void *nums,
                                   size_t len,
                                   unsigned char dtype,
                                   unsigned int level,
                                   struct PcoFfiVec *dst);

enum PcoError pco_simple_decompress(const void *compressed,
                                    size_t len,
                                    unsigned char dtype,
                                    struct PcoFfiVec *dst);

enum PcoError pco_free_pcovec(struct PcoFfiVec *ffi_vec);
