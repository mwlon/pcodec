#define PCO_TYPE_U32 1

#define PCO_TYPE_U64 2

#define PCO_TYPE_I32 3

#define PCO_TYPE_I64 4

#define PCO_TYPE_F32 5

#define PCO_TYPE_F64 6

typedef enum PcoError {
  Success,
  InvalidType,
  DecompressionError,
} PcoError;

typedef struct PcoVec {
  const void *ptr;
  unsigned int len;
  const void *raw_box;
} PcoVec;

enum PcoError auto_compress(const void *nums,
                            unsigned int len,
                            unsigned char dtype,
                            unsigned int level,
                            void *pco_vec);

enum PcoError auto_decompress(const void *compressed,
                              unsigned int len,
                              unsigned char dtype,
                              void *pco_vec);

enum PcoError free_pcovec(struct PcoVec *pco_vec);
