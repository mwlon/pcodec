#include "cpcodec_generated.h"

// cbindgen can only handle literal constants, which isn't helpful when these
// come from public constants in the pco crate, so we just redefine them here.
#define PCO_TYPE_U32 1
#define PCO_TYPE_U64 2
#define PCO_TYPE_I32 3
#define PCO_TYPE_I64 4
#define PCO_TYPE_F32 5
#define PCO_TYPE_F64 6
