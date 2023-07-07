using System.Runtime.InteropServices;

namespace QCompress {
    class Native
    {
        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern FfiVec auto_compress_i32(
            int* nums,
            uint len,
            uint level
        );

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern void free_compressed(FfiVec ffi_vec);

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern FfiVec auto_decompress_i32(
            byte* compressed,
            uint len
        );

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern void free_i32(FfiVec c_vec);

        public struct FfiVec
        {
            private UIntPtr raw_box;
            public UIntPtr ptr;
            public uint len;
        }
    }

    class Example
    {
        unsafe static byte[] auto_compress_i32(
            int[] nums,
            uint level)
        {
            Native.FfiVec ffi_vec;
            fixed (int* nums_ptr = nums)
            {
                ffi_vec = Native.auto_compress_i32(nums_ptr, ((uint)nums.Length), level);
            };
            byte[] buffer = new byte[ffi_vec.len];
            for (int i = 0; i < ffi_vec.len; i++)
            {
                buffer[i] = (*(byte*) (ffi_vec.ptr + sizeof(byte) * i));
            }
            Native.free_compressed(ffi_vec);
            return buffer;
        }

        unsafe static int[] auto_decompress_i32(
            byte[] compressed)
        {
            Native.FfiVec ffi_vec;
            fixed (byte* compressed_ptr = compressed)
            {
                ffi_vec = Native.auto_decompress_i32(compressed_ptr, ((uint)compressed.Length));
            };
            int[] buffer = new int[ffi_vec.len];
            for (int i = 0; i < ffi_vec.len; i++)
            {
                buffer[i] = (*(int*)(ffi_vec.ptr + sizeof(int) * i));
            }
            Native.free_i32(ffi_vec);
            return buffer;
        }

        static void Main(String[] args)
        {
            int[] nums = { 1, 2, 3 };
            byte[] compressed = auto_compress_i32(nums, 6);
            Console.WriteLine($"compressed to {compressed.Length} bytes");
            int[] decompressed = auto_decompress_i32(compressed);
            for (int i = 0; i < decompressed.Length; i++)
            {
                Console.WriteLine(decompressed[i]);
            }
        }
    }
}