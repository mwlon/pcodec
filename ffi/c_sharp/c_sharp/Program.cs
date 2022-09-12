using System.Runtime.InteropServices;

namespace QCompress {
    class Native
    {
        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern CVec auto_compress_i32(
            int* nums,
            uint len,
            uint level
        );

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern void move_compressed_into_buffer(
            CVec c_vec,
            byte* buffer
        );

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern CVec auto_decompress_i32(
            byte* compressed,
            uint len
        );

        [DllImport("q_compress_ffi.dylib")]
        public unsafe static extern void move_i32_into_buffer(
            CVec c_vec,
            int* buffer
        );

        public struct CVec
        {
            public UIntPtr vec;
            public uint len;
        }
    }

    class Example
    {
        unsafe static byte[] auto_compress_i32(
            int[] nums,
            uint level)
        {
            Native.CVec c_vec;
            fixed (int* nums_ptr = nums)
            {
                c_vec = Native.auto_compress_i32(nums_ptr, ((uint)nums.Length), level);
            };
            byte[] buffer = new byte[c_vec.len];
            fixed (byte* buffer_ptr = buffer)
            {
                Native.move_compressed_into_buffer(c_vec, buffer_ptr);
            }
            return buffer;
        }

        unsafe static int[] auto_decompress_i32(
            byte[] compressed)
        {
            Native.CVec c_vec;
            fixed (byte* compressed_ptr = compressed)
            {
                c_vec = Native.auto_decompress_i32(compressed_ptr, ((uint)compressed.Length));
            };
            int[] buffer = new int[c_vec.len];
            fixed (int* buffer_ptr = buffer)
            {
                Native.move_i32_into_buffer(c_vec, buffer_ptr);
            }
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