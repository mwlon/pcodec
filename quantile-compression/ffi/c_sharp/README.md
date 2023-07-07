# `q_compress` C# FFI

I know almost nothing about C#, but here's one way to run this example:

1. Compile `q_compress`; `cd` into `q_compress/ffi` and run
`cargo build --release`.
2. In VSCode with this `c_sharp` subfolder open, Add > Existing Files >
`q_compress/target/release/libq_compress_ffi.dylib/dll/whatever`. If necessary,
change the system library extension in `program.cs`.
3. In VSCode, right click that file you just added and set its build property
to always copy.
4. Run the project.

There's probably a better way to automate some of these steps!