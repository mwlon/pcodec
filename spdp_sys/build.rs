use std::env;
use std::path::PathBuf;

fn main() {
  let bindings = bindgen::Builder::default()
    .header("include/spdp_11.h")
    .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
    .generate()
    .expect("Unable to generate bindings");

  // Write the bindings to the $OUT_DIR/bindings.rs file.
  let out_path = PathBuf::from(env::var("OUT_DIR").expect("must specify OUT_DIR"));
  // panic!("writing to {:?}", out_path);
  bindings
    .write_to_file(out_path.join("bindings.rs"))
    .expect("Couldn't write bindings!");

  cc::Build::new()
    .files(&["src/spdp_11.c"])
    .include("include")
    .compile("spdp");
}
