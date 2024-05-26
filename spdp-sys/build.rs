fn main() {
  cc::Build::new()
    .files(&["src/spdp_11.c"])
    .include("include")
    .compile("spdp");
}
