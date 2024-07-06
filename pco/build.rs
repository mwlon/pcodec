#[cfg(all(feature = "recommended-instruction-sets", target_arch = "x86_64"))]
mod x86_recommended_instruction_sets {
  fn add_target_feature(target_feature: &str) {
    println!(
      "cargo:rustc-env=RUSTFLAGS=-C target-feature=+{}",
      target_feature
    );
  }

  fn add() {
    if is_x86_feature_detected!("avx2") {
      add_target_feature("avx2")
    }
    if is_x86_feature_detected!("bmi1") {
      add_target_feature("bmi1")
    }
    if is_x86_feature_detected!("bmi2") {
      add_target_feature("bmi2")
    }
  }
}

fn main() {
  #[cfg(all(feature = "recommended-instruction-sets", target_arch = "x86_64"))]
  x86_recommended_instruction_sets::add();
}
