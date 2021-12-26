use std::fmt;

use crate::prefix::Prefix;
use crate::types::NumberLike;

pub fn display_prefixes<T: NumberLike>(prefixes: &[Prefix<T>], f: &mut fmt::Formatter<'_>) -> fmt::Result {
  let s = prefixes
    .iter()
    .map(|p| p.to_string())
    .collect::<Vec<String>>()
    .join("\n");
  write!(f, "({} prefixes)\n{}", prefixes.len(), s)
}
