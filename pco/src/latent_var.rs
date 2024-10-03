use std::fmt::Debug;
use std::iter;

pub enum LatentVar {
  Delta,
  Primary,
  Secondary,
}

pub struct LatentVarMap<V> {
  pub delta: Option<V>,
  pub primary: V,
  pub secondary: Option<V>,
}

impl<V> LatentVarMap<V> {
  pub fn to_vec(&self) -> Vec<(LatentVar, &V)> {
    let mut res = Vec::with_capacity(3);
    if let Some(delta) = &self.delta {
      res.push((LatentVar::Delta, delta));
    }
    res.push((LatentVar::Primary, &self.primary));
    if let Some(secondary) = &self.secondary {
      res.push((LatentVar::Secondary, secondary));
    }
    res
  }

  pub fn iter_values(&self) -> impl Iterator<Item = V> {
    self
      .delta
      .iter()
      .chain(iter::once(&self.primary))
      .chain(self.secondary.iter())
  }

  pub fn map_values<O, F: Fn(&V) -> O>(&self, f: F) -> Self<O> {
    Self {
      delta: self.delta.as_ref().map(&f),
      primary: f(&self.primary),
      secondary: self.secondary.as_ref().map(f),
    }
  }
}
