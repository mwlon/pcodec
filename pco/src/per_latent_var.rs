use std::fmt::Debug;
use std::iter::Sum;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LatentVarKey {
  Delta,
  Primary,
  Secondary,
}

impl LatentVarKey {
  pub const ALL: [LatentVarKey; 3] = [
    LatentVarKey::Delta,
    LatentVarKey::Primary,
    LatentVarKey::Secondary,
  ];
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PerLatentVar<T> {
  pub delta: Option<T>,
  pub primary: T,
  pub secondary: Option<T>,
}

#[derive(Clone, Debug)]
pub(crate) struct PerLatentVarBuilder<T> {
  pub delta: Option<T>,
  pub primary: Option<T>,
  pub secondary: Option<T>,
}

impl<T> Default for PerLatentVarBuilder<T> {
  fn default() -> Self {
    Self {
      delta: None,
      primary: None,
      secondary: None,
    }
  }
}

impl<T> PerLatentVarBuilder<T> {
  pub fn set(&mut self, key: LatentVarKey, value: T) {
    match key {
      LatentVarKey::Delta => self.delta = Some(value),
      LatentVarKey::Primary => self.primary = Some(value),
      LatentVarKey::Secondary => self.secondary = Some(value),
    }
  }
}

impl<T> From<PerLatentVarBuilder<T>> for PerLatentVar<T> {
  fn from(value: PerLatentVarBuilder<T>) -> Self {
    PerLatentVar {
      delta: value.delta,
      primary: value.primary.unwrap(),
      secondary: value.secondary,
    }
  }
}

impl<T> PerLatentVar<T> {
  pub(crate) fn map<S, F: Fn(LatentVarKey, T) -> S>(self, f: F) -> PerLatentVar<S> {
    PerLatentVar {
      delta: self.delta.map(|delta| f(LatentVarKey::Delta, delta)),
      primary: f(LatentVarKey::Primary, self.primary),
      secondary: self
        .secondary
        .map(|secondary| f(LatentVarKey::Secondary, secondary)),
    }
  }

  pub(crate) fn as_ref(&self) -> PerLatentVar<&T> {
    PerLatentVar {
      delta: self.delta.as_ref(),
      primary: &self.primary,
      secondary: self.secondary.as_ref(),
    }
  }

  pub(crate) fn as_mut(&mut self) -> PerLatentVar<&mut T> {
    PerLatentVar {
      delta: self.delta.as_mut(),
      primary: &mut self.primary,
      secondary: self.secondary.as_mut(),
    }
  }

  pub(crate) fn get(&self, key: LatentVarKey) -> Option<&T> {
    match key {
      LatentVarKey::Delta => self.delta.as_ref(),
      LatentVarKey::Primary => Some(&self.primary),
      LatentVarKey::Secondary => self.secondary.as_ref(),
    }
  }

  pub(crate) fn zip_exact<S>(self, other: PerLatentVar<S>) -> PerLatentVar<(T, S)> {
    let zip_option = |a: Option<T>, b: Option<S>| match (a, b) {
      (Some(a), Some(b)) => Some((a, b)),
      (None, None) => None,
      _ => panic!("expected values of left and right sides to match"),
    };

    PerLatentVar {
      delta: zip_option(self.delta, other.delta),
      primary: (self.primary, other.primary),
      secondary: zip_option(self.secondary, other.secondary),
    }
  }

  pub(crate) fn enumerated(self) -> Vec<(LatentVarKey, T)> {
    let mut res = Vec::with_capacity(3);
    if let Some(value) = self.delta {
      res.push((LatentVarKey::Delta, value));
    }
    res.push((LatentVarKey::Primary, self.primary));
    if let Some(value) = self.secondary {
      res.push((LatentVarKey::Secondary, value));
    }
    res
  }

  pub(crate) fn sum(self) -> T
  where
    T: Sum,
  {
    let mut values = Vec::with_capacity(3);
    if let Some(value) = self.delta {
      values.push(value);
    }
    values.push(self.primary);
    if let Some(value) = self.secondary {
      values.push(value);
    }
    T::sum(values.into_iter())
  }
  // pub(crate) fn enumerated_mut(&mut self) -> Vec<(LatentVarKey, &mut T)> {
  //   let mut res = Vec::with_capacity(3);
  //   if let Some(value) = &mut self.delta {
  //     res.push((LatentVarKey::Delta, value));
  //   }
  //   res.push((LatentVarKey::Primary, &mut self.primary));
  //   if let Some(value) = &mut self.secondary {
  //     res.push((LatentVarKey::Secondary, value));
  //   }
  //   res
  // }
}

impl<T> From<PerLatentVar<T>> for Vec<T> {
  fn from(value: PerLatentVar<T>) -> Self {
    let mut res = Vec::with_capacity(3);
    res.extend(value.delta);
    res.push(value.primary);
    res.extend(value.secondary);
    res
  }
}
