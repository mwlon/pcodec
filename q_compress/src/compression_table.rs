use crate::data_types::{NumberLike, UnsignedLike};
use crate::prefix::PrefixCompressionInfo;
use crate::errors::{QCompressResult, QCompressError};
use crate::Prefix;

const TARGET_BRANCHING_FACTOR: usize = 16; // chosen for performance

#[derive(Debug, Clone)]
pub struct CompressionTableItem<U: UnsignedLike> {
  pub upper: U,
  pub table: CompressionTable<U>
}

#[derive(Debug, Clone)]
pub enum CompressionTable<U: UnsignedLike> {
  Leaf(PrefixCompressionInfo<U>),
  NonLeaf(Vec<CompressionTableItem<U>>),
}

impl<T: NumberLike> From<&[Prefix<T>]> for CompressionTable<T::Unsigned> {
  fn from(prefixes: &[Prefix<T>]) -> Self {
    let mut infos = prefixes.iter()
      .map(PrefixCompressionInfo::from)
      .collect::<Vec<_>>();
    infos.sort_unstable_by_key(|p| p.upper);
    CompressionTable::from_sorted(&infos)
  }
}

impl<U: UnsignedLike> CompressionTable<U> {
  fn from_sorted(prefixes: &[PrefixCompressionInfo<U>]) -> Self {
    if prefixes.is_empty() {
      return CompressionTable::Leaf(PrefixCompressionInfo::default());
    } else if prefixes.len() == 1 {
      return CompressionTable::Leaf(prefixes[0]);
    }

    let total_count: usize = prefixes.iter()
      .map(|p| p.count)
      .sum();

    let mut last_idx = 0;
    let mut idx = 0;
    let mut cumulative = 0;
    let mut children = Vec::new();
    for i in 0..TARGET_BRANCHING_FACTOR {
      let target = (total_count * (i + 1)) / TARGET_BRANCHING_FACTOR;
      while cumulative < target {
        let incr = prefixes[idx].count;
        if incr < 2 * target - cumulative {
          cumulative += prefixes[idx].count;
          idx += 1;
        } else {
          break;
        }
      }

      if idx > last_idx {
        children.push(CompressionTableItem {
          table: CompressionTable::from_sorted(&prefixes[last_idx..idx]),
          upper: prefixes[idx - 1].upper,
        });
        last_idx = idx;
      }
    }
    CompressionTable::NonLeaf(children)
  }

  pub fn search(&self, unsigned: U) -> QCompressResult<&PrefixCompressionInfo<U>> {
    let mut node = self;
    loop {
      match node {
        CompressionTable::Leaf(p) => {
          return if p.contains(unsigned) {
            Ok(p)
          } else {
            Err(QCompressError::invalid_argument(format!(
              "chunk compressor was not trained to include number with unsigned value {}",
              unsigned,
            )))
          };
        }
        CompressionTable::NonLeaf(linear_scan) => {
          let mut found = false;
          for item in linear_scan {
            if unsigned <= item.upper {
              node = &item.table;
              found = true;
              break;
            }
          }

          if !found {
            return Err(QCompressError::invalid_argument(format!(
              "chunk compressor was not trained to include number with unsigned value {}",
              unsigned,
            )));
          }
        },
      }
    }
  }
}


