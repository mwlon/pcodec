use crate::bin::BinCompressionInfo;
use crate::constants::Weight;
use crate::data_types::UnsignedLike;
use crate::errors::{PcoError, PcoResult};

// this isn't really the same type of quantity as weight,
// but it needs to have the same data type
const TARGET_BRANCHING_FACTOR: Weight = 16; // chosen for performance

#[derive(Debug, Clone)]
pub struct CompressionTableItem<U: UnsignedLike> {
  pub upper: U,
  pub table: CompressionTable<U>,
}

#[derive(Debug, Clone)]
pub enum CompressionTable<U: UnsignedLike> {
  Leaf(BinCompressionInfo<U>),
  NonLeaf(Vec<CompressionTableItem<U>>),
}

impl<U: UnsignedLike> From<Vec<BinCompressionInfo<U>>> for CompressionTable<U> {
  fn from(mut infos: Vec<BinCompressionInfo<U>>) -> Self {
    infos.sort_unstable_by_key(|info| info.upper);
    CompressionTable::from_sorted(&infos)
  }
}

impl<U: UnsignedLike> CompressionTable<U> {
  fn from_sorted(bins: &[BinCompressionInfo<U>]) -> Self {
    if bins.is_empty() {
      return CompressionTable::Leaf(BinCompressionInfo::default());
    } else if bins.len() == 1 {
      return CompressionTable::Leaf(bins[0]);
    }

    let total_count: Weight = bins.iter().map(|p| p.weight).sum();

    let mut last_idx = 0;
    let mut idx = 0;
    let mut cumulative = 0;
    let mut children = Vec::new();
    for i in 0..TARGET_BRANCHING_FACTOR {
      let target = (total_count * (i + 1)) / TARGET_BRANCHING_FACTOR;
      while cumulative < target {
        let incr = bins[idx].weight;
        if incr < 2 * target - cumulative {
          cumulative += bins[idx].weight;
          idx += 1;
        } else {
          break;
        }
      }

      if idx > last_idx {
        children.push(CompressionTableItem {
          table: CompressionTable::from_sorted(&bins[last_idx..idx]),
          upper: bins[idx - 1].upper,
        });
        last_idx = idx;
      }
    }
    CompressionTable::NonLeaf(children)
  }

  pub fn search(&self, unsigned: U) -> PcoResult<&BinCompressionInfo<U>> {
    let mut node = self;
    loop {
      match node {
        CompressionTable::Leaf(info) => {
          return if info.contains(unsigned) {
            Ok(info)
          } else {
            Err(PcoError::invalid_argument(format!(
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
            return Err(PcoError::invalid_argument(format!(
              "chunk compressor was not trained to include number with unsigned value {}",
              unsigned,
            )));
          }
        }
      }
    }
  }
}
