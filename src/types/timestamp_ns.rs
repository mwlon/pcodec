use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::errors::QCompressError;
use crate::types::NumberLike;

const BILLION_U32: u32 = 1_000_000_000;
const BILLION_I128: i128 = 1_000_000_000;
// we choose these bounds to match the convention of using i64 for seconds
// and u32 for sub-second nanos
// but we use i128 in memory for efficiency
const MAX_NANOS: i128 = BILLION_I128 * (i64::MAX as i128 + 1) - 1;
const MIN_NANOS: i128 = BILLION_I128 * (i64::MIN as i128);

// an instant - does not store time zone
// always relative to Unix Epoch
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TimestampNs(i128);

impl TimestampNs {
  pub fn new(nanos: i128) -> Result<Self, QCompressError> {
    if nanos > MAX_NANOS || nanos < MIN_NANOS {
      Err(QCompressError::InvalidTimestampError { nanos })
    } else {
      Ok(TimestampNs(nanos))
    }
  }

  pub fn from_secs_and_nanos(seconds: i64, subsec_nanos: u32) -> Self {
    TimestampNs((seconds as i128) * BILLION_I128 + subsec_nanos as i128)
  }

  pub fn to_secs_and_nanos(self) -> (i64, u32) {
    let nanos = self.0;
    let seconds = nanos.div_euclid(BILLION_I128) as i64;
    let subsec_nanos = nanos.rem_euclid(BILLION_I128) as u32;
    (seconds, subsec_nanos)
  }

  pub fn to_total_nanos(self) -> i128 {
    self.0
  }
}

impl From<SystemTime> for TimestampNs {
  fn from(system_time: SystemTime) -> TimestampNs {
    let (seconds, subsec_nanos) = if system_time.lt(&UNIX_EPOCH) {
      let dur = UNIX_EPOCH.duration_since(system_time)
        .expect("time difference error (pre-epoch)");
      (dur.as_secs() as i64, dur.subsec_nanos())
    } else {
      let dur = system_time.duration_since(UNIX_EPOCH)
        .expect("time difference error");
      let complement_nanos = dur.subsec_nanos();
      let ceil_secs = -(dur.as_secs() as i64);
      if complement_nanos == 0 {
        (ceil_secs, 0)
      } else {
        (ceil_secs - 1, BILLION_U32 - complement_nanos)
      }
    };

    TimestampNs::from_secs_and_nanos(seconds, subsec_nanos)
  }
}

impl From<TimestampNs> for SystemTime {
  fn from(value: TimestampNs) -> SystemTime {
    let (seconds, subsec_nanos) = value.to_secs_and_nanos();
    if seconds >= 0 {
      let dur = Duration::new(seconds as u64, subsec_nanos);
      UNIX_EPOCH + dur
    } else {
      let dur = if subsec_nanos == 0 {
        Duration::new((-seconds) as u64, 0)
      } else {
        Duration::new((-seconds - 1) as u64, BILLION_U32 - subsec_nanos)
      };
      UNIX_EPOCH - dur
    }
  }
}

impl Display for TimestampNs {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "TimestampNs({})",
      self.0,
    )
  }
}

impl NumberLike for TimestampNs {
  const HEADER_BYTE: u8 = 8;
  const PHYSICAL_BITS: usize = 96;

  type Diff = u128;

  fn num_eq(&self, other: &Self) -> bool {
    self.0.eq(&other.0)
  }

  fn num_cmp(&self, other: &Self) -> Ordering {
    self.0.cmp(&other.0)
  }

  fn offset_diff(upper: TimestampNs, lower: TimestampNs) -> u128 {
    (upper.0 - lower.0) as u128
  }

  fn add_offset(lower: TimestampNs, off: u128) -> TimestampNs {
    TimestampNs(lower.0 + off as i128)
  }

  fn bytes_from(value: TimestampNs) -> Vec<u8> {
    ((value.0 - MIN_NANOS) as u128).to_be_bytes()[4..].to_vec()
  }

  fn from_bytes(bytes: Vec<u8>) -> TimestampNs {
    TimestampNs::from_bytes_safe(&bytes).expect("corrupt timestamp bytes")
  }
}

impl TimestampNs {
  pub fn from_bytes_safe(bytes: &[u8]) -> Result<TimestampNs, QCompressError> {
    let mut full_bytes = vec![0;4];
    full_bytes.extend(bytes);
    let nanos = (u128::from_be_bytes(full_bytes.try_into().unwrap()) as i128) + MIN_NANOS;
    TimestampNs::new(nanos)
  }
}

pub type TimestampNsCompressor = Compressor<TimestampNs>;
pub type TimestampNsDecompressor = Decompressor<TimestampNs>;
