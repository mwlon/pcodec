use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::compressor::Compressor;
use crate::decompressor::Decompressor;
use crate::errors::{QCompressError, QCompressResult};
use crate::types::NumberLike;

const BILLION_U32: u32 = 1_000_000_000;

// an instant - does not store time zone
// always relative to Unix Epoch
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TimestampNs(i128);

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TimestampMicros(i128);

macro_rules! impl_timestamp {
  ($t: ty, $parts_per_sec: expr, $header_byte: expr) => {
    impl $t {
      const MAX: i128 = $parts_per_sec as i128 * (i64::MAX as i128 + 1) - 1;
      const MIN: i128 = $parts_per_sec as i128 * (i64::MIN as i128);
      const NS_PER_PART: u32 = 1_000_000_000 / $parts_per_sec;

      pub fn new(parts: i128) -> QCompressResult<Self> {
        if parts > Self::MAX || parts < Self::MIN {
          Err(QCompressError::InvalidTimestampError { parts, parts_per_sec: $parts_per_sec })
        } else {
          Ok(Self(parts))
        }
      }

      pub fn from_secs_and_nanos(seconds: i64, subsec_nanos: u32) -> Self {
        Self(seconds as i128 * $parts_per_sec as i128 + (subsec_nanos / Self::NS_PER_PART) as i128)
      }

      pub fn to_secs_and_nanos(self) -> (i64, u32) {
        let parts = self.0;
        let seconds = parts.div_euclid($parts_per_sec as i128) as i64;
        let subsec_nanos = parts.rem_euclid($parts_per_sec as i128) as u32 * Self::NS_PER_PART;
        (seconds, subsec_nanos)
      }

      pub fn to_total_parts(self) -> i128 {
        self.0
      }

      pub fn from_bytes_safe(bytes: &[u8]) -> QCompressResult<$t> {
        let mut full_bytes = vec![0; 4];
        full_bytes.extend(bytes);
        let parts = (u128::from_be_bytes(full_bytes.try_into().unwrap()) as i128) + Self::MIN;
        Self::new(parts)
      }
    }

    impl From<SystemTime> for $t {
      fn from(system_time: SystemTime) -> Self {
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

        Self::from_secs_and_nanos(seconds, subsec_nanos)
      }
    }

    impl From<$t> for SystemTime {
      fn from(value: $t) -> SystemTime {
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

    impl Display for $t {
      fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
          f,
          "Timestamp({}/{})",
          self.0,
          $parts_per_sec,
        )
      }
    }

    impl NumberLike for $t {
      const HEADER_BYTE: u8 = $header_byte;
      const PHYSICAL_BITS: usize = 96;

      type Unsigned = u128;

      fn to_unsigned(self) -> u128 {
        self.0.wrapping_sub(i128::MIN) as u128
      }

      fn from_unsigned(off: u128) -> Self {
        Self(i128::MIN.wrapping_add(off as i128))
      }

      fn num_eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
      }

      fn num_cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
      }

      fn bytes_from(value: Self) -> Vec<u8> {
        ((value.0 - Self::MIN) as u128).to_be_bytes()[4..].to_vec()
      }

      fn from_bytes(bytes: Vec<u8>) -> Self {
        Self::from_bytes_safe(&bytes).expect("corrupt timestamp bytes")
      }
    }
  }
}

impl_timestamp!(TimestampNs, 1_000_000_000_u32, 8);
impl_timestamp!(TimestampMicros, 1_000_000_u32, 9);

pub type TimestampNsCompressor = Compressor<TimestampNs>;
pub type TimestampNsDecompressor = Decompressor<TimestampNs>;
pub type TimestampMicrosCompressor = Compressor<TimestampMicros>;
pub type TimestampMicrosDecompressor = Decompressor<TimestampMicros>;
