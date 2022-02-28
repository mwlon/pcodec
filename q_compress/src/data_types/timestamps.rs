use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::errors::{QCompressError, QCompressResult};
use crate::data_types::NumberLike;

const BILLION_U32: u32 = 1_000_000_000;

/// A nanosecond-precise, timezone-naive timestamp.
///
/// All `q_compress` timestamps use a signed 64 bit integer for the number of
/// seconds since the Unix Epoch, which is a range of about +/- 500 million
/// years.
/// This is (generally) the most generous timestamp range standard used by
/// other major tools today.
///
/// `TimestampNanos` in particular have fine enough granularity to losslessly
/// convert timestamps for any operating system or (generally) major tool.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TimestampNanos(i128);

/// A microsecond-precise, timezone-naive timestamp.
///
/// All `q_compress` timestamps use a signed 64 bit integer for the number of
/// seconds since the Unix Epoch, which is a range of about +/- 500 million
/// years.
/// This is (generally) the most generous timestamp range standard used by
/// other major tools today.
///
/// `TimestampMicros` has fine enough granularity to losslessly convert
/// timestamps for many (but not all) operating systems and major tools.
/// For most practical purposes, resolution finer than microsecond is not
/// helpful.
///
/// Supports `SystemTime::from()` and vice versa.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TimestampMicros(i128);

macro_rules! impl_timestamp {
  ($t: ty, $parts_per_sec: expr, $header_byte: expr) => {
    impl $t {
      const MAX: i128 = $parts_per_sec as i128 * (i64::MAX as i128 + 1) - 1;
      const MIN: i128 = $parts_per_sec as i128 * (i64::MIN as i128);
      const NS_PER_PART: u32 = BILLION_U32 / $parts_per_sec;

      /// Returns a timestamp with the corresponding `parts` since the Unix
      /// Epoch. Will return an error if outside the bounds of a 64-bit signed
      /// integer for seconds and 32-bit unsigned integer for
      /// fractional parts.
      pub fn new(parts: i128) -> QCompressResult<Self> {
        if Self::is_valid(parts) {
          Ok(Self(parts))
        } else {
          Err(QCompressError::invalid_argument(format!(
            "invalid timestamp with {}/{} seconds",
            parts,
            $parts_per_sec,
          )))
        }
      }

      /// Returns a timestamp with the corresponding seconds and fractional
      /// nanoseconds since the Unix Epoch.
      pub fn from_secs_and_nanos(seconds: i64, subsec_nanos: u32) -> Self {
        Self(seconds as i128 * $parts_per_sec as i128 + (subsec_nanos / Self::NS_PER_PART) as i128)
      }

      /// Returns the `(seconds, subsec_nanos)` since the Unix Epoch.
      pub fn to_secs_and_nanos(self) -> (i64, u32) {
        let parts = self.0;
        let seconds = parts.div_euclid($parts_per_sec as i128) as i64;
        let subsec_nanos = parts.rem_euclid($parts_per_sec as i128) as u32 * Self::NS_PER_PART;
        (seconds, subsec_nanos)
      }

      /// Returns the total number of `parts` (e.g. microseconds or
      /// nanoseconds) since the Unix Epoch.
      pub fn to_total_parts(self) -> i128 {
        self.0
      }

      /// Return an error if the timestamp is out of range.
      ///
      /// Valid timestamps fit into a 64-bit signed integer
      /// for seconds and 32-bit unsigned integer for the fractional part
      /// of the second. However, the in-memory representation uses a 128-bit
      /// signed integer for the total number of fractional parts.
      /// It is theoretically possible for a corrupt delta-encoded file to
      /// cause a decompressor to return invalid timestamps.
      /// If you are concerned about a data corruption affecting such a case
      /// without being noticed, you may want to `.validate()` every returned
      /// timestamp. Otherwise, it is possible that a panic occurs when you try
      /// to use the corrupt timestamps.
      pub fn validate(&self) -> QCompressResult<()> {
        if Self::is_valid(self.0) {
          Ok(())
        } else {
          Err(QCompressError::corruption(format!(
            "corrupt timestamp with {}/{} seconds",
            self.0,
            $parts_per_sec,
          )))
        }
      }

      fn is_valid(parts: i128) -> bool {
        parts <= Self::MAX && parts >= Self::MIN
      }
    }

    impl From<SystemTime> for $t {
      fn from(system_time: SystemTime) -> Self {
        let (seconds, subsec_nanos) = match system_time.duration_since(UNIX_EPOCH) {
          Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
          Err(e) => {
            let dur = e.duration();
            let complement_nanos = dur.subsec_nanos();
            let ceil_secs = -(dur.as_secs() as i64);
            if complement_nanos == 0 {
              (ceil_secs, 0)
            } else {
              (ceil_secs - 1, BILLION_U32 - complement_nanos)
            }
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

      type Signed = i128;
      type Unsigned = u128;

      fn to_unsigned(self) -> u128 {
        self.0.wrapping_sub(i128::MIN) as u128
      }

      fn from_unsigned(off: u128) -> Self {
        Self(i128::MIN.wrapping_add(off as i128))
      }

      fn to_signed(self) -> i128 {
        self.0
      }

      fn from_signed(signed: i128) -> Self {
        // TODO configure some check at the end of decompression to make sure
        // all timestamps are within bounds
        Self(signed)
      }

      fn to_bytes(self) -> Vec<u8> {
        ((self.0 - Self::MIN) as u128).to_be_bytes()[4..].to_vec()
      }

      fn from_bytes(bytes: Vec<u8>) -> QCompressResult<Self> {
        let mut full_bytes = vec![0; 4];
        full_bytes.extend(bytes);
        let parts = (u128::from_be_bytes(full_bytes.try_into().unwrap()) as i128) + Self::MIN;
        Self::new(parts)
      }
    }
  }
}

impl_timestamp!(TimestampNanos, BILLION_U32, 8);
impl_timestamp!(TimestampMicros, 1_000_000_u32, 9);

#[cfg(test)]
mod tests {
  use std::time::{Duration, SystemTime};
  use crate::data_types::{TimestampMicros, TimestampNanos};

  #[test]
  fn test_system_time_conversion() {
    let t = SystemTime::now();
    let micro_t = TimestampMicros::from(t);
    let nano_t = TimestampNanos::from(t);
    let (micro_t_s, micro_t_ns) = micro_t.to_secs_and_nanos();
    let (nano_t_s, nano_t_ns) = nano_t.to_secs_and_nanos();
    assert!(micro_t_s > 1500000000); // would be better if we mocked time
    assert_eq!(micro_t_s, nano_t_s);
    assert!(micro_t_ns <= nano_t_ns);
    assert!(micro_t_ns + 1000 > nano_t_ns);
    assert!(t.duration_since(SystemTime::from(micro_t)).unwrap() < Duration::from_secs(1));
    assert_eq!(SystemTime::from(nano_t), t);
  }
}
