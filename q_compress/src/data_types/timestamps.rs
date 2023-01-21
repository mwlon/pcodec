use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::data_types::NumberLike;
use crate::errors::{QCompressError, QCompressResult};

const BILLION_I64: i64 = 1_000_000_000;

macro_rules! impl_timestamp {
  ($t: ident, $parts_per_sec: expr, $header_byte: expr, $precision: expr) => {
    #[doc = concat!(
      "A ",
      $precision,
      "-precise, timezone-naive, 64-bit timestamp."
    )]
    ///
    /// All `q_compress` 64-bit timestamps use a single signed 64 bit integer
    /// for the number of units since 1970.
    /// This means that the date range can be somewhat limited; e.g.
    /// `TimestampNanos` covers from about year 1678 to 2262.
    /// Constructors will panic if the input time lies outside the valid range
    /// for this type.
    ///
    /// Provides conversions to/from `SystemTime`.
    #[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
    pub struct $t(i64);

    impl $t {
      const NS_PER_PART: i64 = BILLION_I64 / $parts_per_sec;

      /// Returns a timestamp with the corresponding `parts` since the Unix
      /// Epoch.
      pub fn new(parts: i64) -> Self {
        Self(parts)
      }

      /// Returns a timestamp with the corresponding seconds and fractional
      /// nanoseconds since the Unix Epoch.
      /// Will panic if the time specified is outside the valid range.
      pub(crate) fn from_secs_and_nanos(seconds: i64, subsec_nanos: i64) -> QCompressResult<Self> {
        seconds.checked_mul($parts_per_sec)
          .and_then(|seconds_parts| seconds_parts.checked_add(subsec_nanos / Self::NS_PER_PART))
          .map($t::new)
          .ok_or_else(|| QCompressError::invalid_argument("timestamp out of range"))
      }

      /// Returns the `(seconds, subsec_nanos)` since the Unix Epoch.
      fn to_secs_and_nanos(self) -> (i64, i64) {
        let parts = self.0;
        let seconds = parts.div_euclid($parts_per_sec);
        let subsec_nanos = parts.rem_euclid($parts_per_sec) * Self::NS_PER_PART;
        (seconds, subsec_nanos)
      }

      /// Returns the total number of `parts` (e.g. microseconds or
      /// nanoseconds) since the Unix Epoch.
      pub fn to_total_parts(self) -> i64 {
        self.0
      }
    }

    impl TryFrom<SystemTime> for $t {
      type Error = QCompressError;

      fn try_from(system_time: SystemTime) -> QCompressResult<Self> {
        let (seconds, subsec_nanos) = match system_time.duration_since(UNIX_EPOCH) {
          Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos() as i64),
          Err(e) => {
            let dur = e.duration();
            let complement_nanos = dur.subsec_nanos();
            let ceil_secs = -(dur.as_secs() as i64);
            if complement_nanos == 0 {
              (ceil_secs, 0)
            } else {
              (ceil_secs - 1, BILLION_I64 - complement_nanos as i64)
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
          let dur = Duration::new(seconds as u64, subsec_nanos as u32);
          UNIX_EPOCH + dur
        } else {
          let dur = if subsec_nanos == 0 {
            Duration::new((-seconds) as u64, 0)
          } else {
            Duration::new((-seconds - 1) as u64, (BILLION_I64 - subsec_nanos) as u32)
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
      const PHYSICAL_BITS: usize = 64;

      type Signed = i64;
      type Unsigned = u64;

      fn to_unsigned(self) -> u64 {
        self.0.wrapping_sub(i64::MIN) as u64
      }

      fn from_unsigned(off: u64) -> Self {
        Self(i64::MIN.wrapping_add(off as i64))
      }

      fn to_signed(self) -> i64 {
        self.0
      }

      fn from_signed(signed: i64) -> Self {
        Self(signed)
      }

      fn to_bytes(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
      }

      fn from_bytes(bytes: &[u8]) -> QCompressResult<Self> {
        Ok(Self(i64::from_be_bytes(bytes.try_into().unwrap())))
      }
    }
  }
}

impl_timestamp!(TimestampNanos, BILLION_I64, 14, "nanosecond");
impl_timestamp!(TimestampMicros, 1_000_000_i64, 15, "microsecond");

#[cfg(test)]
mod tests {
  use std::convert::TryFrom;
  use std::time::{Duration, SystemTime};

  use crate::data_types::{TimestampMicros, TimestampNanos};
  use crate::errors::QCompressResult;

  #[test]
  fn test_system_time_conversion() -> QCompressResult<()> {
    let t = SystemTime::now();
    let micro_t = TimestampMicros::try_from(t)?;
    let nano_t = TimestampNanos::try_from(t)?;
    let (micro_t_s, micro_t_ns) = micro_t.to_secs_and_nanos();
    let (nano_t_s, nano_t_ns) = nano_t.to_secs_and_nanos();
    assert!(micro_t_s > 1500000000); // would be better if we mocked time
    assert_eq!(micro_t_s, nano_t_s);
    assert!(micro_t_ns <= nano_t_ns);
    assert!(micro_t_ns + 1000 > nano_t_ns);
    assert!(t.duration_since(SystemTime::from(micro_t)).unwrap() < Duration::from_secs(1));
    assert_eq!(SystemTime::from(nano_t), t);
    Ok(())
  }
}
