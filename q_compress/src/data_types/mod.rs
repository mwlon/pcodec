use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};
use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitOrAssign, Shl, Shr, Sub};

use crate::{BitReader, BitWriter};
use crate::bits;
use crate::errors::QCompressResult;

pub use timestamps::{TimestampMicros, TimestampNanos};

mod boolean;
mod floats;
mod signeds;
mod timestamps;
mod unsigneds;

/// Trait for data types that behave like signed integers.
///
/// This is used for delta encoding/decoding; i.e. the difference
/// between consecutive numbers must be a `SignedLike`.
/// For example,
/// * The deltas between consecutive `u64`s are `i64`.
/// * The deltas between consecutive `i64`s are `i64`.
/// * The deltas between consecutive timestamps are `i128`.
/// * The deltas between consecutive `bool`s are `bool`s (basically 1 bit
/// signed integers under XOR).
///
/// This is important because deltas like +1 and -1 are numerically close to
/// each other and easily compressible, which would not be the case with
/// unsigned integers.
pub trait SignedLike {
  const ZERO: Self;

  fn wrapping_add(self, other: Self) -> Self;
  fn wrapping_sub(self, other: Self) -> Self;
}

/// Trait for data types that behave like unsigned integers.
///
/// This is used extensively in `q_compress` to guarantee that bitwise
/// operations like `>>` and `|=` are available and that certain properties
/// hold.
/// Under the hood, when numbers are encoded or decoded, they go through their
/// corresponding `UnsignedLike` representation.
pub trait UnsignedLike: Add<Output=Self> + BitAnd<Output=Self> + BitOrAssign +
Copy + Debug + Display + From<u8> + Ord + PartialOrd +
Shl<usize, Output=Self> + Shr<usize, Output=Self> + Sub<Output=Self> {
  const ZERO: Self;
  const ONE: Self;
  const MAX: Self;
  const BITS: usize;

  fn to_f64(self) -> f64;

  /// Shifts the unsigned integer right and returns its lowest bits as a usize.
  /// For example,
  /// ```
  /// use q_compress::data_types::UnsignedLike;
  /// assert_eq!(6_u8.rshift_word(1), 3_usize);
  /// assert_eq!(((1_u128 << 100) + (1_u128 << 4)).rshift_word(1), 8_usize);
  /// ```
  ///
  /// Used for some bit arithmetic operations during compression.
  fn rshift_word(self, shift: usize) -> usize;

  /// Shifts the unsigned integer left and returns its lowest bits as a usize.
  /// For example,
  /// ```
  /// use q_compress::data_types::UnsignedLike;
  /// assert_eq!(6_u8.lshift_word(1), 12_usize);
  /// assert_eq!(((1_u128 << 100) + (1_u128 << 4)).lshift_word(1), 32_usize);
  /// ```
  ///
  /// Used for some bit arithmetic operations during compression.
  fn lshift_word(self, shift: usize) -> usize;
}

/// Trait for data types supported for compression/decompression.
///
/// If you have a new data type you would like to add to the library or
/// implement as custom in your own, these are the questions you need to
/// answer:
/// * What are the corresponding signed integer and unsigned integer types?
/// These are usually the next-larger signed and unsigned integers.
/// * How can I convert to these signed and unsigned representations and back
/// in *a way that preserves ordering*? For instance, converting `f32` to `i32`
/// can be done trivially by transmuting the bytes in memory, but converting
/// from `f32`
/// to `u32` in an order-preserving way requires flipping the sign bit and, if
/// negative, the rest of the bits.
/// * How can I encode and decode this number in an uncompressed way? This
/// uncompressed representation is used to store metadata in each chunk of the
/// Quantile Compression format.
pub trait NumberLike: Copy + Debug + Display + Default + PartialEq + 'static {
  /// A number from 0-255 that corresponds to the number's data type.
  ///
  /// Each `NumberLike` implementation should have a different `HeaderByte`.
  /// This byte gets written into the file's header during compression, and
  /// if the wrong header byte shows up during decompression, the decompressor
  /// will return an error.
  ///
  /// To choose a header byte for a new data type, review all header bytes in
  /// the library and pick an unused one. For instance, as of writing, bytes
  /// 1 through 13 are used, so 14 would be a good choice for another
  /// `q_compress`-supported data type, and 255 would be a good choice for a
  /// custom data type.
  const HEADER_BYTE: u8;
  /// The number of bits in the number's uncompressed representation.
  /// This must match the number of bytes in the `to_bytes` and `from_bytes`
  /// implementations.
  /// Note that booleans have 8 physical bits (not 1)
  /// and timestamps have 96 (not 128).
  const PHYSICAL_BITS: usize;

  /// The unsigned integer this type can convert between to do
  /// bitwise logic and such.
  type Unsigned: UnsignedLike;
  /// The signed integer this type can convert between to do wrapped
  /// subtraction and addition for delta encoding/decoding.
  /// Must be another `NumberLike` with the same `Signed` and `Unsigned` as
  /// this type; in this way, if we take 7th order deltas, they are ensured to
  /// have the same type as 1st order deltas.
  type Signed: SignedLike + NumberLike<Signed=Self::Signed, Unsigned=Self::Unsigned>;

  /// Lossless check for bit-exact equality. This is important because not all data types
  /// support full ordering:
  /// <https://stackoverflow.com/questions/26489701/why-does-rust-not-implement-total-ordering-via-the-ord-trait-for-f64-and-f32>.
  fn num_eq(&self, other: &Self) -> bool;

  /// Lossless numerical comparison. This is important for the same reason as
  /// `num_eq`.
  /// We use it to sort numbers and calculate quantiles.
  /// For example, this function can order the many `f32` and `f64` NaN
  /// representations.
  fn num_cmp(&self, other: &Self) -> Ordering;

  /// Used during compression to convert to an unsigned integer.
  fn to_unsigned(self) -> Self::Unsigned;

  /// Used during decompression to convert back from an unsigned integer.
  fn from_unsigned(off: Self::Unsigned) -> Self;

  /// Used during delta encoding to convert to a signed integer.
  fn to_signed(self) -> Self::Signed;

  /// Used during delta decoding to convert back from a signed integer.
  fn from_signed(signed: Self::Signed) -> Self;

  /// Returns an uncompressed representation for the number.
  fn to_bytes(self) -> Vec<u8>;

  /// Creates a number from an uncompressed representation.
  fn from_bytes(bytes: Vec<u8>) -> QCompressResult<Self>;

  /// Parses an uncompressed representation of the number from the
  /// `BitReader`.
  fn read_from(reader: &mut BitReader) -> QCompressResult<Self> {
    let bools = reader.read(Self::PHYSICAL_BITS)?;
    Self::from_bytes(bits::bits_to_bytes(bools))
  }

  /// Appends an uncompressed representation of the number to the
  /// `BitWriter`.
  fn write_to(self, writer: &mut BitWriter) {
    writer.write(&bits::bytes_to_bits(self.to_bytes()));
  }

  fn le(&self, other: &Self) -> bool {
    !matches!(self.num_cmp(other), Greater)
  }

  fn lt(&self, other: &Self) -> bool {
    matches!(self.num_cmp(other), Less)
  }

  fn ge(&self, other: &Self) -> bool {
    !matches!(self.num_cmp(other), Less)
  }

  fn gt(&self, other: &Self) -> bool {
    matches!(self.num_cmp(other), Greater)
  }
}
