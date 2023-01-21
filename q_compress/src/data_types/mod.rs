use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitOr, BitOrAssign, Div, Mul, RemAssign, Shl, Shr, Sub};

use crate::bit_reader::BitReader;
use crate::bit_writer::BitWriter;
use crate::bits;
use crate::errors::QCompressResult;

pub use timestamps::{TimestampMicros, TimestampNanos};

mod boolean;
mod floats;
mod signeds;
mod timestamps;
mod unsigneds;

#[cfg(feature="timestamps_96")]
mod timestamps_96;
#[cfg(feature="timestamps_96")]
pub use timestamps_96::{TimestampMicros96, TimestampNanos96};

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
/// Note: API stability of `SignedLike` is not guaranteed.
pub trait SignedLike: NumberLike<Signed=Self> {
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
///
/// Note: API stability of `UnsignedLike` is not guaranteed.
pub trait UnsignedLike: Add<Output=Self> + BitAnd<Output=Self> + BitOr<Output=Self> + BitOrAssign +
Copy + Debug + Display + Div<Output=Self> + Mul<Output = Self> + Ord +
PartialOrd + RemAssign + Shl<usize, Output=Self> + Shr<usize, Output=Self> +
Sub<Output=Self> {
  const ZERO: Self;
  const ONE: Self;
  const MAX: Self;
  const BITS: usize;

  /// Converts a `usize` into this type. Panics if the conversion is
  /// impossible.
  fn from_word(word: usize) -> Self;

  fn to_f64(self) -> f64;

  /// Shifts the unsigned integer right and returns its lowest bits as a
  /// `usize`.
  /// For example,
  /// ```
  /// use q_compress::data_types::UnsignedLike;
  /// assert_eq!(6_u8.rshift_word(1), 3_usize);
  /// assert_eq!(((1_u128 << 100) + (1_u128 << 4)).rshift_word(1), 8_usize);
  /// ```
  ///
  /// Used for some bit arithmetic operations during compression.
  fn rshift_word(self, shift: usize) -> usize;

  /// Shifts the unsigned integer left and returns its lowest bits as a
  /// `usize`.
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
///
/// Note: API stability of `NumberLike` is not guaranteed.
pub trait NumberLike: Copy + Debug + Display + Default + PartialEq + 'static {
  /// A number from 0-255 that corresponds to the number's data type.
  ///
  /// Each `NumberLike` implementation should have a different `HEADER_BYTE`.
  /// This byte gets written into the file's header during compression, and
  /// if the wrong header byte shows up during decompression, the decompressor
  /// will return an error.
  ///
  /// To choose a header byte for a new data type, review all header bytes in
  /// the library and pick an unused one. For instance, as of writing, bytes
  /// 1 through 15 are used, so 16 would be a good choice for another
  /// `q_compress`-supported data type, and 255 would be a good choice for a
  /// custom data type.
  const HEADER_BYTE: u8;
  /// The number of bits in the number's uncompressed representation.
  /// This must match the number of bytes in the `to_bytes` and `from_bytes`
  /// implementations.
  /// Note that booleans have 8 physical bits (not 1)
  /// and timestamps have 96 (not 128).
  const PHYSICAL_BITS: usize;

  /// The signed integer this type can convert between to do wrapped
  /// subtraction and addition for delta encoding/decoding.
  /// Must be another `NumberLike` with the same `Signed` and `Unsigned` as
  /// this type; in this way, if we take 7th order deltas, they are ensured to
  /// have the same type as 1st order deltas.
  type Signed: SignedLike + NumberLike<Signed=Self::Signed, Unsigned=Self::Unsigned>;
  /// The unsigned integer this type can convert between to do
  /// bitwise logic and such.
  type Unsigned: UnsignedLike;

  // TODO in 1.0 remove this
  /// This is no longer important and will go away in a future release.
  fn num_eq(&self, other: &Self) -> bool {
    self.to_unsigned() == other.to_unsigned()
  }

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
  fn from_bytes(bytes: &[u8]) -> QCompressResult<Self>;

  /// Parses an uncompressed representation of the number from the
  /// `BitReader`.
  fn read_from(reader: &mut BitReader) -> QCompressResult<Self> {
    let bools = reader.read(Self::PHYSICAL_BITS)?;
    Self::from_bytes(&bits::bits_to_bytes(bools))
  }

  /// Appends an uncompressed representation of the number to the
  /// `BitWriter`.
  fn write_to(self, writer: &mut BitWriter) {
    writer.write(&bits::bytes_to_bits(self.to_bytes()));
  }
}
