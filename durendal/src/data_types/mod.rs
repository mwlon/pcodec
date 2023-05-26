use std::fmt::{Debug, Display};
use std::ops::{Add, BitAnd, BitAndAssign, BitOr, BitOrAssign, Div, Mul, RemAssign, Shl, Shr, Sub};



use crate::constants::Bitlen;



mod floats;
mod signeds;
mod unsigneds;

pub trait FloatLike: Copy + Debug + Mul<Output = Self> {
  fn inv(self) -> Self;
  fn round(self) -> Self;
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
pub trait UnsignedLike:
  Add<Output = Self>
  + BitAnd<Output = Self>
  + BitOr<Output = Self>
  + BitAndAssign
  + BitOrAssign
  + Copy
  + Debug
  + Display
  + Div<Output = Self>
  + Mul<Output = Self>
  + NumberLike<Unsigned = Self>
  + Ord
  + PartialOrd
  + RemAssign
  + Shl<Bitlen, Output = Self>
  + Shr<Bitlen, Output = Self>
  + Sub<Output = Self>
{
  const ZERO: Self;
  const ONE: Self;
  const MID: Self;
  const MAX: Self;
  const BITS: Bitlen;

  type Float: FloatLike + NumberLike<Unsigned=Self>;

  /// Converts a `usize` into this type. Panics if the conversion is
  /// impossible.
  fn from_word(word: usize) -> Self;

  fn leading_zeros(self) -> Bitlen;

  /// Shifts the unsigned integer right and returns its lowest bits as a
  /// `usize`.
  /// For example,
  /// ```
  /// use durendal::data_types::UnsignedLike;
  /// assert_eq!(6_u32.rshift_word(1), 3_usize);
  /// ```
  ///
  /// Used for some bit arithmetic operations during compression.
  fn rshift_word(self, shift: Bitlen) -> usize;

  /// Shifts the unsigned integer left and returns its lowest bits as a
  /// `usize`.
  /// For example,
  /// ```
  /// use durendal::data_types::UnsignedLike;
  /// assert_eq!(6_u32.lshift_word(1), 12_usize);
  /// ```
  ///
  /// Used for some bit arithmetic operations during compression.
  fn lshift_word(self, shift: Bitlen) -> usize;

  fn wrapping_add(self, other: Self) -> Self;

  fn wrapping_sub(self, other: Self) -> Self;

  fn to_float(self) -> Self::Float;

  fn from_float_bits(float: Self::Float) -> Self;
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
  /// Note that booleans have 8 physical bits (not 1).
  const PHYSICAL_BITS: usize;

  /// The unsigned integer this type can convert between to do
  /// bitwise logic and such.
  type Unsigned: UnsignedLike;

  /// Used during compression to convert to an unsigned integer in a way that
  /// preserves ordering.
  fn to_unsigned(self) -> Self::Unsigned;

  /// Used during decompression to convert back from an unsigned integer in a
  /// way that preserves ordering.
  fn from_unsigned(off: Self::Unsigned) -> Self;

  // These transmute functions do not preserve ordering.
  // Their purpose is to allow certain operations in-place, relying on the fact
  // that each NumberLike should have the same size as its UnsignedLike.
  fn transmute_to_unsigned_slice(slice: &mut [Self]) -> &mut [Self::Unsigned];

  fn transmute_to_unsigned(self) -> Self::Unsigned;
}
