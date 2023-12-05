use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::{
  Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, Div, DivAssign, Mul, MulAssign, Rem,
  RemAssign, Shl, Shr, Sub, SubAssign,
};

use crate::constants::Bitlen;

mod floats;
mod signeds;
mod unsigneds;

/// *unstable API* Trait for data types that behave like floats.
///
/// This is used internally for compressing and decompressing with
/// [`FloatMultMode`][`crate::Mode::FloatMult`].
pub trait FloatLike:
  Add<Output = Self>
  + AddAssign
  + Copy
  + Debug
  + Display
  + Mul<Output = Self>
  + PartialOrd
  + RemAssign
  + Sub<Output = Self>
  + SubAssign
  + Div<Output = Self>
{
  /// Number of bits that aren't used for exponent or sign.
  /// E.g. for f32 this should be 23.
  const PRECISION_BITS: Bitlen;
  /// The largest positive int `x` expressible in this float such that
  /// `x - 1.0` is also exactly representable as this float.
  const GREATEST_PRECISE_INT: Self;
  const ZERO: Self;
  const ONE: Self;
  const MIN: Self;
  const MAX: Self;
  fn abs(self) -> Self;
  fn inv(self) -> Self;
  fn round(self) -> Self;
  fn from_f64(x: f64) -> Self;
  fn to_f64(self) -> f64;
  fn is_finite_and_normal(&self) -> bool;
  /// Returns the float's exponent. For instance, for f32 this should be
  /// between -126 and +127.
  fn exponent(&self) -> i32;
  fn max(a: Self, b: Self) -> Self;
  fn min(a: Self, b: Self) -> Self;
}

/// *unstable API* Trait for data types that behave like unsigned integers.
///
/// This is used extensively in `pco` to guarantee that bitwise
/// operations like `>>` and `|=` are available and that certain properties
/// hold.
/// Under the hood, when numbers are encoded or decoded, they go through their
/// corresponding `UnsignedLike` representation.
/// Metadata stores numbers as their unsigned representations.
pub trait UnsignedLike:
  Add<Output = Self>
  + AddAssign
  + BitAnd<Output = Self>
  + BitOr<Output = Self>
  + BitAndAssign
  + BitOrAssign
  + Div<Output = Self>
  + DivAssign
  + Hash
  + Mul<Output = Self>
  + MulAssign
  + NumberLike<Unsigned = Self>
  + Ord
  + PartialOrd
  + Rem<Output = Self>
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

  /// The floating point type with the same number of bits.
  type Float: FloatLike + NumberLike<Unsigned = Self>;

  /// Converts a `usize` into this type. Panics if the conversion is
  /// impossible.
  fn from_u64(x: u64) -> Self;

  fn leading_zeros(self) -> Bitlen;

  /// Converts the unsigned integer to a usize, truncating higher bits if necessary.
  fn to_u64(self) -> u64;

  fn wrapping_add(self, other: Self) -> Self;
  fn wrapping_sub(self, other: Self) -> Self;

  /// This should surjectively map the unsigned to the set of integers in its
  /// floating point type. E.g. 3.0, Inf, and NaN are int floats, but 3.5 is
  /// not.
  fn to_int_float(self) -> Self::Float;
  /// This should be the inverse of to_int_float.
  fn from_int_float(float: Self::Float) -> Self;

  /// This should use something like [`f32::from_bits()`]
  fn to_float_bits(self) -> Self::Float;
  /// This should use something like [`f32::to_bits()`]
  fn from_float_bits(float: Self::Float) -> Self;
}

/// *unstable API* Trait for data types supported for compression/decompression.
///
/// If you have a new data type you would like to add to the library or
/// implement as custom in your own, these are the questions you need to
/// answer:
/// * What is the corresponding unsigned integer type? This is probably the
/// smallest unsigned integer with enough bits to represent the number.
/// * How can I convert to this unsigned representation and back
/// in *a way that preserves ordering*? For instance, transmuting `f32` to `u32`
/// wouldn't preserve ordering and would cause pco to fail. In this example,
/// one needs to flip the sign bit and, if negative, the rest of the bits.
pub trait NumberLike: Copy + Debug + Display + Default + PartialEq + 'static {
  /// A number from 1-255 that corresponds to the number's data type.
  ///
  /// Each `NumberLike` implementation should have a different `DTYPE_BYTE`.
  /// This byte gets written into the file's header during compression, and
  /// if the wrong header byte shows up during decompression, the decompressor
  /// will return an error.
  ///
  /// To choose a header byte for a new data type, review all header bytes in
  /// the library and pick an unused one. For instance, as of writing, bytes
  /// 1 through 6 are used, so 7 would be a good choice for another
  /// `pco` data type implementation, and 255 would be a good choice for a
  /// custom data type.
  const DTYPE_BYTE: u8;
  /// The number of bits in the number's uncompressed representation.
  /// This must match the number of bytes in the `to_bytes` and `from_bytes`
  /// implementations.
  /// Note that booleans have 8 physical bits (not 1).
  const IS_FLOAT: bool = false;

  /// The unsigned integer this type can convert between to do
  /// bitwise logic and such.
  type Unsigned: UnsignedLike;

  /// If IS_FLOAT = true, this must be reimplemented as an identity function.
  fn assert_float(_nums: &[Self]) -> &[<Self::Unsigned as UnsignedLike>::Float] {
    panic!("bug; not a float")
  }

  /// Used during compression to convert to an unsigned integer in a way that
  /// preserves ordering.
  fn to_unsigned(self) -> Self::Unsigned;

  /// Used during decompression to convert back from an unsigned integer in a
  /// way that preserves ordering.
  fn from_unsigned(off: Self::Unsigned) -> Self;

  // These transmute functions do not preserve ordering.
  // Their purpose is to allow certain operations in-place, relying on the fact
  // that each NumberLike should have the same size as its UnsignedLike.
  /// Used during decompression to share memory for this type and its
  /// corresponding UnsignedLike.
  fn transmute_to_unsigned_slice(slice: &mut [Self]) -> &mut [Self::Unsigned];

  /// Used during decompression to share memory for this type and its
  /// corresponding UnsignedLike.
  fn transmute_to_unsigned(self) -> Self::Unsigned;
}
