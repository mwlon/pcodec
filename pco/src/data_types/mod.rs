use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::{
  Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, Div, DivAssign, Mul, MulAssign, Neg,
  Rem, RemAssign, Shl, Shr, Sub, SubAssign,
};

pub use dynamic::CoreDataType;

use crate::constants::Bitlen;
use crate::describers::LatentDescriber;
use crate::{ChunkConfig, ChunkMeta, Mode};

mod dynamic;
mod floats;
mod signeds;
mod unsigneds;

/// This is used internally for compressing and decompressing with
/// [`FloatMultMode`][`crate::Mode::FloatMult`].
pub(crate) trait FloatLike:
  Add<Output = Self>
  + AddAssign
  + Copy
  + Debug
  + Display
  + Mul<Output = Self>
  + Neg<Output = Self>
  + NumberLike
  + PartialOrd
  + RemAssign
  + Send
  + Sync
  + Sub<Output = Self>
  + SubAssign
  + Div<Output = Self>
{
  const BITS: Bitlen;
  /// Number of bits that aren't used for exponent or sign.
  /// E.g. for f32 this should be 23.
  const PRECISION_BITS: Bitlen;
  const ZERO: Self;
  const MAX_FOR_SAMPLING: Self;

  fn abs(self) -> Self;
  fn inv(self) -> Self;
  fn round(self) -> Self;
  fn exp2(power: i32) -> Self;
  fn from_f64(x: f64) -> Self;
  fn to_f64(self) -> f64;
  fn is_finite_and_normal(&self) -> bool;
  fn is_sign_positive_(&self) -> bool;
  /// Returns the float's exponent. For instance, for f32 this should be
  /// between -127 and +126.
  fn exponent(&self) -> i32;
  fn trailing_zeros(&self) -> u32;
  fn max(a: Self, b: Self) -> Self;
  fn min(a: Self, b: Self) -> Self;

  /// This should use something like [`f32::to_bits()`]
  fn to_latent_bits(self) -> Self::L;
  /// This should surjectively map the latent to the set of integers in its
  /// floating point type. E.g. 3.0, Inf, and NaN are int floats, but 3.5 is
  /// not.
  fn int_float_from_latent(l: Self::L) -> Self;
  /// This should be the inverse of `int_float_from_latent`.
  fn int_float_to_latent(self) -> Self::L;
  /// This should map from e.g. 7_u32 -> 7.0_f32
  fn from_latent_numerical(l: Self::L) -> Self;
}

/// *unstable API* Trait for data types that behave like unsigned integers.
///
/// This is used extensively in `pco` to guarantee that bitwise
/// operations like `>>` and `|=` are available and that certain properties
/// hold.
/// Under the hood, when numbers are encoded or decoded, they go through their
/// corresponding `Latent` representation.
/// Metadata stores numbers as their latent representations.
pub trait Latent:
  Add<Output = Self>
  + AddAssign
  + BitAnd<Output = Self>
  + BitOr<Output = Self>
  + BitAndAssign
  + BitOrAssign
  + Display
  + Div<Output = Self>
  + DivAssign
  + Hash
  + Mul<Output = Self>
  + MulAssign
  + NumberLike<L = Self>
  + Ord
  + PartialOrd
  + Rem<Output = Self>
  + RemAssign
  + Send
  + Sync
  + Shl<Bitlen, Output = Self>
  + Shr<Bitlen, Output = Self>
  + Sub<Output = Self>
{
  const ZERO: Self;
  const ONE: Self;
  const MID: Self;
  const MAX: Self;
  const BITS: Bitlen;

  /// Converts a `usize` into this type. Panics if the conversion is
  /// impossible.
  fn from_u64(x: u64) -> Self;

  fn leading_zeros(self) -> Bitlen;

  /// Converts the latent to a usize, truncating higher bits if necessary.
  fn to_u64(self) -> u64;

  fn wrapping_add(self, other: Self) -> Self;
  fn wrapping_sub(self, other: Self) -> Self;

  fn toggle_center(self) -> Self {
    self.wrapping_add(Self::MID)
  }
}

/// *unstable API* Trait for data types supported for compression/decompression.
///
/// If you have a new data type you would like to add to the library or
/// implement as custom in your own, these are the questions you need to
/// answer:
/// * What is the corresponding latent type? This is probably the
/// smallest unsigned integer with enough bits to represent the number.
/// * How can I convert to this latent representation and back
/// in *a way that preserves ordering*? For instance, transmuting `f32` to `u32`
/// wouldn't preserve ordering and would cause pco to fail. In this example,
/// one needs to flip the sign bit and, if negative, the rest of the bits.
pub trait NumberLike: Copy + Debug + Display + Default + PartialEq + Send + Sync + 'static {
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
  /// `pco` data type implementation.
  const DTYPE_BYTE: u8;
  /// If true, decompressors write the primary latent stream to `dst` directly
  /// instead of a separate buffer.
  /// It is expected that transmutable latents implement
  /// [`transmute_to_latents`][Self::transmute_to_latents] and that
  /// `join_latents` uses `dst` as the primary latent.
  const TRANSMUTABLE_TO_LATENT: bool;

  /// The latent this type can convert between to do
  /// bitwise logic and such.
  type L: Latent;

  /// Returns a `LatentDescriber` for each latent variable in the chunk
  /// metadata.
  fn get_latent_describers(meta: &ChunkMeta<Self::L>) -> Vec<LatentDescriber<Self::L>>;

  fn mode_is_valid(mode: Mode<Self::L>) -> bool;
  fn choose_mode_and_split_latents(
    nums: &[Self],
    config: &ChunkConfig,
  ) -> (Mode<Self::L>, Vec<Vec<Self::L>>);

  fn from_latent_ordered(l: Self::L) -> Self;
  fn to_latent_ordered(self) -> Self::L;
  fn join_latents(mode: Mode<Self::L>, primary: &mut [Self::L], secondary: &[Self::L]);

  fn transmute_to_latents(_slice: &mut [Self]) -> &mut [Self::L] {
    unimplemented!("transmutable numbers must reimplement this");
  }
  fn transmute_to_latent(self) -> Self::L {
    unimplemented!("transmutable numbers must reimplement this");
  }
}

pub(crate) fn split_latents_classic<T: NumberLike>(nums: &[T]) -> Vec<Vec<T::L>> {
  vec![nums.iter().map(|&x| x.to_latent_ordered()).collect()]
}
