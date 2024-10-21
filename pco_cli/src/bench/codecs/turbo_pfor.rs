use std::convert::TryInto;
use std::mem;

use clap::Parser;
use half::f16;

use crate::bench::codecs::CodecInternal;
use crate::dtypes::{PcoNumber, TurboPforable};

#[derive(Clone, Debug, Parser)]
pub struct TurboPforConfig {}

impl CodecInternal for TurboPforConfig {
  fn name(&self) -> &'static str {
    "tpfor"
  }

  fn get_confs(&self) -> Vec<(&'static str, String)> {
    vec![]
  }

  fn compress<T: PcoNumber>(&self, nums: &[T]) -> Vec<u8> {
    let mut nums = nums.to_vec();
    // not sure this is the real contract, just a heuristic
    let dst_size = 64 + ((nums.len() * mem::size_of::<T>()) as f32 * 1.01) as usize;
    let mut dst = vec![0; dst_size];
    dst[..8].copy_from_slice(&(nums.len() as u64).to_le_bytes());
    let byte_len = unsafe { <T as TurboPforable>::encode(&mut nums, &mut dst[8..]) };
    dst.truncate(byte_len + 8);
    dst
  }

  fn decompress<T: PcoNumber>(&self, src: &[u8]) -> Vec<T> {
    let n = u64::from_le_bytes(src[..8].try_into().unwrap()) as usize;
    let mut src = src[8..].to_vec();
    let mut dst = Vec::with_capacity(n);
    unsafe {
      <T as TurboPforable>::decode(&mut src, n, &mut dst);
      dst.set_len(n);
    }
    dst
  }
}

macro_rules! impl_pforable {
  ($t: ty, $pfor: ty, $enc: ident, $dec: ident) => {
    impl TurboPforable for $t {
      unsafe fn encode(src: &mut [Self], dst: &mut [u8]) -> usize {
        let n = src.len();
        turbo_pfor_sys::$enc(
          src.as_mut_ptr() as *mut $pfor,
          n,
          dst.as_mut_ptr(),
        )
      }
      unsafe fn decode(src: &mut [u8], n: usize, dst: &mut [Self]) {
        turbo_pfor_sys::$dec(
          src.as_mut_ptr(),
          n,
          dst.as_mut_ptr() as *mut $pfor,
        );
      }
    }
  };
}

impl_pforable!(u16, u16, p4nenc128v16, p4ndec128v16);
impl_pforable!(u32, u32, p4nenc128v32, p4ndec128v32);
impl_pforable!(u64, u64, p4nenc128v64, p4ndec128v64);
impl_pforable!(i16, u16, p4nenc128v16, p4ndec128v16);
impl_pforable!(i32, u32, p4nenc128v32, p4ndec128v32);
impl_pforable!(i64, u64, p4nenc128v64, p4ndec128v64);
impl_pforable!(f16, u16, p4nenc128v16, p4ndec128v16);
impl_pforable!(f32, u32, p4nenc128v32, p4ndec128v32);
impl_pforable!(f64, u64, p4nenc128v64, p4ndec128v64);
