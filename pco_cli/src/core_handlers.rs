use std::marker::PhantomData;

use pco::data_types::NumberType;
use pco::match_number_enum;

use crate::decompress::handler::DecompressHandler;
use crate::dtypes::PcoNumber;
use crate::inspect::handler::InspectHandler;

fn new_boxed_handler<T: PcoNumber>() -> Box<dyn CoreHandler> {
  Box::new(CoreHandlerImpl {
    phantom: PhantomData::<T>,
  })
}

pub fn from_dtype(dtype: NumberType) -> Box<dyn CoreHandler> {
  match_number_enum!(
    dtype,
    NumberType<T> => {
      new_boxed_handler::<T>()
    }
  )
}

pub trait CoreHandler: DecompressHandler + InspectHandler {}

#[derive(Clone, Debug, Default)]
pub struct CoreHandlerImpl<T> {
  phantom: PhantomData<T>,
}

impl<T: PcoNumber> CoreHandler for CoreHandlerImpl<T> {}
