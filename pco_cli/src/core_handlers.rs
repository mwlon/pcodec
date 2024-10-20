use std::marker::PhantomData;

use pco::data_types::CoreDataType;
use pco::match_number_like_enum;

use crate::decompress::handler::DecompressHandler;
use crate::dtypes::PcoNumberLike;
use crate::inspect::handler::InspectHandler;

fn new_boxed_handler<T: PcoNumberLike>() -> Box<dyn CoreHandler> {
  Box::new(CoreHandlerImpl {
    phantom: PhantomData::<T>,
  })
}

pub fn from_dtype(dtype: CoreDataType) -> Box<dyn CoreHandler> {
  match_number_like_enum!(
    dtype,
    CoreDataType<T> => {
      new_boxed_handler::<T>()
    }
  )
}

pub trait CoreHandler: DecompressHandler + InspectHandler {}

#[derive(Clone, Debug, Default)]
pub struct CoreHandlerImpl<T> {
  phantom: PhantomData<T>,
}

impl<T: PcoNumberLike> CoreHandler for CoreHandlerImpl<T> {}
