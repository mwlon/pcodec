use std::marker::PhantomData;

use pco::data_types::CoreDataType;
use pco::with_core_dtypes;

use crate::decompress::decompress_handler::DecompressHandler;
use crate::dtypes::PcoNumberLike;
use crate::inspect::inspect_handler::InspectHandler;

fn new_boxed_handler<T: PcoNumberLike>() -> Box<dyn CoreHandler> {
  Box::new(CoreHandlerImpl {
    phantom: PhantomData::<T>,
  })
}

pub fn from_dtype(dtype: CoreDataType) -> Box<dyn CoreHandler> {
  macro_rules! match_dtype {
    {$($name:ident($lname:ident) => $t:ty,)+} => {
      match dtype {
        $(CoreDataType::$name => new_boxed_handler::<$t>(),)+
      }
    }
  }

  with_core_dtypes!(match_dtype)
}

pub trait CoreHandler: DecompressHandler + InspectHandler {}

#[derive(Clone, Debug, Default)]
pub struct CoreHandlerImpl<T> {
  phantom: PhantomData<T>,
}

impl<T: PcoNumberLike> CoreHandler for CoreHandlerImpl<T> {}
