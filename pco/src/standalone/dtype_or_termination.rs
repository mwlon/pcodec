use DataTypeOrTermination::*;

use crate::data_types::CoreDataType;
use crate::standalone::constants::MAGIC_TERMINATION_BYTE;

/// A value representing either a standalone chunk's data type or a termination
/// byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataTypeOrTermination {
  /// Termination indicates that the file is ending. There are no chunks after
  /// this point.
  Termination,
  /// This indicates that there is a chunk, and its data type is the given
  /// known value.
  Known(CoreDataType),
  /// This indicates that there is a chunk, but its data type is not part of
  /// pco's core implementation. It may be possible to decode it with custom
  /// data types.
  Unknown(u8),
}

impl From<DataTypeOrTermination> for u8 {
  fn from(dtype_or_termination: DataTypeOrTermination) -> u8 {
    match dtype_or_termination {
      Termination => MAGIC_TERMINATION_BYTE,
      Known(core) => core as u8,
      Unknown(byte) => byte,
    }
  }
}

impl From<u8> for DataTypeOrTermination {
  fn from(byte: u8) -> Self {
    if byte == MAGIC_TERMINATION_BYTE {
      Termination
    } else if let Some(core) = CoreDataType::from_descriminant(byte) {
      Known(core)
    } else {
      Unknown(byte)
    }
  }
}
