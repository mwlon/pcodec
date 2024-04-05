use std::path::PathBuf;
use std::str::FromStr;

use arrow::datatypes::DataType;
use clap::{Args, Parser};

use crate::bench::codecs::CodecConfig;
use crate::input::InputFileOpt;
use crate::{dtypes, parse};
