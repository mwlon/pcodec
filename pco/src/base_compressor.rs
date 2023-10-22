use std::cmp::{max, min};
use std::fmt::Debug;

use crate::bin::{Bin, BinCompressionInfo};
use crate::bit_writer::BitWriter;
use crate::chunk_metadata::{ChunkLatentMetadata, ChunkMetadata};
use crate::chunk_spec::ChunkSpec;
use crate::compression_table::CompressionTable;
use crate::constants::*;
use crate::data_types::{NumberLike, UnsignedLike};
use crate::delta::DeltaMoments;
use crate::errors::{PcoError, PcoResult};
use crate::float_mult_utils::FloatMultConfig;
use crate::latent_batch_dissector::LatentBatchDissector;
use crate::modes::classic::ClassicMode;
use crate::modes::gcd::{GcdMode, use_gcd_arithmetic};
use crate::modes::{gcd, Mode};
use crate::unsigned_src_dst::{DissectedLatents, DissectedSrc, LatentSrc};
use crate::{ans, delta};
use crate::auto;
use crate::{bin_optimization, float_mult_utils};
use crate::chunk_config::ChunkConfig;
use crate::page_metadata::{PageLatentMetadata, PageMetadata};
