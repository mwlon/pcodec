use crate::bin::BinDecompressionInfo;
use crate::bit_reader::BitReader;
use crate::constants::Bitlen;
use crate::data_types::UnsignedLike;

struct AnsNode<U: UnsignedLike> {
  decompression_info: BinDecompressionInfo<U>,
  next_base_idx: usize,
  bits_to_read: Bitlen,
}

struct AnsDecoder<U: UnsignedLike> {
  log_size: Bitlen,
  nodes: Vec<AnsNode<U>>,
  idx: usize,
}

impl<U: UnsignedLike> AnsDecoder<U> {
  pub fn unchecked_decode(&mut self, reader: &mut BitReader) -> &BinDecompressionInfo<U> {
    let node = &self.nodes[self.idx];
    let next_idx = node.next_base_idx + reader.unchecked_read_uint(node.bits_to_read);
    self.idx = next_idx;
    &node.decompression_info
  }
}
