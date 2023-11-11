pub use decoding::Decoder;
pub use encoding::quantize_weights;
pub use encoding::Encoder;

mod decoding;
mod encoding;
mod spec;

// must be u16 or larger
// should not be exposed in public API
pub(crate) type AnsState = u32;
// must be u16 or larger
// should not be exposed in public API
pub(crate) type Token = u16;

#[cfg(test)]
mod tests {
  use crate::ans::spec::Spec;
  use crate::ans::{AnsState, Decoder, Encoder, Token};
  use crate::bit_reader::BitReader;
  use crate::bit_writer::BitWriter;
  use crate::errors::PcoResult;

  fn assert_recovers(spec: &Spec, tokens: Vec<Token>, expected_byte_len: usize) -> PcoResult<()> {
    // ENCODE
    let encoder = Encoder::new(spec);
    let mut state = encoder.default_state();
    let mut to_write = Vec::new();
    for &token in tokens.iter().rev() {
      let (new_state, bitlen) = encoder.encode(state, token);
      to_write.push((state, bitlen));
      state = new_state;
    }

    let mut compressed = Vec::new();
    let mut writer = BitWriter::new(&mut compressed, 5);
    for (word, bitlen) in to_write.into_iter().rev() {
      writer.write_uint(word, bitlen);
      writer.flush()?;
    }
    writer.finish_byte();
    writer.flush()?;
    drop(writer);
    assert_eq!(compressed.len(), expected_byte_len);
    let final_state = state;
    let table_size = 1 << encoder.size_log();

    // DECODE
    compressed.extend(&vec![0; 100]);
    let mut reader = BitReader::new(&compressed, expected_byte_len, 0);
    let decoder = Decoder::new(spec);
    let mut decoded = Vec::new();
    let mut state_idx = final_state - table_size;
    for _ in 0..tokens.len() {
      let node = decoder.get_node(state_idx);
      decoded.push(node.token);
      state_idx = node.next_state_idx_base + reader.read_uint::<AnsState>(node.bits_to_read);
    }

    assert_eq!(decoded, tokens);
    Ok(())
  }

  #[test]
  fn ans_encoder_decoder() -> PcoResult<()> {
    let spec = Spec {
      size_log: 3,
      state_tokens: vec![0, 1, 2, 0, 1, 2, 0, 1],
      token_weights: vec![3, 3, 2],
    };
    // let the tokens be A, B, C
    // the average bit cost per token should be
    // * log2(8/3) = 1.415 for A or B,
    // * log2(4) = 2 for C
    let tokens = vec![2, 0, 1, 1, 1, 0, 0, 1, 2];

    // 9 of these tokens makes ~15 bits or ~2 bytes
    assert_recovers(&spec, tokens, 2)?;

    let mut tokens = Vec::new();
    for _ in 0..200 {
      tokens.push(0);
      tokens.push(1);
      tokens.push(2);
    }
    // With 200 each of A, B, C, we should have about 986 / 8 = 123 bytes
    assert_recovers(&spec, tokens, 125)?;
    Ok(())
  }

  #[test]
  fn ans_encoder_decoder_sparse() -> PcoResult<()> {
    let spec = Spec {
      size_log: 3,
      state_tokens: vec![0, 0, 0, 0, 0, 0, 0, 1],
      token_weights: vec![7, 1],
    };
    let mut tokens = Vec::new();
    for _ in 0..100 {
      for _ in 0..7 {
        tokens.push(0);
      }
      tokens.push(1);
    }
    // let the tokens be A and B
    // each A should cost about log2(8/7) = 0.19 bits
    // each B should cost log2(8) = 3 bits
    // total cost should be about (700 * 0.19 + 100 * 3) / 8 = 55 bytes
    // vs. total cost of huffman would be 1 * 800 / 8 = 100 bytes
    assert_recovers(&spec, tokens, 50)?;
    Ok(())
  }
}
