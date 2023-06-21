mod encoding;
mod decoding;
mod spec;

pub use encoding::AnsEncoder;
pub use encoding::quantize_weights;
pub use decoding::AnsDecoder;
pub use spec::Token;

#[cfg(test)]
mod tests {
  use crate::ans::{AnsDecoder, AnsEncoder};
  use crate::ans::spec::{AnsSpec, Token};
  use crate::bit_reader::BitReader;
  use crate::bit_words::BitWords;
  use crate::bit_writer::BitWriter;

  fn assert_recovers(spec: &AnsSpec, tokens: Vec<Token>, expected_byte_len: usize) {
    // ENCODE
    let mut encoder = AnsEncoder::new(spec);
    let mut to_write = Vec::new();
    for &token in tokens.iter().rev() {
      to_write.push(encoder.encode(token));
    }
    let mut writer = BitWriter::default();
    for (word, bitlen) in to_write.into_iter().rev() {
      writer.write_usize(word, bitlen);
    }
    let final_state = encoder.state();

    // DECODE
    let mut decoder = AnsDecoder::new(spec, final_state);
    let bytes = writer.drain_bytes();
    assert_eq!(bytes.len(), expected_byte_len);
    let bit_words = BitWords::from(bytes);
    let mut reader = BitReader::from(&bit_words);
    let mut decoded = Vec::new();
    for i in 0..tokens.len() {
      decoded.push(decoder.unchecked_decode(&mut reader));
    }

    assert_eq!(decoded, tokens);
  }

  #[test]
  fn ans_encoder_decoder() {
    let spec = AnsSpec {
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
    assert_recovers(&spec, tokens, 2);

    let mut tokens = Vec::new();
    for _ in 0..200 {
      tokens.push(0);
      tokens.push(1);
      tokens.push(2);
    }
    // With 200 each of A, B, C, we should have about 986 / 8 = 123 bytes
    assert_recovers(&spec, tokens, 125);
  }

  #[test]
  fn ans_encoder_decoder_sparse() {
    let spec = AnsSpec {
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
    assert_recovers(&spec, tokens, 50);
  }
}