mod encoding;
mod decoding;
mod spec;

pub use decoding::AnsDecoder;
pub use encoding::AnsEncoder;

#[cfg(test)]
mod tests {
  use crate::ans::{AnsDecoder, AnsEncoder};
  use crate::ans::spec::AnsSpec;
  use crate::bit_reader::BitReader;
  use crate::bit_words::BitWords;
  use crate::bit_writer::BitWriter;

  #[test]
  fn ans_encoder_decoder() {
    let spec = AnsSpec {
      size_log: 3,
      state_tokens: vec![0, 1, 2, 0, 1, 2, 0, 1],
      token_weights: vec![3, 3, 2],
    };
    let tokens = vec![2, 0, 1, 1, 1, 0, 0, 1, 2];

    // ENCODE
    let mut encoder = AnsEncoder::new(&spec);
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
    let bit_words = BitWords::from(writer.drain_bytes());
    let mut reader = BitReader::from(&bit_words);
    let mut decoder = AnsDecoder::new(&spec, final_state);
    let mut decoded = Vec::new();
    for i in 0..tokens.len() {
      decoded.push(decoder.unchecked_decode(&mut reader));
    }

    assert_eq!(decoded, tokens);
  }
}