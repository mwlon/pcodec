use std::convert::TryInto;

fn encode_usize(x: usize) -> [u8; 4] {
  (x as u32).to_le_bytes()
}

fn decode_usize(bytes: &mut [u8]) -> (usize, &mut [u8]) {
  let res = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
  (res, &mut bytes[4..])
}
