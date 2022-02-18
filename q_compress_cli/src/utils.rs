use anyhow::Result;

pub fn get_header_byte(bytes: &[u8]) -> Result<u8> {
  if bytes.len() >= 5 {
    Ok(bytes[4])
  } else {
    Err(anyhow::anyhow!("only {} bytes found in file", bytes.len()))
  }
}