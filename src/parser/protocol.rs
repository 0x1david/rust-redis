use crate::parser::Payload;
use std::io::{BufRead, Read};

use anyhow::{anyhow, Result};

pub struct RedisProtocolParser;

impl RedisProtocolParser {
    pub fn parse<R: Read + BufRead>(reader: &mut R) -> Result<Payload> {
        let payload_type = reader
            .fill_buf()?
            .first()
            .copied()
            .ok_or_else(|| anyhow!("Empty buffer"))?;

        let mut payload: Vec<u8> = vec![];
        reader.read_to_end(&mut payload)?;
        println!("Payload data: {:?}", payload);
        let payload = std::str::from_utf8(&payload)?;
        println!("parsing payload: {:?}", payload);
        let (payload, _) = Payload::from_byte(payload_type, payload)?;
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let mut cursor = Cursor::new(data);
        let result = RedisProtocolParser::parse(&mut cursor).unwrap();
        assert_eq!(result, Payload::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_parse_bulk_string() {
        let data = b"$6\r\nfoobar\r\n";
        let mut cursor = Cursor::new(data);
        let result = RedisProtocolParser::parse(&mut cursor).unwrap();
        assert_eq!(result, Payload::BulkString("foobar".to_string()));
    }
}
