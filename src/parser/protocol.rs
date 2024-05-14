use crate::parser::Payload;
use std::io::{BufRead, Read};

use anyhow::{anyhow, Result};

/// A parser for handling Redis Protocol messages.
///
/// The `RedisProtocolParser` is responsible for parsing messages
/// based on the Redis Serialization Protocol (RESP). It processes
/// input from a `Read` and `BufRead` source and transforms it into
/// structured payloads.
pub struct RedisProtocolParser;

impl RedisProtocolParser {
    /// Parses the data from the reader and organizes it into structured payloads.
    ///
    /// This method reads all available data from the given reader, expects it to
    /// be in RESP format, and converts it into a vector of `Payload` items.
    /// Each `Payload` may consist of multiple nested payloads if the input data
    /// represents an array of commands or data elements.
    ///
    /// # Parameters
    /// - `reader`: A mutable reference to any object that implements `Read` and `BufRead`.
    ///
    /// # Returns
    /// - A `Result` containing either:
    ///   - A `Vec<Payload>` on success, representing the parsed payloads.
    ///   - An `anyhow::Error` on failure, for instance if the buffer is empty or data is malformed.
    ///
    /// # Examples
    /// ```rust
    /// use std::io::Cursor;
    /// use your_crate::RedisProtocolParser;
    ///
    /// let data = Cursor::new("+OK\r\n-ERR some error\r\n:1234\r\n$6\r\nfoobar\r\n");
    /// let payloads = RedisProtocolParser::parse(&mut data).unwrap();
    /// assert_eq!(payloads.len(), 5);
    /// ```
    pub fn parse<R: Read + BufRead>(reader: &mut R) -> Result<Vec<Payload>> {
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
        let payloads = match payload {
            Payload::Array(arr) => {
        let mut result = Vec::new();
        let mut current_group = Vec::new();

        for val in arr.iter() {
            if val.is_command() {
                if !current_group.is_empty() {
                    result.push(Payload::Array(current_group));
                    current_group = Vec::new();
                }
                current_group.push(val.clone());
            } else {
                current_group.push(val.clone());
            }
        }

        if !current_group.is_empty() {
            result.push(Payload::Array(current_group));
        }

        result
    },
            _ => vec!(payload),
        };
        println!("Parsed payload: {:?}", payloads);
        

        Ok(payloads)
    }
}
