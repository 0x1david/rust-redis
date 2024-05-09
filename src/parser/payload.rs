pub const DELIMITER: &str = "\r\n";
const TYPE_SPECIFIER_LEN: usize = 1;

use super::RedisEncodable;
use crate::parser::Command;
use anyhow::{anyhow, bail, Context, Result};
use std::fmt::{Display, Write};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Payload {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Payload>),
    RdbFile(Vec<u8>),
}

impl Payload {
    pub fn build_bulk_string_array(strs: Vec<&str>) -> Self {
        let mut arr = vec![];
        strs.into_iter().for_each(|s| {
            arr.push(Payload::BulkString(s.to_string()));
        });
        Payload::Array(arr)
    }
    pub fn retrieve_content(self) -> Result<(Option<Command>, Value)> {
        match self {
            Self::BulkString(s) => {
                let command = Command::parse(&s);
                let value = command.map_or(Value::String(s.to_string()), |_| Value::Empty);
                Ok((command, value))
            }
            Self::Array(v) => {
                if let Some(Self::BulkString(s)) = v.first() {
                    let command = Command::parse(s);
                    let value = command.map_or_else(
                        || Value::Array(v.clone()),
                        |_| Value::Array(v[1..].to_vec()),
                    );
                    Ok((command, value))
                } else {
                    Err(anyhow!(
                        "Array does not start with a BulkString or is empty"
                    ))
                }
            }
            _ => Err(anyhow!("Payload type not supported for content retrieval")),
        }
    }

    pub fn from_byte(byte: u8, payload: &str) -> Result<(Self, usize)> {
        println!("parsing from byte: {}, with payload: {}", byte, payload);
        match byte {
            b'+' => Self::from_simple_string(payload),
            b'*' => Payload::from_array(payload),
            b'$' => Payload::from_bulk_string(payload),
            e => bail!("Unimplemented payload type {}", e),
        }
    }
    pub fn from_char(c: char, payload: &str) -> Result<(Self, usize)> {
        println!("parsing from char {}", c);
        match c {
            '+' => Self::from_simple_string(payload),
            '*' => Payload::from_array(payload),
            '$' => Payload::from_bulk_string(payload),
            e => bail!("Unimplemented payload type {}", e),
        }
    }

    pub(super) fn from_simple_string(s: &str) -> Result<(Self, usize)> {
        let (payload, _) = s[TYPE_SPECIFIER_LEN..]
            .split_once(DELIMITER)
            .context("No ending delimiter")?;
        Ok((
            Payload::SimpleString(payload.to_string()),
            payload.len() + 3,
        ))
    }
    pub(super) fn from_bulk_string(s: &str) -> Result<(Self, usize)> {
        println!("parsing from bulk string");
        let (length_str, rest) = &s[TYPE_SPECIFIER_LEN..]
            .split_once(DELIMITER)
            .context("Failed splitting at delimiter.")?;
        let length = length_str
            .parse::<usize>()
            .context("Failed to parse len as usize")?;

        let start_index = length_str.len() + 2;

        if rest.len() < length {
            bail!("The data segment is shorter than the specified length.");
        }

        let data = &rest[..length];
        let total_consumed = TYPE_SPECIFIER_LEN + start_index + length + 2;

        println!("Returning Payload::BulkString");
        Ok((Payload::BulkString(data.to_string()), total_consumed))
    }
    pub(super) fn from_array(s: &str) -> Result<(Self, usize)> {
        let (number_of_elements_str, mut rest) = s[TYPE_SPECIFIER_LEN..]
            .split_once(DELIMITER)
            .context("Failed splitting at delimiter.")?;

        let number_of_elements = number_of_elements_str.parse::<usize>()?;
        let mut parsed_elements = Vec::with_capacity(number_of_elements);
        let mut cumulative_offset = 0;

        for _ in 0..number_of_elements {
            let payload_type = rest.chars().next().context("Payload empty")?;

            let (parsed_payload, step) = Payload::from_char(payload_type, rest)?;
            parsed_elements.push(parsed_payload);

            rest = &rest[step..];
            cumulative_offset += step;
        }
        cumulative_offset += TYPE_SPECIFIER_LEN + number_of_elements_str.len() + DELIMITER.len();
        Ok((Payload::Array(parsed_elements), cumulative_offset))
    }
}

impl Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::BulkString(s) => write!(f, "{}", s),
            _ => write!(f, "unimplemented!"),
        }
    }
}

impl RedisEncodable for Payload {
    fn redis_encode(&self) -> String {
        match self {
            Payload::SimpleString(value) => format!("+{}{}", value, DELIMITER),
            Payload::BulkString(value) => {
                format!("${}{}{}{}", value.len(), DELIMITER, value, DELIMITER)
            }
            Payload::Array(elements) => {
                let mut f = format!("*{}{}", elements.len(), DELIMITER);
                for item in elements {
                    write!(f, "{}", item.redis_encode())
                        .expect("Writing to string created just beforehand should never fail");
                }
                f
            }
            _ => unimplemented!(),
        }
    }
}

pub struct PayloadVec(pub Vec<Payload>);

impl RedisEncodable for PayloadVec {
    fn redis_encode(&self) -> String {
        let payloads = self
            .0
            .iter()
            .map(|p| p.redis_encode())
            .collect::<Vec<String>>()
            .join(", ");
        payloads
    }
}

#[derive(Debug)]
pub enum Value {
    Array(Vec<Payload>),
    String(String),
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_simple_string() {
        let input = format!("+OK{}", DELIMITER);
        let result = Payload::from_simple_string(&input);
        assert!(result.is_ok());
        let (payload, length) = result.unwrap();
        assert_eq!(payload, Payload::SimpleString("OK".to_string()));
        assert_eq!(length, 5);
    }

    #[test]
    fn test_from_bulk_string() {
        let input = format!("$4{}PING{}", DELIMITER, DELIMITER);
        let result = Payload::from_bulk_string(&input);
        assert!(result.is_ok());
        let (payload, consumed) = result.unwrap();
        assert_eq!(payload, Payload::BulkString("PING".to_string()));
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_bulk_string_correct_length() {
        let input = format!("$4{}PING{}", DELIMITER, DELIMITER);
        let result = Payload::from_bulk_string(&input);
        assert!(result.is_ok());
        let (payload, consumed) = result.unwrap();
        assert_eq!(payload, Payload::BulkString("PING".to_string()));
        assert_eq!(consumed, 10);
    }

    #[test]
    fn test_array_with_multiple_elements() {
        let input = format!(
            "*2{delim}$4{delim}ECHO{delim}$5{delim}mykey{delim}",
            delim = DELIMITER
        );
        let result = Payload::from_array(&input);
        println!("result is {:?}", result);
        assert!(result.is_ok());
        let (payload, consumed) = result.unwrap();
        match payload {
            Payload::Array(elements) => {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], Payload::BulkString("ECHO".to_string()));
                assert_eq!(elements[1], Payload::BulkString("mykey".to_string()));
            }
            _ => panic!("Expected Payload::Array"),
        }
        assert_eq!(consumed, 25);
    }
}
