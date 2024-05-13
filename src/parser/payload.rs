pub const DELIMITER: &str = "\r\n";
const TYPE_SPECIFIER_LEN: usize = 1;

use super::RedisEncodable;
use crate::parser::Command;
use anyhow::{anyhow, bail, Context, Result};
use std::fmt::{Display, Write};

/// Represents the various types of payloads that can be encoded and decoded within the Redis protocol.
///
/// This enum captures the different forms of data that can be communicated between a Redis client
/// and server, adhering to the RESP (Redis Serialization Protocol). Each variant corresponds to a
/// specific data type or structure described in the RESP specification, allowing for structured
/// communication of commands, data, and responses.
///
/// Variants:
/// - `SimpleString`: Represents a simple string in RESP, which is a non-binary string encoded with
///   a leading '+' sign and terminated by "\r\n". Used primarily for conveying non-critical messages
///   or statuses (e.g., OK or PONG).
/// - `BulkString`: Represents a bulk string in RESP, which is a length-prefixed binary-safe string.
///   Begins with '$' followed by the length of the string and "\r\n", then the string itself and another "\r\n".
///   This type is used for transmitting potentially large or binary data.
/// - `Array`: Represents an array of payloads in RESP, encoded with a leading '*' followed by the number
///   of elements in the array and "\r\n", followed by the serialization of each element. Arrays can nest
///   other arrays or different types of payloads, facilitating complex data structures or multiple commands.
/// - `RdbFile`: Encapsulates raw binary data typically associated with Redis Database (RDB) files or snapshots.
///   This variant is not part of standard RESP but is used for handling RDB file transmissions in certain Redis
///   replication or persistence scenarios.
///
/// # Examples
/// Parsing a simple RESP message:
/// ```
/// use crate::Payload;
///
/// let data = "+OK\r\n";
/// let payload = Payload::SimpleString("OK".to_string());
/// assert_eq!(format!("{}", payload), "OK"); // Using Display trait for SimpleString
/// ```
///
/// Handling a bulk string:
/// ```
/// use crate::Payload;
///
/// let data = "$6\r\nfoobar\r\n";
/// let payload = Payload::BulkString("foobar".to_string());
/// assert_eq!(format!("{}", payload), "foobar"); // Using Display trait for BulkString
/// ```
///
/// Working with an array of payloads:
/// ```
/// use crate::Payload;
///
/// let inner_payload1 = Payload::SimpleString("First".to_string());
/// let inner_payload2 = Payload::SimpleString("Second".to_string());
/// let array_payload = Payload::Array(vec![inner_payload1, inner_payload2]);
/// // Array handling can be complex, involving iteration and further parsing
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Payload {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Payload>),
    RdbFile(Vec<u8>),
}

impl Payload {
    /// Creates a `Payload::Array` containing `Payload::BulkString` items from a list of string slices.
    ///
    /// This method takes a vector of string slices and converts each into a `Payload::BulkString`,
    /// collecting all of these into a `Payload::Array`. It is useful for constructing complex
    /// Redis commands that involve multiple bulk strings.
    ///
    /// # Parameters
    /// - `strs`: A vector of string slices that will be converted into bulk string payloads.
    ///
    /// # Returns
    /// - A `Payload::Array` containing the bulk strings created from the input strings.
    ///
    /// # Examples
    /// ```
    /// let commands = vec!["SET", "key", "value"];
    /// let payload = Payload::build_bulk_string_array(commands);
    /// assert!(matches!(payload, Payload::Array(_)));
    /// ```
    pub fn build_bulk_string_array(strs: Vec<&str>) -> Self {
        let mut arr = vec![];
        strs.into_iter().for_each(|s| {
            arr.push(Payload::BulkString(s.to_string()));
        });
        Payload::Array(arr)
    }
    /// Determines whether the payload represents a command.
    ///
    /// This method checks if the payload is a bulk string that corresponds to a known Redis command.
    /// It is primarily used to identify if payloads received from or prepared for Redis should
    /// be treated as commands.
    ///
    /// # Returns
    /// - `true` if the payload is a bulk string that matches a known Redis command.
    /// - `false` otherwise, including for all other payload types.
    ///
    /// # Examples
    /// ```
    /// let payload = Payload::BulkString("SET".to_string());
    /// assert!(payload.is_command());
    /// ```
    pub fn is_command(&self) -> bool {
        match self {
            Self::BulkString(value) => Command::parse(value).is_some(),
            _ => false
        }

    }
    /// Extracts command and value content from the payload, handling command identification.
    ///
    /// This method is used to separate command payloads from associated data, facilitating the
    /// handling of Redis commands and their arguments. It returns a tuple containing an optional
    /// `Command` and a `Value`, which can represent further payloads or simple strings.
    ///
    /// # Returns
    /// - A `Result` containing:
    ///   - A tuple of an optional `Command` and a `Value`. The command is present if the payload
    ///     is recognized as a Redis command. The value contains additional data or arguments for the command.
    ///   - An error if the payload is not compatible with content retrieval (e.g., unsupported payload types
    ///     or incorrectly formatted arrays).
    ///
    /// # Errors
    /// - Returns an error if the payload is not a `BulkString` or if the first element of an `Array`
    ///   is not a `BulkString`. Also returns an error for unsupported payload types.
    ///
    /// # Examples
    /// ```
    /// let payload = Payload::BulkString("GET key".to_string());
    /// let (command, value) = payload.retrieve_content().unwrap();
    /// assert_eq!(command, Some(Command::Get));
    /// assert_eq!(value, Value::String("key".to_string()));
    /// ```
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
    /// Parses a payload based on the first byte and the subsequent text content.
    ///
    /// This method interprets the first byte of the payload to determine its type according to
    /// the Redis Serialization Protocol (RESP). It supports simple strings, bulk strings, and arrays,
    /// delegating to specific parsing methods based on the byte encountered.
    ///
    /// # Parameters
    /// - `byte`: The first byte of the payload, indicating the RESP data type.
    /// - `payload`: The remainder of the string after the type specifier.
    ///
    /// # Returns
    /// - A `Result` containing a tuple of the parsed `Payload` and the number of bytes consumed
    ///   from the input string, or an error if the byte does not correspond to a recognized payload type.
    ///
    /// # Errors
    /// - Returns an error if the payload type is unimplemented or unrecognized.
    ///
    /// # Examples
    /// ```
    /// let input = "+OK\r\n";
    /// let result = Payload::from_byte(b'+', &input[1..]);
    /// assert!(result.is_ok());
    /// let (payload, consumed) = result.unwrap();
    /// assert_eq!(payload, Payload::SimpleString("OK".to_string()));
    /// assert_eq!(consumed, 5);
    /// ```
    pub fn from_byte(byte: u8, payload: &str) -> Result<(Self, usize)> {
        println!("parsing from byte: {}, with payload: {}", byte, payload);
        match byte {
            b'+' => Self::from_simple_string(payload),
            b'*' => Payload::from_array(payload),
            b'$' => Payload::from_bulk_string(payload),
            e => bail!("Unimplemented payload type {}", e),
        }
    }
    /// Similar to `from_byte`, but initializes parsing from a character instead of a byte.
    ///
    /// This method functions identically to `from_byte`, translating the initial character
    /// into the appropriate payload parsing function. This is used when dealing with character-
    /// oriented input sources.
    ///
    /// # Parameters
    /// - `c`: The first character of the payload string indicating the RESP data type.
    /// - `payload`: The rest of the payload string after the type specifier.
    ///
    /// # Returns
    /// - A `Result` containing a tuple of the parsed `Payload` and the number of bytes consumed,
    ///   or an error if the character does not correspond to a recognized payload type.
    ///
    /// # Errors
    /// - Returns an error if the payload type is unimplemented or unrecognized.
    ///
    /// # Examples
    /// ```
    /// let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    /// let result = Payload::from_char('*', &input[1..]);
    /// assert!(result.is_ok());
    /// ```
    pub fn from_char(c: char, payload: &str) -> Result<(Self, usize)> {
        println!("parsing from char {}", c);
        match c {
            '+' => Self::from_simple_string(payload),
            '*' => Payload::from_array(payload),
            '$' => Payload::from_bulk_string(payload),
            e => bail!("Unimplemented payload type {}", e),
        }
    }
    /// Parses a simple string from a given RESP formatted input.
    ///
    /// Simple strings are identified by a leading '+' and end with "\r\n".
    /// This method extracts the content of a simple string, excluding its type specifier and delimiter.
    ///
    /// # Parameters
    /// - `s`: The payload string after the '+' specifier.
    ///
    /// # Returns
    /// - A `Result` containing a tuple of the parsed `Payload::SimpleString` and the total bytes consumed.
    ///
    /// # Errors
    /// - Returns an error if the ending delimiter is missing.
    ///
    /// # Examples
    /// ```
    /// let input = "OK\r\n";
    /// let result = Payload::from_simple_string(input);
    /// assert!(result.is_ok());
    /// let (payload, length) = result.unwrap();
    /// assert_eq!(payload, Payload::SimpleString("OK".to_string()));
    /// assert_eq!(length, 5); // Including + and \r\n
    /// ```
    pub(super) fn from_simple_string(s: &str) -> Result<(Self, usize)> {
        let (payload, _) = s[TYPE_SPECIFIER_LEN..]
            .split_once(DELIMITER)
            .context("No ending delimiter")?;
        Ok((
            Payload::SimpleString(payload.to_string()),
            payload.len() + 3,
        ))
    }
    // Parses a bulk string from a given RESP formatted input.
    ///
    /// Bulk strings start with a '$' followed by the length of the string, a "\r\n",
    /// the string content, and another "\r\n". This method parses the bulk string according
    /// to these specifications, verifying the length and extracting the string.
    ///
    /// # Parameters
    /// - `s`: The payload string after the '$' specifier.
    ///
    /// # Returns
    /// - A `Result` containing a tuple of the parsed `Payload::BulkString` and the total bytes consumed.
    ///
    /// # Errors
    /// - Returns an error if the length specifier is invalid, the delimiter is missing,
    ///   or the actual string length does not match the specified length.
    ///
    /// # Examples
    /// ```
    /// let input = "4\r\nPING\r\n";
    /// let result = Payload::from_bulk_string(input);
    /// assert!(result.is_ok());
    /// let (payload, consumed) = result.unwrap();
    /// assert_eq!(payload, Payload::BulkString("PING".to_string()));
    /// assert_eq!(consumed, 10); // Including $, length, both \r\n, and string content
    /// ```
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
    /// Parses an array from a given RESP formatted input.
    ///
    /// Arrays in RESP are prefixed with an asterisk '*' followed by the number of elements in the array
    /// and a CRLF ("\r\n"). Each element of the array is then encoded according to its own data type
    /// rules (e.g., simple strings, bulk strings). This method parses the array, recursively handling
    /// each element according to the RESP rules.
    ///
    /// # Parameters
    /// - `s`: The payload string after the '*' specifier, which should start with the number of elements
    ///        followed by each element's data.
    ///
    /// # Returns
    /// - A `Result` containing a tuple of the parsed `Payload::Array` and the total bytes consumed
    ///   from the input string.
    ///
    /// # Errors
    /// - Returns an error if the initial number of elements is missing, the format is incorrect,
    ///   or any contained element fails to parse according to its expected format.
    ///
    /// # Examples
    /// ```
    /// let input = "2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    /// let result = Payload::from_array(input);
    /// assert!(result.is_ok());
    /// let (payload, consumed) = result.unwrap();
    /// match payload {
    ///     Payload::Array(elements) => {
    ///         assert_eq!(elements.len(), 2);
    ///         assert_eq!(elements[0], Payload::BulkString("foo".to_string()));
    ///         assert_eq!(elements[1], Payload::BulkString("bar".to_string()));
    ///     },
    ///     _ => panic!("Expected Payload::Array"),
    /// }
    /// assert_eq!(consumed, 23); // Total bytes including all elements and metadata
    /// ```
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
