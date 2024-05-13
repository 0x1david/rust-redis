/// Defines a trait for encoding types into a Redis-compatible string format.
///
/// This trait should be implemented by types that need to be serialized into the format
/// used by Redis commands or data structures. The primary method `redis_encode` handles
/// the conversion of the implementing type into a string that adheres to the Redis Serialization
/// Protocol (RESP), which is a simple protocol used by Redis to communicate with clients.
///
/// Implementors of this trait should ensure that the returned string is correctly formatted
/// according to RESP rules. For example, integers should be encoded with a leading colon (`:`),
/// bulk strings should have their length prefixed, etc.
///
/// # Required Methods
/// - `redis_encode`: Returns a `String` that represents the encoded format of the type,
///   suitable for transmission to a Redis server or storage within Redis data structures.
///
/// # Examples
/// Implementation for a custom struct `MyData`:
/// ```rust
/// struct MyData {
///     key: String,
///     value: i32,
/// }
///
/// impl RedisEncodable for MyData {
///     fn redis_encode(&self) -> String {
///         format!("${}{}\r\n{}{}\r\n", self.key.len(), self.key, self.value.to_string().len(), self.value)
///     }
/// }
///
/// let my_data = MyData { key: "age".to_string(), value: 30 };
/// assert_eq!(my_data.redis_encode(), "$3\r\nage\r\n2\r\n30\r\n");
/// ```
pub trait RedisEncodable {
    /// Encodes the implementing type into a Redis-compatible string format.
    ///
    /// This method should convert the type into a string that can be directly used within Redis commands
    /// or stored in Redis as part of its key-value pairs, lists, sets, or other data structures.
    /// Proper RESP formatting must be ensured by the implementor.
    ///
    /// # Returns
    /// A `String` representing the Redis-encoded format of the type.
    fn redis_encode(&self) -> String;
}
