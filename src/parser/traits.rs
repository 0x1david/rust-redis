pub trait RedisEncodable {
    fn redis_encode(&self) -> String;
}
