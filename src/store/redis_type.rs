use std::collections::HashMap;

use crate::parser::{Payload, DELIMITER};

#[derive(Clone)]
pub enum RedisType {
    String(String),
    Stream(Stream),
}
impl RedisType {
    pub fn as_inner(&self) -> &str {
        match self {
            RedisType::String(s) => s,
            RedisType::Stream(_) => "Invalid call for stream.",
        }
    }

    pub fn type_str(&self) -> String {
        match self {
            RedisType::String(_) => format!("+string{}", DELIMITER),
            RedisType::Stream(_) => format!("+stream{}", DELIMITER),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Stream {
    key: String,
    entries: HashMap<String, String>,
}

impl Stream {
    pub fn new(key: &str, args: &mut Vec<Payload>) -> Self {
        let mut entries = HashMap::new();
        let mut args_iter = args.drain(..);

        while let (Some(k), Some(v)) = (args_iter.next(), args_iter.next()) {
            entries.insert(k.to_string(), v.to_string());
        }

        Self {
            key: key.to_string(),
            entries,
        }
    }
}
