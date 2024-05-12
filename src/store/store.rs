use crate::{parser::RedisEncodable, store::RedisType};
use std::collections::{BTreeMap, HashMap};

use crate::parser::{Payload, DELIMITER};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

#[derive(Clone)]
pub struct KeyValueStore {
    data: HashMap<String, RedisType>,
    expiries: BTreeMap<DateTime<Utc>, Vec<String>>,
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiries: BTreeMap::new(),
        }
    }
    pub fn set(&mut self, key: &str, value: RedisType, expiry_ms: Option<i64>) -> Result<String> {
        println!("Setting k:{}, v:{}", key, value.type_str());
        if let Some(expiry) = expiry_ms {
            let _ = self.set_expiry(key, expiry);
        };
        self.data.insert(key.to_string(), value);
        Ok(format!("+OK{}", DELIMITER))
    }

    pub fn get(&mut self, key: &str) -> String {
        if let Err(failed) = self.clean_expiries() {
            panic!(
                "Failed cleaning expired records due to an error: {}",
                failed
            )
        }
        println!("Getting k:{}", key);
        match self.data.get(key) {
            Some(value) => Payload::BulkString(value.as_inner().to_string()).redis_encode(),
            None => format!("$-1{}", DELIMITER),
        }
    }

    pub fn set_expiry(&mut self, key: &str, expiry_ms: i64) -> Result<String> {
        let expiry_time = Utc::now() + Duration::milliseconds(expiry_ms);
        println!("Setting k:{}, with expiry {}", key, expiry_time);
        self.expiries
            .entry(expiry_time)
            .or_default()
            .push(key.to_string());
        Ok(format!("+OK{}", DELIMITER))
    }

    pub fn clean_expiries(&mut self) -> Result<()> {
        let now = Utc::now();
        let keys_to_remove: Vec<String> = self
            .expiries
            .range(..=now)
            .flat_map(|(_expiry, keys)| keys.clone())
            .collect();

        for key in keys_to_remove {
            self.data.remove(&key);
        }

        self.expiries = self.expiries.split_off(&now);
        Ok(())
    }
    pub fn get_type(&self, key: &str) -> String {
        match self.data.get(key) {
            Some(value) => value.type_str(),
            None => format!("+none{}", DELIMITER),
        }
    }
}
