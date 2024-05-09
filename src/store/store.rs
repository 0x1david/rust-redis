use crate::{parser::RedisEncodable, store::RedisType};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;

use crate::parser::{Payload, DELIMITER};
use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

#[derive(Clone)]
pub struct KeyValueStore {
    data: Arc<Mutex<HashMap<String, RedisType>>>,
    expiries: Arc<Mutex<BTreeMap<DateTime<Utc>, Vec<String>>>>,
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            expiries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
    pub async fn set(
        &mut self,
        key: &str,
        value: RedisType,
        expiry_ms: Option<i64>,
    ) -> Result<String> {
        println!("Setting k:{}, v:{}", key, value.type_str());
        if let Some(expiry) = expiry_ms {
            let _ = self.set_expiry(key, expiry).await;
        };
        self.data.lock().await.insert(key.to_string(), value);
        Ok(format!("+OK{}", DELIMITER))
    }

    pub async fn get(&mut self, key: &str) -> String {
        if let Err(failed) = self.clean_expiries().await {
            panic!(
                "Failed cleaning expired records due to an error: {}",
                failed
            )
        }
        println!("Getting k:{}", key);
        match self.data.lock().await.get(key) {
            Some(value) => Payload::BulkString(value.as_inner().to_string()).redis_encode(),
            None => format!("$-1{}", DELIMITER),
        }
    }

    pub async fn set_expiry(&mut self, key: &str, expiry_ms: i64) -> Result<String> {
        let expiry_time = Utc::now() + Duration::milliseconds(expiry_ms);
        println!("Setting k:{}, with expiry {}", key, expiry_time);
        self.expiries
            .lock()
            .await
            .entry(expiry_time)
            .or_default()
            .push(key.to_string());
        Ok(format!("+OK{}", DELIMITER))
    }

    pub async fn clean_expiries(&mut self) -> Result<()> {
        let now = Utc::now();
        let mut expiries = self.expiries.lock().await;
        let keys_to_remove: Vec<String> = expiries
            .range(..=now)
            .flat_map(|(_expiry, keys)| keys.clone())
            .collect();

        for key in keys_to_remove {
            self.data.lock().await.remove(&key);
        }

        *expiries = expiries.split_off(&now);
        Ok(())
    }
    pub async fn get_type(&self, key: &str) -> String {
        match self.data.lock().await.get(key) {
            Some(value) => value.type_str(),
            None => format!("+none{}", DELIMITER),
        }
    }
}
