use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, StringCacheVal}, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct SetCommand {
    key: String,
    value: String,
    expiration: Option<u128>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl SetCommand {
    pub fn new(key: String, value: String, expiration: Option<u128>, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        SetCommand {
            key: key,
            value: value,
            expiration: expiration,
            cache: cache,
        }
    }
}

impl RedisCommand for SetCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut cache_guard = self.cache.lock().unwrap();
        if let Some(exp_milis) = self.expiration {
            let expiry = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + exp_milis;
            cache_guard.insert(self.key.clone(), CacheVal::String(StringCacheVal { val: self.value.clone(), expiry_time: Some(expiry) }));
            return vec![create_simple_string_resp("OK".to_string())]
        } else {
            cache_guard.insert(self.key.clone(), CacheVal::String(StringCacheVal { val: self.value.clone(), expiry_time: None }));
            return vec![create_simple_string_resp("OK".to_string())];
        }
    }
}