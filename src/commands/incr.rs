use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, StringCacheVal}, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct IncrCommand {
    key: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl IncrCommand {
    pub fn new(key: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        IncrCommand {
            key: key,
            cache: cache
        }
    }
}

impl RedisCommand for IncrCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        let mut cache_guard = self.cache.lock().unwrap();
        return match cache_guard.get_mut(&self.key) {
            Some(CacheVal::String(v)) =>  {
                let expiry_time = v.expiry_time;
                let new_val = match v.val.parse::<i64>() {
                    Ok(v) => v + 1,
                    Err(_) => return create_null_bulk_string_resp(),
                };
                cache_guard.insert(self.key.clone(), CacheVal::String(StringCacheVal { val: new_val.to_string(), expiry_time: expiry_time }));
                create_int_resp(new_val)
            }
            None => {
                cache_guard.insert(self.key.clone(), CacheVal::String(StringCacheVal { val: "1".to_string(), expiry_time: None }));
                create_int_resp(1)
            }
            _ => create_null_bulk_string_resp()
        }
    }
}