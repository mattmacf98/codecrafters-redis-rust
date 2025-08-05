use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct GetCommand {
    key: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl GetCommand {
    pub fn new(key: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        GetCommand {
            key: key,
            cache: cache
        }
    }
}

impl RedisCommand for GetCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let cache_guard = self.cache.lock().unwrap();
        return match cache_guard.get(&self.key) {
            Some(CacheVal::String(v)) =>  {
                match v.expiry_time {
                    Some(exp) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        if now < exp {
                            vec![create_simple_string_resp(v.val.to_string())]
                        } else {
                            vec![create_null_bulk_string_resp()]
                        }
                    },
                    None => vec![create_simple_string_resp(v.val.to_string())]
                }
            }
            _ => vec![create_null_bulk_string_resp()]
        }
    }
}