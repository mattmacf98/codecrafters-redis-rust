use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct TypeCommand {
    key: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl TypeCommand {
    pub fn new(key: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        TypeCommand {
            key: key,
            cache: cache
        }
    }
}

impl RedisCommand for TypeCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        let cache_guard = self.cache.lock().unwrap();
        let value = cache_guard.get(&self.key);
        match value {
            Some(CacheVal::String(_)) => return create_simple_string_resp("string".to_string()),
            Some(CacheVal::List(_)) => return create_simple_string_resp("list".to_string()),
            Some(CacheVal::Stream(_)) => return create_simple_string_resp("stream".to_string()),
            None => return create_simple_string_resp("none".to_string())
        }
    }
}