use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct LlenCommand {
    list_key: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl LlenCommand {
    pub fn new(list_key: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        LlenCommand {
            list_key: list_key,
            cache: cache
        }
    }
}

impl RedisCommand for LlenCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        let cache_guard = self.cache.lock().unwrap();
        return match cache_guard.get(&self.list_key) {
            Some(CacheVal::List(val)) => {
                return create_int_resp(val.list.len())
            },
            _ =>  create_int_resp(0)
        }
    }
}