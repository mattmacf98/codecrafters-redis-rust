use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, ListCacheVal}}, resp::{create_int_resp, types::RespType}};

pub struct RpushCommand {
    list_key: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl RpushCommand {
    pub fn new(list_key: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        RpushCommand {
            list_key: list_key,
            cache: cache,
        }
    }
}

impl RedisCommand for RpushCommand {
    fn execute(&self, iter: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut cache_gaurd = self.cache.lock().unwrap();
        return match cache_gaurd.get_mut(&self.list_key) {
            Some(CacheVal::List(list_cache_val)) => {
                while let Some(RespType::String(val)) = iter.next() {
                    list_cache_val.list.push(val.into());
                }
                vec![create_int_resp(list_cache_val.list.len())]
            },
            None => {
                let mut list = vec![];
                while let Some(RespType::String(val)) = iter.next() {
                    list.push(val.into());
                }

                let len = list.len();
                cache_gaurd.insert(self.list_key.clone(), CacheVal::List(ListCacheVal { list: list, block_queue: vec![] }));
                vec![create_int_resp(len)]
            },
            _ => vec![create_int_resp(0)]
        }
    }
}