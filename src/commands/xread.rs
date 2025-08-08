use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, KeyVal, StreamCacheVal, StreamItem, StringCacheVal}}, resp::{create_array_resp, create_bulk_string_resp, create_null_bulk_string_resp}, resp::types::RespType};

pub struct XreadCommand {
    stream_key: String,
    start_id_exclusive: String,
    timeout_ms: Option<u128>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl XreadCommand {
    pub fn new(stream_key: String, timeout_ms: Option<u128>, start_id_exclusive: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        XreadCommand {
            stream_key: stream_key,
            timeout_ms: timeout_ms,
            start_id_exclusive: start_id_exclusive,
            cache: cache,
        }
    }
}

impl RedisCommand for XreadCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let expiration = match  self.timeout_ms {
            Some(ms) if ms == 0 => {
                Some(u128::MAX)
            },
            Some(ms) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                Some(now + ms)
            }
            None => None
        };

        let mut start_id_exclusive = "0-0".to_string();

        if self.start_id_exclusive.eq("$") {
            let cache_guard = self.cache.lock().unwrap();
            if let Some(CacheVal::Stream(cache_stream)) = cache_guard.get(&self.stream_key) {
                if let Some(stream_item) = cache_stream.stream.last() {
                    start_id_exclusive = stream_item.id.clone();
                }
            }
        } else {
            start_id_exclusive = self.start_id_exclusive.clone();
        }

        loop {
            let cache_guard = self.cache.lock().unwrap();
            if let Some(CacheVal::Stream(cache_stream)) = cache_guard.get(&self.stream_key) {
                let start_index = cache_stream.stream.iter().position(|item| item.id > start_id_exclusive);
                if start_index.is_some() {
                    let entries_to_return = cache_stream.stream[start_index.unwrap()..].to_vec();

                    let stream_items: Vec<String> = entries_to_return.iter().map(|item| {
                        let data: Vec<String> = item.key_vals.iter().flat_map(|kv_item| {
                            vec![create_bulk_string_resp(kv_item.key.clone()), create_bulk_string_resp(kv_item.val.clone())]
                        }).collect();

                        create_array_resp(vec![create_bulk_string_resp(item.id.clone()), create_array_resp(data)])
                    }).collect();

                    if stream_items.len() > 0 {
                        return vec![create_array_resp(vec![create_bulk_string_resp(self.stream_key.clone()), create_array_resp(stream_items)])];
                    }
                }
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            if expiration.is_none() || now > expiration.unwrap() {
                break;
            }
        }
        
        vec![create_null_bulk_string_resp()]
    }
}