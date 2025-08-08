use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, KeyVal, StreamCacheVal, StreamItem, StringCacheVal}}, resp::{create_array_resp, create_bulk_string_resp}, resp::types::RespType};

pub struct XrangeCommand {
    stream_key: String,
    start_id: String,
    end_id: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl XrangeCommand {
    pub fn new(stream_key: String, start_id: String, end_id: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        XrangeCommand {
            stream_key: stream_key,
            start_id: start_id,
            end_id: end_id,
            cache: cache,
        }
    }
}

impl RedisCommand for XrangeCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let cache_guard = self.cache.lock().unwrap();
        match cache_guard.get(&self.stream_key) {
            Some(CacheVal::Stream(cache_stream)) => {
                let start_index = match self.start_id.as_str() {
                    "-" => 0,
                    _ => cache_stream.stream.iter().position(|item| item.id > self.start_id).unwrap_or(1) - 1
                };
                let end_index = match self.end_id.as_str() {
                    "+" => cache_stream.stream.len(),
                    _ => cache_stream.stream.iter().position(|item| item.id > self.end_id).unwrap_or(cache_stream.stream.len())
                };
                let entries_to_return = cache_stream.stream[start_index..end_index].to_vec();

                let stream_items: Vec<String> = entries_to_return.iter().map(|item| {
                    let data: Vec<String> = item.key_vals.iter().flat_map(|kv_item| {
                        vec![create_bulk_string_resp(kv_item.key.clone()), create_bulk_string_resp(kv_item.val.clone())]
                    }).collect();

                    create_array_resp(vec![create_bulk_string_resp(item.id.clone()), create_array_resp(data)])
                }).collect();

                vec![create_array_resp(stream_items)]
            },
            _ => vec![create_array_resp(vec![])]
        }
    }
}