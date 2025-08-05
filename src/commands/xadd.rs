use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, KeyVal, StreamCacheVal, StreamItem, StringCacheVal}, create_basic_err_resp, create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct XaddCommand {
    stream_key: String,
    entry_id: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl XaddCommand {
    pub fn new(stream_key: String, entry_id: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        XaddCommand {
            stream_key: stream_key,
            entry_id: entry_id,
            cache: cache,
        }
    }
}

impl RedisCommand for XaddCommand {
    fn execute(&self, iter: &mut Iter<'_, RespType>) -> String {
        let mut cache_guard = self.cache.lock().unwrap();
        if !cache_guard.contains_key(&self.stream_key) {
            cache_guard.insert(self.stream_key.clone(), CacheVal::Stream(StreamCacheVal { stream: vec![] }));
        }

        match cache_guard.get_mut(&self.stream_key) {
            Some(CacheVal::Stream(cache_stream)) => {
                let mut entry_id = self.entry_id.clone();
                if entry_id.eq("*") {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    entry_id = format!("{}-*", now);
                }
                let parts: Vec<&str> = entry_id.split('-').collect();
                if parts.len() != 2 {
                    return create_basic_err_resp("ERR Invalid stream ID format".to_string());
                }

                if entry_id == "0-0" {
                    return create_basic_err_resp("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }
                
                let stream_id = parts[1];
                let final_id_sequence = match (stream_id, cache_stream.stream.is_empty()) {
                    ("*", true) => if parts[0] == "0" { 1 } else { 0 },
                    ("*", false) => {
                        let last_id = &cache_stream.stream.last().unwrap().id;
                        let last_id_parts: Vec<&str> = last_id.split('-').collect();
                        if last_id_parts[0] == parts[0] {
                            let last_id_sequence = last_id_parts[1].parse::<i64>().unwrap_or(0);
                            last_id_sequence + 1    
                        } else {
                            if parts[0] == "0" { 1 } else { 0 }
                        }
                        
                    },
                    (val, _) => val.parse::<i64>().unwrap_or(0)
                };

                let entry_id = format!("{}-{}", parts[0], final_id_sequence);

                if !cache_stream.stream.is_empty() {
                    let last_id = &cache_stream.stream.last().unwrap().id;
                    if entry_id <= last_id.clone() {
                        return create_basic_err_resp("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                    }
                }

                let mut kvs: Vec<KeyVal> = vec![];
                loop {
                    match (iter.next(), iter.next()) {
                        (Some(RespType::String(entry_key)), Some(RespType::String(entry_val))) => {
                            kvs.push(KeyVal { key: entry_key.to_string(), val: entry_val.to_string() });
                        },
                        _ => break
                    }
                }
                
                cache_stream.stream.push(StreamItem { id: entry_id.clone().into(), key_vals: kvs });
                return create_bulk_string_resp(entry_id.to_string());
            },
            _ => return create_null_bulk_string_resp()
        }
    }
}